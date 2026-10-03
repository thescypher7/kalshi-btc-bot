#!/usr/bin/env python3
"""Keep the live SQLite database small: move old rows into gzip archives, then delete them.

Run hourly (kalshi-retention.timer) as the `kalshi` user.

  live DB   keeps the last KEEP_HOURS hours, so recent data stays queryable
  archive   hourly files (UTC hours), three kinds:
              raw_YYYYMMDDTHH.jsonl.gz    BULK order-book rows (orderbook_delta / orderbook_snapshot)
              core_YYYYMMDDTHH.jsonl.gz   every other raw row: ticker quotes, settlements, market lists, errors
              brti_YYYYMMDDTHH.jsonl.gz   BRTI price ticks
  pruning   only the bulk raw_ files are deleted: after ARCHIVE_DAYS, or oldest first if free disk drops below
            MIN_FREE_GB. core_ and brti_ files are tiny and are KEPT (set CORE_DAYS > 0 to expire them).
            Before a raw_ file is deleted, any non-bulk rows still inside it are copied into core_ first.

Safety rules:
  * rows are written to the archive and fsync'd BEFORE they are deleted from the database
  * every archived record carries "id" (the SQLite rowid). If the job dies between the archive
    write and the delete, the next run archives those rows again; dedupe on (table, id) when analysing
  * deletes run in small transactions so the logger's writer is never blocked for long
  * freed pages are reused by SQLite, so the database file stays about the size of KEEP_HOURS of data
    (it does not shrink on its own; run VACUUM during a logger stop if you ever need the space back)
  * a lock file next to the archive folder stops two retention runs from writing the same files at once

One-off backfill for archives written before core_ files existed (also makes analysis scripts fast):
  python retention.py --extract-core
"""
import argparse
import contextlib
import fcntl
import gzip
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

TABLES = {
    "raw": ("recv_ms", "mono_ns", "src", "kind", "ticker", "payload"),
    "brti": ("recv_ms", "channel", "source_ts_ms", "kalshi_recv_ms", "sending_ts_ms",
             "value", "avg60", "avg60_n", "avg15", "avg15_n"),
}
BULK_KINDS = ("orderbook_delta", "orderbook_snapshot")      # ~99% of the volume; everything else is "core"
BULK_MARK = '"kind":"orderbook_'                            # cheap substring test on compact archive lines
CORE_MARKER = ".core_complete"                              # written by --extract-core when the backfill is done
FILE_RE = re.compile(r"^(raw|core|brti)_(\d{8}T\d{2})\.jsonl\.gz$")
DELETE_CHUNK = 2000     # rows per delete transaction (keeps the write lock short)


def log(msg: str) -> None:
    print(f"[retention] {msg}", flush=True)


def hour_stamp(recv_ms: int) -> str:
    return datetime.fromtimestamp(recv_ms / 1000, tz=timezone.utc).strftime("%Y%m%dT%H")


@contextlib.contextmanager
def archive_lock(archive_dir: Path):
    """Exclusive lock kept OUTSIDE the archive folder (so it never shows up among the archive files)."""
    archive_dir.parent.mkdir(parents=True, exist_ok=True)
    with open(archive_dir.parent / f".{archive_dir.name}.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def append_gz(path: Path, lines: list[str]) -> None:
    """Append one gzip member to `path` and fsync it. Concatenated members are valid gzip."""
    data = ("\n".join(lines) + "\n").encode()
    with open(path, "ab") as raw:
        with gzip.GzipFile(fileobj=raw, mode="ab", compresslevel=6) as gz:
            gz.write(data)
        raw.flush()
        os.fsync(raw.fileno())


def archive_prefix(table: str, kind: str | None) -> str:
    if table == "raw":
        return "raw" if kind in BULK_KINDS else "core"
    return table


def archive_table(db: sqlite3.Connection, table: str, cutoff_ms: int, archive_dir: Path,
                  batch: int, pause: float = 0.05) -> int:
    cols = TABLES[table]
    col_sql = ", ".join(cols)
    moved = 0
    while True:
        rows = db.execute(
            f"SELECT rowid, {col_sql} FROM {table} WHERE recv_ms < ? ORDER BY recv_ms LIMIT ?",
            (cutoff_ms, batch)).fetchall()
        if not rows:
            return moved
        by_file: dict[tuple[str, str], list[str]] = {}
        for r in rows:
            rec = {"id": r[0], **dict(zip(cols, r[1:]))}
            key = (archive_prefix(table, rec.get("kind")), hour_stamp(r[1]))
            by_file.setdefault(key, []).append(json.dumps(rec, separators=(",", ":")))
        for (prefix, stamp), lines in by_file.items():
            append_gz(archive_dir / f"{prefix}_{stamp}.jsonl.gz", lines)
        # Archive is durable. Now delete in short transactions.
        ids = [(r[0],) for r in rows]
        for i in range(0, len(ids), DELETE_CHUNK):
            with db:
                db.executemany(f"DELETE FROM {table} WHERE rowid = ?", ids[i:i + DELETE_CHUNK])
            time.sleep(pause)
        moved += len(rows)


def archive_files(archive_dir: Path):
    """Archive files oldest first, as (hour_datetime, path)."""
    out = []
    for p in archive_dir.iterdir():
        m = FILE_RE.match(p.name)
        if m:
            hour = datetime.strptime(m.group(2), "%Y%m%dT%H").replace(tzinfo=timezone.utc)
            out.append((hour, p))
    return sorted(out)


# ---------- keeping the small, useful rows out of the bulk files ----------
def extract_core(src: Path, archive_dir: Path) -> int:
    """Copy the non-bulk records of one raw_ archive into the core_ archive of the same hour.

    Idempotent: records already present in the core file (same id) are skipped. Returns rows added."""
    stamp = FILE_RE.match(src.name).group(2)
    dst = archive_dir / f"core_{stamp}.jsonl.gz"
    have: set = set()
    if dst.exists():
        with gzip.open(dst, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    have.add(json.loads(line).get("id"))
    keep: list[str] = []
    with gzip.open(src, "rt", encoding="utf-8") as f:
        for line in f:
            if BULK_MARK in line:
                continue
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("kind") in BULK_KINDS or rec.get("id") in have:
                continue
            have.add(rec.get("id"))
            keep.append(line)
    for i in range(0, len(keep), 50_000):
        append_gz(dst, keep[i:i + 50_000])
    return len(keep)


def extract_all(archive_dir: Path) -> int:
    """Backfill core_ files from every existing raw_ archive, then write the completion marker."""
    total = 0
    with archive_lock(archive_dir):
        for _, p in archive_files(archive_dir):
            if p.name.startswith("raw_"):
                n = extract_core(p, archive_dir)
                total += n
                log(f"{p.name}: copied {n} core rows")
        (archive_dir / CORE_MARKER).write_text(datetime.now(timezone.utc).isoformat() + "\n")
    return total


def prune_archives(archive_dir: Path, now: datetime, archive_days: float, min_free_gb: float,
                   core_days: float = 0) -> int:
    removed = 0
    files = archive_files(archive_dir)
    expiry = now - timedelta(days=archive_days)

    def drop_bulk(p: Path, force: bool) -> bool:
        try:
            extract_core(p, archive_dir)          # never lose ticker quotes / settlements with the order book
        except Exception as e:                    # unreadable file: keep it unless we are out of disk
            log(f"WARNING: could not extract core rows from {p.name}: {e!r}" + ("" if force else "; keeping the file"))
            if not force:
                return False
        p.unlink()
        return True

    for hour, p in list(files):
        if p.name.startswith("raw_") and hour < expiry:
            if drop_bulk(p, force=False):
                files.remove((hour, p))
                removed += 1
        elif core_days and not p.name.startswith("raw_") and hour < now - timedelta(days=core_days):
            p.unlink()
            files.remove((hour, p))
            removed += 1
    # Disk guard: free space beats keeping old order-book data. core_ and brti_ files are never touched here.
    for hour, p in list(files):
        if not p.name.startswith("raw_"):
            continue
        if shutil.disk_usage(archive_dir).free / 1e9 >= min_free_gb:
            break
        log(f"WARNING: free disk below {min_free_gb} GB, deleting archive {p.name}")
        drop_bulk(p, force=True)
        removed += 1
    return removed


def run(db_path: str, archive_dir: Path, keep_hours: float, archive_days: float,
        min_free_gb: float, batch: int, now: datetime | None = None, dry_run: bool = False,
        core_days: float = 0) -> dict:
    now = now or datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(hours=keep_hours)).timestamp() * 1000)
    archive_dir.mkdir(parents=True, exist_ok=True)
    result = {"cutoff_ms": cutoff_ms, "moved": {}, "pruned": 0}
    db = sqlite3.connect(db_path, timeout=60)
    try:
        db.execute("PRAGMA busy_timeout=60000")
        lock = contextlib.nullcontext() if dry_run else archive_lock(archive_dir)
        with lock:
            for table in TABLES:
                if dry_run:
                    n = db.execute(f"SELECT COUNT(*) FROM {table} WHERE recv_ms < ?", (cutoff_ms,)).fetchone()[0]
                    result["moved"][table] = n
                else:
                    result["moved"][table] = archive_table(db, table, cutoff_ms, archive_dir, batch)
            if not dry_run:
                result["pruned"] = prune_archives(archive_dir, now, archive_days, min_free_gb, core_days)
    finally:
        db.close()
    return result


def resolve_archive_dir(db_path: str, archive_dir: str | None = None) -> Path:
    """--archive-dir wins, then $ARCHIVE_DIR, then an `archive` folder next to the database file."""
    return Path(archive_dir or os.getenv("ARCHIVE_DIR") or Path(db_path).parent / "archive")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.getenv("LOG_DB", "kalshi_log.sqlite"))
    ap.add_argument("--archive-dir", default=None, help="default: an 'archive' folder next to --db")
    ap.add_argument("--keep-hours", type=float, default=float(os.getenv("KEEP_HOURS", 6)))
    ap.add_argument("--archive-days", type=float, default=float(os.getenv("ARCHIVE_DAYS", 7)),
                    help="how long bulk order-book archives (raw_) are kept")
    ap.add_argument("--core-days", type=float, default=float(os.getenv("CORE_DAYS", 0)),
                    help="how long core_ and brti_ archives are kept; 0 = forever (default)")
    ap.add_argument("--min-free-gb", type=float, default=float(os.getenv("MIN_FREE_GB", 8)))
    ap.add_argument("--batch", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true", help="only count rows that would be archived")
    ap.add_argument("--extract-core", action="store_true",
                    help="one-off: copy non-order-book rows out of existing raw_ archives into core_ files, then exit")
    a = ap.parse_args(argv)
    t0 = time.time()
    archive = resolve_archive_dir(a.db, a.archive_dir)
    if a.extract_core:
        n = extract_all(archive)
        log(f"extract-core done: {n} rows copied in {time.time() - t0:.1f}s")
        return 0
    res = run(a.db, archive, a.keep_hours, a.archive_days, a.min_free_gb, a.batch,
              dry_run=a.dry_run, core_days=a.core_days)
    log(f"{'would move' if a.dry_run else 'moved'} {res['moved']} rows, pruned {res['pruned']} archive files "
        f"in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
