#!/usr/bin/env python3
"""Keep the live SQLite database small: move old rows into gzip archives, then delete them.

Run hourly (kalshi-retention.timer) as the `kalshi` user.

  live DB   keeps the last KEEP_HOURS hours, so recent data stays queryable
  archive   hourly files  raw_YYYYMMDDTHH.jsonl.gz / brti_YYYYMMDDTHH.jsonl.gz  (UTC hours)
  pruning   archives older than ARCHIVE_DAYS are deleted, and the oldest ones go first
            if free disk space drops below MIN_FREE_GB

Safety rules:
  * rows are written to the archive and fsync'd BEFORE they are deleted from the database
  * every archived record carries "id" (the SQLite rowid). If the job dies between the archive
    write and the delete, the next run archives those rows again; dedupe on (table, id) when analysing
  * deletes run in small transactions so the logger's writer is never blocked for long
  * freed pages are reused by SQLite, so the database file stays about the size of KEEP_HOURS of data
    (it does not shrink on its own; run VACUUM during a logger stop if you ever need the space back)
"""
import argparse
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
FILE_RE = re.compile(r"^(raw|brti)_(\d{8}T\d{2})\.jsonl\.gz$")
DELETE_CHUNK = 2000     # rows per delete transaction (keeps the write lock short)


def log(msg: str) -> None:
    print(f"[retention] {msg}", flush=True)


def hour_stamp(recv_ms: int) -> str:
    return datetime.fromtimestamp(recv_ms / 1000, tz=timezone.utc).strftime("%Y%m%dT%H")


def append_gz(path: Path, lines: list[str]) -> None:
    """Append one gzip member to `path` and fsync it. Concatenated members are valid gzip."""
    data = ("\n".join(lines) + "\n").encode()
    with open(path, "ab") as raw:
        with gzip.GzipFile(fileobj=raw, mode="ab", compresslevel=6) as gz:
            gz.write(data)
        raw.flush()
        os.fsync(raw.fileno())


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
        by_hour: dict[str, list[str]] = {}
        for r in rows:
            rec = {"id": r[0], **dict(zip(cols, r[1:]))}
            by_hour.setdefault(hour_stamp(r[1]), []).append(json.dumps(rec, separators=(",", ":")))
        for stamp, lines in by_hour.items():
            append_gz(archive_dir / f"{table}_{stamp}.jsonl.gz", lines)
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


def prune_archives(archive_dir: Path, now: datetime, archive_days: float, min_free_gb: float) -> int:
    removed = 0
    files = archive_files(archive_dir)
    expiry = now - timedelta(days=archive_days)
    for hour, p in list(files):
        if hour < expiry:
            p.unlink()
            files.remove((hour, p))
            removed += 1
    # Disk guard: free space beats keeping old research data.
    for hour, p in list(files):
        if shutil.disk_usage(archive_dir).free / 1e9 >= min_free_gb:
            break
        log(f"WARNING: free disk below {min_free_gb} GB, deleting archive {p.name}")
        p.unlink()
        removed += 1
    return removed


def run(db_path: str, archive_dir: Path, keep_hours: float, archive_days: float,
        min_free_gb: float, batch: int, now: datetime | None = None, dry_run: bool = False) -> dict:
    now = now or datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(hours=keep_hours)).timestamp() * 1000)
    archive_dir.mkdir(parents=True, exist_ok=True)
    result = {"cutoff_ms": cutoff_ms, "moved": {}, "pruned": 0}
    db = sqlite3.connect(db_path, timeout=60)
    try:
        db.execute("PRAGMA busy_timeout=60000")
        for table in TABLES:
            if dry_run:
                n = db.execute(f"SELECT COUNT(*) FROM {table} WHERE recv_ms < ?", (cutoff_ms,)).fetchone()[0]
                result["moved"][table] = n
            else:
                result["moved"][table] = archive_table(db, table, cutoff_ms, archive_dir, batch)
        if not dry_run:
            result["pruned"] = prune_archives(archive_dir, now, archive_days, min_free_gb)
    finally:
        db.close()
    return result


def main() -> int:
    db_default = os.getenv("LOG_DB", "kalshi_log.sqlite")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=db_default)
    ap.add_argument("--archive-dir", default=os.getenv("ARCHIVE_DIR") or str(Path(db_default).parent / "archive"))
    ap.add_argument("--keep-hours", type=float, default=float(os.getenv("KEEP_HOURS", 6)))
    ap.add_argument("--archive-days", type=float, default=float(os.getenv("ARCHIVE_DAYS", 7)))
    ap.add_argument("--min-free-gb", type=float, default=float(os.getenv("MIN_FREE_GB", 8)))
    ap.add_argument("--batch", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true", help="only count rows that would be archived")
    a = ap.parse_args()
    t0 = time.time()
    res = run(a.db, Path(a.archive_dir), a.keep_hours, a.archive_days, a.min_free_gb, a.batch,
              dry_run=a.dry_run)
    log(f"{'would move' if a.dry_run else 'moved'} {res['moved']} rows, pruned {res['pruned']} archive files "
        f"in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
