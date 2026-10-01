"""Retention job tests: archive-then-delete, hour bucketing, idempotence, pruning, flusher requeue."""
import asyncio
import gzip
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import kalshi_logger as kl
import retention as rt

NOW = datetime(2026, 10, 1, 12, 30, tzinfo=timezone.utc)


def ms(dt):
    return int(dt.timestamp() * 1000)


def make_db(path, n_old=30, n_new=10):
    s = kl.Store(str(path))
    old_base = ms(NOW - timedelta(hours=10))      # ~02:30 UTC, older than keep window
    new_base = ms(NOW - timedelta(hours=1))
    raw, brti = [], []
    for i in range(n_old):
        raw.append((old_base + i * 300_000, i, "ws", "orderbook_delta", "KXBTC15M-T", json.dumps({"i": i})))
        brti.append((old_base + i * 300_000, "cfbenchmarks_value", None, None, None, 68000.0 + i, None, None, None, None))
    for i in range(n_new):
        raw.append((new_base + i * 1000, 1000 + i, "ws", "ticker", "KXBTC15M-T", "{}"))
        brti.append((new_base + i * 1000, "cfbenchmarks_value", None, None, None, 70000.0, None, None, None, None))
    s.write(raw, brti)
    s.db.close()


def read_archive(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f if line.strip()]


def count(db, table):
    return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def run(tmp_path, **kw):
    args = dict(keep_hours=6, archive_days=7, min_free_gb=0, batch=7, now=NOW)
    args.update(kw)
    return rt.run(str(tmp_path / "t.sqlite"), tmp_path / "archive", **args)


def test_moves_old_rows_keeps_recent(tmp_path):
    make_db(tmp_path / "t.sqlite")
    res = run(tmp_path)
    assert res["moved"] == {"raw": 30, "brti": 30}
    db = sqlite3.connect(tmp_path / "t.sqlite")
    assert count(db, "raw") == 10 and count(db, "brti") == 10          # only the recent rows remain
    assert db.execute("SELECT MIN(recv_ms) FROM raw").fetchone()[0] >= res["cutoff_ms"]


def test_archive_is_complete_and_hour_bucketed(tmp_path):
    make_db(tmp_path / "t.sqlite")
    run(tmp_path)
    files = sorted(p.name for p in (tmp_path / "archive").iterdir())
    assert files == [f"{t}_20261001T0{h}.jsonl.gz" for t in ("brti", "raw") for h in (2, 3, 4)]
    recs = sum((read_archive(tmp_path / "archive" / f"raw_20261001T0{h}.jsonl.gz") for h in (2, 3, 4)), [])
    assert len(recs) == 30
    assert sorted(r["id"] for r in recs) == list(range(1, 31))          # rowids preserved for dedupe
    assert {r["kind"] for r in recs} == {"orderbook_delta"}
    assert recs[0]["payload"] == json.dumps({"i": 0}) and recs[0]["ticker"] == "KXBTC15M-T"


def test_second_run_is_a_noop(tmp_path):
    make_db(tmp_path / "t.sqlite")
    run(tmp_path)
    before = {p.name: p.stat().st_size for p in (tmp_path / "archive").iterdir()}
    res = run(tmp_path)
    assert res["moved"] == {"raw": 0, "brti": 0}
    assert before == {p.name: p.stat().st_size for p in (tmp_path / "archive").iterdir()}


def test_dry_run_changes_nothing(tmp_path):
    make_db(tmp_path / "t.sqlite")
    res = run(tmp_path, dry_run=True)
    assert res["moved"] == {"raw": 30, "brti": 30}
    db = sqlite3.connect(tmp_path / "t.sqlite")
    assert count(db, "raw") == 40
    assert list((tmp_path / "archive").iterdir()) == []


def test_crash_between_archive_and_delete_leaves_data_recoverable(tmp_path, monkeypatch):
    """If the delete step dies, rows are still in the DB (nothing lost); re-running archives them again."""
    make_db(tmp_path / "t.sqlite")

    def boom(*a, **k):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(rt.time, "sleep", boom)      # sleep happens right after the first delete chunk
    try:
        run(tmp_path)
    except RuntimeError:
        pass
    monkeypatch.undo()
    db = sqlite3.connect(tmp_path / "t.sqlite")
    assert count(db, "raw") >= 40 - rt.DELETE_CHUNK    # at most one chunk deleted, all of it archived first
    run(tmp_path)
    recs = sum((read_archive(tmp_path / "archive" / f"raw_20261001T0{h}.jsonl.gz") for h in (2, 3, 4)), [])
    assert {r["id"] for r in recs} == set(range(1, 31))   # nothing missing (duplicates allowed)


def test_prune_by_age(tmp_path):
    arch = tmp_path / "archive"
    arch.mkdir()
    old = arch / "raw_20260920T05.jsonl.gz"
    fresh = arch / "raw_20260930T05.jsonl.gz"
    other = arch / "notes.txt"
    for p in (old, fresh, other):
        p.write_bytes(b"x")
    assert rt.prune_archives(arch, NOW, archive_days=7, min_free_gb=0) == 1
    assert not old.exists() and fresh.exists() and other.exists()   # never touches unrelated files


def test_disk_guard_deletes_oldest_first(tmp_path, monkeypatch):
    arch = tmp_path / "archive"
    arch.mkdir()
    names = ["raw_20260929T01.jsonl.gz", "raw_20260930T01.jsonl.gz", "raw_20260930T02.jsonl.gz"]
    for n in names:
        (arch / n).write_bytes(b"x")
    free = iter([1e9, 1e9, 50e9])                    # low, low, then enough
    monkeypatch.setattr(rt.shutil, "disk_usage", lambda p: type("U", (), {"free": next(free)})())
    assert rt.prune_archives(arch, NOW, archive_days=7, min_free_gb=8) == 2
    assert [p.name for p in arch.iterdir()] == [names[2]]


def test_flusher_requeues_rows_when_db_is_locked(tmp_path):
    async def go():
        s = kl.Store(str(tmp_path / "f.sqlite"))
        s.log("ws", "ticker", "T", "{}")
        real = s.write
        calls = {"n": 0}

        def flaky(raw, brti):
            calls["n"] += 1
            if calls["n"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return real(raw, brti)

        s.write = flaky
        stop = asyncio.Event()
        task = asyncio.create_task(kl.flusher(s, stop))
        await asyncio.sleep(2.5)
        stop.set()
        await task
        return s

    s = asyncio.run(go())
    assert s.db.execute("SELECT COUNT(*) FROM raw").fetchone()[0] == 1   # retried once, stored exactly once
