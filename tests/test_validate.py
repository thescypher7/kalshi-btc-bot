"""validate_settlements tests: synthetic markets, ticks split across the live DB and an archive, duplicates."""
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import kalshi_logger as kl
import validate_settlements as vs


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp() * 1000)


def tick(ts, value, channel="cfbenchmarks_value"):
    # (recv_ms, channel, source_ts_ms, kalshi_recv_ms, sending_ts_ms, value, avg60, avg60_n, avg15, avg15_n)
    return (ts + 50, channel, ts, ts + 20, ts + 30, value, None, None, None, None)


def window(start_iso, value, seconds=60, offset=0):
    t0 = ms(start_iso)
    return [tick(t0 + (offset + i) * 1000, value) for i in range(seconds)]


def market(open_iso, close_iso, floor, exp, result):
    return {"market": {"open_time": open_iso, "close_time": close_iso, "floor_strike": floor,
                       "expiration_value": f"{exp:.2f}" if exp is not None else "", "result": result,
                       "strike_type": "greater_or_equal"}}


def write_gz(path, records):
    with gzip.open(path, "wt") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def build(tmp_path, gap=False):
    """A: 21:00-21:15 Yes (100.0 -> 101.5).  B: 21:15-21:30 No (101.5 -> 100.0).  C: unsettled."""
    db_path = tmp_path / "t.sqlite"
    arch = tmp_path / "archive"
    arch.mkdir()
    s = kl.Store(str(db_path))
    brti = []
    brti += window("2026-10-01T20:59:00Z", 100.0)                      # ids 1-60   window before A's open
    brti += window("2026-10-01T21:14:00Z", 101.5, seconds=30)           # ids 61-90  first half of A's close window
    brti += window("2026-10-01T21:29:00Z", 100.0, seconds=0 if gap else 60)   # B's close window
    brti += window("2026-10-01T21:14:00Z", 999.0)                       # decoys: wrong channel, set below
    brti = brti[:-60] + [tick(t[2], 999.0, "cfbenchmarks_value_5hz") for t in brti[-60:]]
    s.write([], brti)
    # B settled in the live DB; an earlier poll of A without a result must not count
    s.db.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?)", [
        (1, 1, "rest", SETTLED, "T-B", json.dumps(market("2026-10-01T21:15:00Z", "2026-10-01T21:30:00Z", 101.5, 100.0, "no"))),
        (2, 2, "rest", SETTLED, "T-A", json.dumps(market("2026-10-01T21:00:00Z", "2026-10-01T21:15:00Z", 100.0, None, ""))),
        (3, 3, "rest", SETTLED, "T-C", json.dumps(market("2026-10-01T21:30:00Z", "2026-10-01T21:45:00Z", 100.0, None, ""))),
    ])
    s.db.commit()
    s.db.close()
    # archive: second half of A's close window (ids 1000+), plus duplicates of ids 61-70 (crash during archiving)
    second_half = window("2026-10-01T21:14:00Z", 101.5, seconds=30, offset=30)
    recs = [dict(zip(vs_cols(), r), id=1000 + i) for i, r in enumerate(second_half)]
    dups = [dict(zip(vs_cols(), r), id=61 + i) for i, r in enumerate(window("2026-10-01T21:14:00Z", 101.5, seconds=10))]
    write_gz(arch / "brti_20261001T21.jsonl.gz", recs + dups)
    write_gz(arch / "raw_20261001T21.jsonl.gz", [
        {"id": 10, "recv_ms": 1, "mono_ns": 1, "src": "ws", "kind": "orderbook_delta", "ticker": "T-A", "payload": "{}"},
        {"id": 11, "recv_ms": 2, "mono_ns": 2, "src": "rest", "kind": SETTLED, "ticker": "T-A",
         "payload": json.dumps(market("2026-10-01T21:00:00Z", "2026-10-01T21:15:00Z", 100.0, 101.5, "yes"))},
    ])
    return str(db_path), arch


SETTLED = vs.SETTLED_KIND


def vs_cols():
    return ["recv_ms", "channel", "source_ts_ms", "kalshi_recv_ms", "sending_ts_ms", "value", "avg60", "avg60_n", "avg15", "avg15_n"]


def test_validates_markets_across_live_db_and_archive(tmp_path):
    db, arch = build(tmp_path)
    rows = vs.validate(db, arch)
    assert [r["ticker"] for r in rows] == ["T-A", "T-B"]                # unsettled T-C ignored, ordered by close
    a, b = rows
    assert (a["result"], b["result"]) == ("yes", "no")
    assert a["rule_ok"] is True and b["rule_ok"] is True
    assert a["ours_ok"] is True and b["ours_ok"] is True
    # A's close window = 30 live ticks + 30 archived ticks; the 10 duplicates are not double counted
    assert (a["n_open"], a["n_close"]) == (60, 60)
    assert a["our_open"] == 100.0 and a["our_close"] == 101.5
    assert abs(a["close_diff"]) < 1e-9 and abs(a["open_diff"]) < 1e-9
    # B opens exactly where A closes, so it shares that window
    assert (b["n_open"], b["n_close"]) == (60, 60) and b["our_open"] == 101.5 and b["our_close"] == 100.0
    # the 5 Hz decoys (value 999) were ignored
    assert max(r["our_close"] for r in rows) < 200


def test_summary_reports_clean_run(tmp_path):
    db, arch = build(tmp_path)
    text = vs.summarize(vs.validate(db, arch))
    assert "Settled markets checked: 2" in text
    assert "matches expiration_value >= floor_strike: 2/2" in text
    assert "same Yes/No as Kalshi:            2/2" in text
    assert "complete (>= 60 ticks):                2/2" in text
    assert "disagreement" not in text and "fewer than 55" not in text


def test_gap_in_ticks_is_flagged(tmp_path):
    db, arch = build(tmp_path, gap=True)
    rows = vs.validate(db, arch)
    b = rows[1]
    assert b["n_close"] == 0 and b["our_close"] is None and b["ours_ok"] is None
    assert "fewer than 55 ticks" in vs.summarize(rows)


def test_disagreement_is_flagged(tmp_path):
    db, arch = build(tmp_path)
    rows = vs.validate(db, arch)
    rows[0]["ours_ok"] = False                          # pretend our data disagreed with Kalshi
    assert "disagreement" in vs.summarize(rows)


def test_no_settled_markets(tmp_path):
    s = kl.Store(str(tmp_path / "e.sqlite"))
    s.db.close()
    assert vs.validate(str(tmp_path / "e.sqlite"), tmp_path / "none") == []
    assert "No settled markets" in vs.summarize([])


# ---------- command line: the archive folder must follow --db (regression: it used to follow $LOG_DB only) ----------
def test_cli_finds_archive_next_to_db_without_env_vars(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("LOG_DB", raising=False)
    monkeypatch.chdir(tmp_path.parent)                      # a cwd with no ./archive, like running from /root
    db, _ = build(tmp_path)
    assert vs.main(["--db", db]) == 0
    out = capsys.readouterr()
    assert "Settled markets checked: 2" in out.out          # T-A exists ONLY in the archive, so this proves it was read
    assert "1 raw / 1 brti archive files" in out.out
    assert "WARNING" not in out.err


def test_cli_warns_when_no_archive_files_found(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("ARCHIVE_DIR", raising=False)
    s = kl.Store(str(tmp_path / "x.sqlite"))
    s.db.close()
    vs.main(["--db", str(tmp_path / "x.sqlite")])
    assert "WARNING: no archive files found" in capsys.readouterr().err


def test_resolve_archive_dir_precedence(monkeypatch):
    monkeypatch.delenv("ARCHIVE_DIR", raising=False)
    assert vs.resolve_archive_dir("/data/x.sqlite") == Path("/data/archive")
    monkeypatch.setenv("ARCHIVE_DIR", "/env/arch")
    assert vs.resolve_archive_dir("/data/x.sqlite") == Path("/env/arch")
    assert vs.resolve_archive_dir("/data/x.sqlite", "/cli/arch") == Path("/cli/arch")
