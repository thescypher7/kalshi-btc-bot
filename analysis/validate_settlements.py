#!/usr/bin/env python3
"""Check the logged data against Kalshi's settlement rule, market by market.

Kalshi KXBTC15M rule (from the market's own rules_primary text): the market resolves Yes when the simple
average of the 60 seconds of BRTI before CLOSE is at least the simple average of the 60 seconds of BRTI before
OPEN. Kalshi publishes both numbers on the settled market: floor_strike (the opening average) and
expiration_value (the closing average).

For every settled market this script recomputes both averages from OUR logged 1 Hz BRTI ticks and compares:
  rule_ok     Kalshi's result agrees with expiration_value >= floor_strike
  close_diff  our closing average minus Kalshi's expiration_value
  open_diff   our opening average minus Kalshi's floor_strike
  ours_ok     our own two averages give the same Yes/No as Kalshi's result
and reports how many ticks we had in each 60 s window (60 is complete).

Reads the live SQLite DB and the hourly archives written by retention.py. Records that appear in both
(after a crash during archiving) are counted once, using the SQLite row id the archive carries.
Read-only: it only runs SELECTs and reads files.

  python analysis/validate_settlements.py --db /var/lib/kalshi-bot/kalshi_log.sqlite [--csv out.csv]
"""
import argparse
import bisect
import csv
import gzip
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean, median

WINDOW_MS = 60_000
BRTI_CHANNEL = "cfbenchmarks_value"        # the 1 Hz channel; the 5 Hz one is not what Kalshi settles on
SETTLED_KIND = "market_after_close"


def parse_ts(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def archive_files(archive_dir: Path, table: str):
    if not archive_dir.is_dir():
        return []
    return sorted(p for p in archive_dir.iterdir() if p.name.startswith(table + "_") and p.name.endswith(".jsonl.gz"))


def read_archive(path: Path, must_contain: str | None = None):
    """Yield dict records. `must_contain` is a cheap substring filter applied before JSON parsing."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if must_contain and must_contain not in line:
                continue
            line = line.strip()
            if line:
                yield json.loads(line)


# ---------- settled markets ----------
def load_markets(db: sqlite3.Connection, archive_dir: Path) -> dict:
    """ticker -> market dict, for markets whose result is yes/no (the last such record wins)."""
    markets: dict[str, dict] = {}

    def consider(ticker, payload):
        try:
            m = json.loads(payload).get("market") or {}
        except (TypeError, ValueError):
            return
        if m.get("result") in ("yes", "no") and ticker:
            markets[ticker] = m

    for ticker, payload in db.execute("SELECT ticker, payload FROM raw WHERE kind=? ORDER BY recv_ms", (SETTLED_KIND,)):
        consider(ticker, payload)
    for p in archive_files(archive_dir, "raw"):
        for rec in read_archive(p, must_contain=f'"{SETTLED_KIND}"'):
            if rec.get("kind") == SETTLED_KIND:
                consider(rec.get("ticker"), rec.get("payload"))
    return markets


# ---------- BRTI ticks grouped into the 60 s windows we need ----------
def collect_window_ticks(db: sqlite3.Connection, archive_dir: Path, starts: list[int]) -> dict:
    """window start (ms) -> list of values, for ticks with start <= ts < start + 60 s."""
    ticks: dict[int, list[float]] = {s: [] for s in starts}
    seen: set[int] = set()

    def add(row_id, recv_ms, source_ts, kalshi_recv, value):
        if value is None:
            return
        if row_id is not None:
            if row_id in seen:
                return
            seen.add(row_id)
        ts = source_ts or kalshi_recv or recv_ms
        i = bisect.bisect_right(starts, ts) - 1
        if i >= 0 and ts < starts[i] + WINDOW_MS:
            ticks[starts[i]].append(value)

    for row in db.execute("SELECT rowid, recv_ms, source_ts_ms, kalshi_recv_ms, value FROM brti WHERE channel=?",
                          (BRTI_CHANNEL,)):
        add(*row)
    for p in archive_files(archive_dir, "brti"):
        for rec in read_archive(p):
            if rec.get("channel") == BRTI_CHANNEL:
                add(rec.get("id"), rec.get("recv_ms"), rec.get("source_ts_ms"), rec.get("kalshi_recv_ms"), rec.get("value"))
    return ticks


def validate(db_path: str, archive_dir: Path) -> list[dict]:
    db = sqlite3.connect(db_path, timeout=60)
    try:
        markets = load_markets(db, archive_dir)
        starts = sorted({parse_ts(m[k]) - WINDOW_MS for m in markets.values() for k in ("open_time", "close_time")})
        ticks = collect_window_ticks(db, archive_dir, starts)
    finally:
        db.close()

    rows = []
    for ticker, m in sorted(markets.items(), key=lambda kv: kv[1]["close_time"]):
        open_ms, close_ms = parse_ts(m["open_time"]), parse_ts(m["close_time"])
        o, c = ticks[open_ms - WINDOW_MS], ticks[close_ms - WINDOW_MS]
        floor = float(m["floor_strike"]) if m.get("floor_strike") is not None else None
        exp = float(m["expiration_value"]) if m.get("expiration_value") not in (None, "") else None
        our_open = mean(o) if o else None
        our_close = mean(c) if c else None
        yes = m["result"] == "yes"
        rows.append({
            "ticker": ticker, "close_time": m["close_time"], "result": m["result"],
            "floor_strike": floor, "expiration_value": exp,
            "rule_ok": None if floor is None or exp is None else ((exp >= floor) == yes),
            "n_open": len(o), "n_close": len(c),
            "our_open": our_open, "our_close": our_close,
            "open_diff": None if our_open is None or floor is None else our_open - floor,
            "close_diff": None if our_close is None or exp is None else our_close - exp,
            "ours_ok": None if our_open is None or our_close is None else ((our_close >= our_open) == yes),
        })
    return rows


def summarize(rows: list[dict]) -> str:
    n = len(rows)
    if n == 0:
        return "No settled markets found (market_after_close rows with a yes/no result)."

    def count(pred):
        return sum(1 for r in rows if pred(r))

    def stats(key):
        vals = [abs(r[key]) for r in rows if r[key] is not None]
        if not vals:
            return "n/a"
        return (f"median ${median(vals):.2f}, max ${max(vals):.2f}, "
                f"within 2c: {sum(v <= 0.02 for v in vals)}/{len(vals)}, within $1: {sum(v <= 1 for v in vals)}/{len(vals)}")

    complete = count(lambda r: r["n_open"] >= 60 and r["n_close"] >= 60)
    short = [r for r in rows if r["n_open"] < 55 or r["n_close"] < 55]
    out = [
        f"Settled markets checked: {n}  ({rows[0]['close_time']} .. {rows[-1]['close_time']})",
        f"Kalshi result matches expiration_value >= floor_strike: {count(lambda r: r['rule_ok'] is True)}/{n}",
        f"Our averages give the same Yes/No as Kalshi:            {count(lambda r: r['ours_ok'] is True)}/{n}",
        f"Both 60 s windows complete (>= 60 ticks):                {complete}/{n}",
        f"Our closing average vs expiration_value: {stats('close_diff')}",
        f"Our opening average vs floor_strike:     {stats('open_diff')}",
    ]
    bad = [r for r in rows if r["ours_ok"] is False or r["rule_ok"] is False]
    if bad:
        out.append("\nMarkets to look at (disagreement):")
        out += [f"  {r['ticker']} result={r['result']} kalshi {r['expiration_value']} vs {r['floor_strike']}, "
                f"ours {r['our_close']} vs {r['our_open']} (ticks {r['n_close']}/{r['n_open']})" for r in bad[:15]]
    if short:
        out.append(f"\n{len(short)} market(s) have a window with fewer than 55 ticks (a gap in our data), e.g.:")
        out += [f"  {r['ticker']} ticks close/open = {r['n_close']}/{r['n_open']}" for r in short[:10]]
    return "\n".join(out)


def resolve_archive_dir(db_path: str, archive_dir: str | None = None) -> Path:
    """--archive-dir wins, then $ARCHIVE_DIR, then an `archive` folder next to the database file."""
    return Path(archive_dir or os.getenv("ARCHIVE_DIR") or Path(db_path).parent / "archive")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.getenv("LOG_DB", "kalshi_log.sqlite"))
    ap.add_argument("--archive-dir", default=None, help="default: an 'archive' folder next to --db")
    ap.add_argument("--csv", help="also write one row per market to this file")
    a = ap.parse_args(argv)
    archive = resolve_archive_dir(a.db, a.archive_dir)
    n_raw, n_brti = len(archive_files(archive, "raw")), len(archive_files(archive, "brti"))
    print(f"Read: {a.db} + {n_raw} raw / {n_brti} brti archive files in {archive}")
    if not archive.is_dir() or not (n_raw or n_brti):
        print(f"WARNING: no archive files found in {archive}; only the live database was checked, "
              f"so older markets are missing and their windows will look incomplete.", file=sys.stderr)
    rows = validate(a.db, archive)
    print(summarize(rows))
    if a.csv and rows:
        with open(a.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {a.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
