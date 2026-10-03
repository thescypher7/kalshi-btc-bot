#!/usr/bin/env python3
"""First look at edge: does a simple price model beat Kalshi's own quotes in KXBTC15M, after taker fees?

For every settled market and at a few checkpoints before close (10, 7, 5, 3 minutes left) it:
  1. takes the latest BRTI tick as spot and the market's floor_strike (opening average) as the strike,
  2. estimates volatility from the last 10 minutes of 1 Hz ticks,
  3. computes a fair Yes probability  p = Phi((spot - strike) / (sigma * sqrt(secs_left - 40)))
     (the 40 s correction: the market settles on the 60 s average before close, not the last tick),
  4. reads Kalshi's latest ticker quote (yes_bid / yes_ask) at that moment,
  5. compares Brier scores (model vs market mid-price) and simulates buying one contract at the ask when
     the model's edge after the taker fee is above --min-edge.

This is a smoke test on a small sample, not proof of edge. Read-only: SELECTs and file reads.
Fee model: 0.07 * P * (1 - P) per contract, unrounded (what large orders approach; a single contract rounds up).
Ignores: queue position, partial fills, quote staleness beyond 120 s (such rows are skipped), maker orders.

  python analysis/edge_probe.py --db /var/lib/kalshi-bot/kalshi_log.sqlite [--min-edge 0.03] [--csv out.csv]
"""
import argparse
import bisect
import csv
import json
import math
import os
import sqlite3
import sys
from pathlib import Path
from statistics import mean, stdev

import validate_settlements as vs

CHECKPOINTS = (600, 420, 300, 180)     # seconds before close
SIGMA_WINDOW_S = 600
QUOTE_MAX_AGE_S = 120
SPOT_MAX_AGE_S = 5
FEE_RATE = 0.07
AVG_WINDOW_CORRECTION_S = 40.0         # 60 s average of a random walk: variance of (tau - 60) + 60/3 seconds


def norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def taker_fee(price: float) -> float:
    return FEE_RATE * price * (1 - price)


def fair_value(spot: float, strike: float, sigma: float, secs_left: float) -> float:
    eff = secs_left - AVG_WINDOW_CORRECTION_S
    if eff <= 0 or sigma <= 0:
        return 1.0 if spot >= strike else 0.0
    return norm_cdf((spot - strike) / (sigma * math.sqrt(eff)))


def _price(msg: dict, name: str):
    v = msg.get(name + "_dollars")
    if v not in (None, ""):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    v = msg.get(name)
    if v in (None, ""):
        return None
    try:
        return float(v) / 100.0              # legacy integer cents
    except (TypeError, ValueError):
        return None


def parse_quote(payload):
    """(yes_bid, yes_ask) in dollars from a logged ticker frame; either may be None. None if unparseable."""
    try:
        d = json.loads(payload)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    msg = d.get("msg") if isinstance(d.get("msg"), dict) else d
    bid, ask = _price(msg, "yes_bid"), _price(msg, "yes_ask")
    if bid is None and ask is None:
        return None
    return bid, ask


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# ---------- loading ----------
def load_ticks(db: sqlite3.Connection, archive_dir: Path):
    """Sorted (timestamps_ms, values) of 1 Hz BRTI ticks, deduped by row id across live DB and archives."""
    seen, out = set(), []

    def add(row_id, recv_ms, source_ts, kalshi_recv, value):
        if value is None:
            return
        if row_id is not None:
            if row_id in seen:
                return
            seen.add(row_id)
        out.append((source_ts or kalshi_recv or recv_ms, value))

    for row in db.execute("SELECT rowid, recv_ms, source_ts_ms, kalshi_recv_ms, value FROM brti WHERE channel=?",
                          (vs.BRTI_CHANNEL,)):
        add(*row)
    for p in vs.archive_files(archive_dir, "brti"):
        for rec in vs.read_archive(p, must_contain=f'"channel":"{vs.BRTI_CHANNEL}"'):
            if rec.get("channel") == vs.BRTI_CHANNEL:
                add(rec.get("id"), rec.get("recv_ms"), rec.get("source_ts_ms"), rec.get("kalshi_recv_ms"), rec.get("value"))
    out.sort()
    return [t for t, _ in out], [v for _, v in out]


def load_quotes(db: sqlite3.Connection, archive_dir: Path):
    """ticker -> sorted [(recv_ms, bid, ask)], plus counters for a sanity line."""
    quotes: dict[str, list] = {}
    stats = {"seen": 0, "parsed": 0, "sample": None}
    seen: set[int] = set()

    def add(row_id, recv_ms, ticker, payload):
        if row_id is not None:
            if row_id in seen:
                return
            seen.add(row_id)
        stats["seen"] += 1
        if stats["sample"] is None:
            stats["sample"] = str(payload)[:300]
        q = parse_quote(payload)
        if q and ticker:
            stats["parsed"] += 1
            quotes.setdefault(ticker, []).append((recv_ms, q[0], q[1]))

    for row in db.execute("SELECT rowid, recv_ms, ticker, payload FROM raw WHERE kind='ticker'"):
        add(*row)
    for p in vs.archive_files(archive_dir, "raw"):
        for rec in vs.read_archive(p, must_contain='"kind":"ticker"'):
            if rec.get("kind") == "ticker":
                add(rec.get("id"), rec.get("recv_ms"), rec.get("ticker"), rec.get("payload"))
    for lst in quotes.values():
        lst.sort()
    return quotes, stats


# ---------- model inputs ----------
def sigma_per_sqrt_s(ts: list, vals: list, t_ms: int):
    """Std dev of 1-second price changes (dollars per sqrt(second)) over the trailing window, or None."""
    lo = bisect.bisect_left(ts, t_ms - SIGMA_WINDOW_S * 1000)
    hi = bisect.bisect_right(ts, t_ms)
    z = []
    for i in range(lo + 1, hi):
        dt = (ts[i] - ts[i - 1]) / 1000.0
        if 0.5 <= dt <= 3.0:
            z.append((vals[i] - vals[i - 1]) / math.sqrt(dt))
    if len(z) < 60:
        return None
    return math.sqrt(sum(x * x for x in z) / len(z))     # zero-mean RMS: drift over 1 s is negligible


def spot_at(ts: list, vals: list, t_ms: int):
    i = bisect.bisect_right(ts, t_ms) - 1
    if i < 0 or t_ms - ts[i] > SPOT_MAX_AGE_S * 1000:
        return None
    return vals[i]


def quote_at(quotes: dict, ticker: str, t_ms: int):
    lst = quotes.get(ticker) or []
    i = bisect.bisect_right(lst, (t_ms, float("inf"), float("inf"))) - 1
    if i < 0 or t_ms - lst[i][0] > QUOTE_MAX_AGE_S * 1000:
        return None
    return lst[i][1], lst[i][2], (t_ms - lst[i][0]) / 1000.0


# ---------- analysis ----------
def analyze(db_path: str, archive_dir: Path, checkpoints=CHECKPOINTS):
    db = sqlite3.connect(db_path, timeout=60)
    try:
        log("loading settled markets ...")
        markets = vs.load_markets(db, archive_dir)
        log(f"  {len(markets)} settled markets; loading BRTI ticks ...")
        ts, vals = load_ticks(db, archive_dir)
        log(f"  {len(ts)} ticks; loading ticker quotes ...")
        quotes, qstats = load_quotes(db, archive_dir)
        log(f"  {qstats['parsed']} usable quotes of {qstats['seen']} ticker messages")
    finally:
        db.close()

    rows = []
    for ticker, m in sorted(markets.items(), key=lambda kv: kv[1]["close_time"]):
        if m.get("floor_strike") is None:
            continue
        strike, close_ms, yes = float(m["floor_strike"]), vs.parse_ts(m["close_time"]), m["result"] == "yes"
        for secs in checkpoints:
            t_ms = close_ms - secs * 1000
            spot = spot_at(ts, vals, t_ms)
            sigma = sigma_per_sqrt_s(ts, vals, t_ms)
            if spot is None or sigma is None:
                continue
            q = quote_at(quotes, ticker, t_ms)
            bid, ask, age = q if q else (None, None, None)
            rows.append({"ticker": ticker, "secs_left": secs, "spot": spot, "strike": strike, "sigma": sigma,
                         "p": fair_value(spot, strike, sigma, secs), "bid": bid, "ask": ask, "quote_age_s": age,
                         "outcome": 1 if yes else 0})
    return rows, {"markets": len(markets), "ticks": len(ts), **qstats}


def best_trade(r: dict, min_edge: float):
    """(side, price, edge) for the better of buy-Yes-at-ask / buy-No-at-(1-bid), or None below min_edge."""
    opts = []
    if r["ask"] is not None and 0 < r["ask"] < 1:
        opts.append(("yes", r["ask"], r["p"] - r["ask"] - taker_fee(r["ask"])))
    if r["bid"] is not None and 0 < r["bid"] < 1:
        price = 1 - r["bid"]
        opts.append(("no", price, (1 - r["p"]) - price - taker_fee(price)))
    if not opts:
        return None
    side, price, edge = max(opts, key=lambda o: o[2])
    return (side, price, edge) if edge > min_edge else None


def trade_pnl(r: dict, side: str, price: float) -> float:
    win = r["outcome"] == 1 if side == "yes" else r["outcome"] == 0
    return (1.0 if win else 0.0) - price - taker_fee(price)


def _stat_line(label: str, pnls: list) -> str:
    n = len(pnls)
    if n == 0:
        return f"  {label}: no trades"
    m = mean(pnls)
    if n < 2:
        return f"  {label}: 1 trade, pnl {m * 100:+.1f}c"
    se = stdev(pnls) / math.sqrt(n)
    return f"  {label}: {n} trades, mean {m * 100:+.1f}c per contract (+/- {se * 100:.1f}c std err), win {sum(p > 0 for p in pnls)}/{n}"


def summarize(rows: list, info: dict, min_edge: float = 0.03) -> str:
    out = [f"Settled markets: {info['markets']}   BRTI ticks: {info['ticks']}   "
           f"usable quotes: {info['parsed']} of {info['seen']} ticker messages"]
    if info["parsed"] == 0:
        out.append("\nNo usable yes_bid/yes_ask found in ticker messages, so market prices could not be compared.")
        if info.get("sample"):
            out.append(f"First ticker payload seen (so the parser can be fixed): {info['sample']}")
        return "\n".join(out)
    q = [r for r in rows if r["bid"] is not None and r["ask"] is not None]
    out.append(f"Rows with model inputs: {len(rows)}; with a fresh two-sided quote: {len(q)}\n")
    out.append("Probability accuracy (Brier score, lower is better; 0.25 = always guessing 50/50)")
    for secs in sorted({r["secs_left"] for r in q}, reverse=True):
        s = [r for r in q if r["secs_left"] == secs]
        bm = mean((r["p"] - r["outcome"]) ** 2 for r in s)
        bk = mean((((r["bid"] + r["ask"]) / 2) - r["outcome"]) ** 2 for r in s)
        spread = mean(r["ask"] - r["bid"] for r in s) * 100
        out.append(f"  {secs // 60:>2} min left  n={len(s):<4} model {bm:.4f}   market mid {bk:.4f}   avg spread {spread:.1f}c")
    out.append(f"\nSimulated taker trades: buy one contract when model edge after fee > {min_edge * 100:.0f}c")
    first: dict[str, float] = {}
    for secs in sorted({r["secs_left"] for r in q}, reverse=True):
        pnls = []
        for r in (x for x in q if x["secs_left"] == secs):
            t = best_trade(r, min_edge)
            if t:
                pnl = trade_pnl(r, t[0], t[1])
                pnls.append(pnl)
                first.setdefault(r["ticker"], pnl)
        out.append(_stat_line(f"{secs // 60:>2} min left", pnls))
    out.append(_stat_line("first signal per market (independent samples)", list(first.values())))
    out.append("\nCaution: one day of data, trades within a market are correlated, and a positive result here would "
               "still need a bigger sample, an out-of-sample check and realistic fills before any real money.")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.getenv("LOG_DB", "kalshi_log.sqlite"))
    ap.add_argument("--archive-dir", default=None, help="default: an 'archive' folder next to --db")
    ap.add_argument("--min-edge", type=float, default=0.03, help="minimum edge after fee, in dollars (0.03 = 3c)")
    ap.add_argument("--csv", help="also write one row per market/checkpoint to this file")
    a = ap.parse_args(argv)
    archive = vs.resolve_archive_dir(a.db, a.archive_dir)
    print(f"Read: {a.db} + {len(vs.archive_files(archive, 'raw'))} raw / {len(vs.archive_files(archive, 'brti'))} "
          f"brti archive files in {archive}")
    rows, info = analyze(a.db, archive)
    print(summarize(rows, info, a.min_edge))
    if a.csv and rows:
        with open(a.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {a.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
