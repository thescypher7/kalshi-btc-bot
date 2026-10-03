#!/usr/bin/env python3
"""Second edge scan: three sharper questions than edge_probe, on a dense grid of sample times.

Every 10 s from 10 minutes down to 10 seconds before close, for every settled market, it builds one row:
our model's Yes probability, Kalshi's two-sided quote at that moment, and the outcome. The model is the same
random-walk model as edge_probe for 60+ s left, and an exact version for the last 60 s (part of the 60 s closing
average is already known by then, which the market may or may not price in).

  1. Calibration: when Kalshi's mid-price says 90c, does Yes win 90% of the time? (favorite/longshot bias)
  2. Information: does the model add anything to the market price? A blend weight is fitted on half the markets
     and scored on the other half, so a weight above 0 only counts if it helps out of sample.
  3. Trades: buy one contract at the ask when the model's edge after fee beats a threshold, by threshold, by
     time left and by how stale the quote is. One trade per market per row of the table, so samples are independent.

All standard errors are clustered by market: rows from one market are strongly correlated, so counting them
separately would overstate the evidence. This is still a small sample; treat anything under ~2 standard errors
as noise. Read-only.

  python analysis/edge_scan.py --db /var/lib/kalshi-bot/kalshi_log.sqlite --cache /var/lib/kalshi-bot/edge_cache.json.gz
(--cache saves the loaded data so reruns take seconds; add --refresh to rebuild it from the database.)
"""
import argparse
import bisect
import csv
import gzip
import json
import math
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev

import edge_probe as ep
import validate_settlements as vs

SCAN_START_S, SCAN_END_S, STEP_S = 600, 10, 10
TIME_BUCKETS = ((600, 310), (300, 130), (120, 10))          # (secs left from, to), both ends included
AGE_BUCKETS = ((0, 5), (5, 30), (30, 120))                  # quote age in seconds, lower end included
MID_EDGES = (0.0, 0.05, 0.15, 0.35, 0.65, 0.85, 0.95, 1.0001)
THRESHOLDS = (0.02, 0.03, 0.05, 0.08)


# ---------- model ----------
def fair_value_late(known_mean: float, spot: float, strike: float, sigma: float, secs_left: float) -> float:
    """P(closing 60 s average >= strike) when `secs_left` < 60 s remain: part of the average is already known."""
    w = (60.0 - secs_left) / 60.0
    mean_avg = w * known_mean + (1 - w) * spot
    sd = (secs_left / 60.0) * sigma * math.sqrt(secs_left / 3.0)
    if sd <= 0:
        return 1.0 if mean_avg >= strike else 0.0
    return ep.norm_cdf((mean_avg - strike) / sd)


def model_prob(spot, strike, sigma, secs_left, known_mean=None) -> float:
    if secs_left >= 60 or known_mean is None:
        return ep.fair_value(spot, strike, sigma, secs_left)
    return fair_value_late(known_mean, spot, strike, sigma, secs_left)


# ---------- loading (optionally cached) ----------
def load_inputs(db_path: str, archive_dir: Path, cache: str | None = None, refresh: bool = False):
    """(markets, ts, vals, quotes, info). With `cache`, the loaded data is saved/reused as gzipped JSON."""
    if cache and Path(cache).exists() and not refresh:
        with gzip.open(cache, "rt", encoding="utf-8") as f:
            d = json.load(f)
        quotes = {t: [tuple(q) for q in qs] for t, qs in d["quotes"].items()}
        return d["markets"], d["ts"], d["vals"], quotes, {**d["info"], "from_cache": True}
    db = sqlite3.connect(db_path, timeout=60)
    try:
        ep.log("loading settled markets ...")
        markets = vs.load_markets(db, archive_dir)
        ep.log(f"  {len(markets)} settled markets; loading BRTI ticks ...")
        ts, vals = ep.load_ticks(db, archive_dir)
        ep.log(f"  {len(ts)} ticks; loading ticker quotes ...")
        quotes, qstats = ep.load_quotes(db, archive_dir)
        ep.log(f"  {qstats['parsed']} usable quotes of {qstats['seen']} ticker messages")
    finally:
        db.close()
    info = {"markets": len(markets), "ticks": len(ts), "quotes": qstats["parsed"], "seen": qstats["seen"]}
    if cache:
        tmp = f"{cache}.tmp"
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump({"markets": markets, "ts": ts, "vals": vals, "quotes": quotes, "info": info}, f)
        os.replace(tmp, cache)
        ep.log(f"  saved cache {cache}")
    return markets, ts, vals, quotes, info


# ---------- rows ----------
def build_rows(markets: dict, ts: list, vals: list, quotes: dict) -> list:
    rows = []
    ordered = sorted(markets.items(), key=lambda kv: kv[1]["close_time"])
    for idx, (ticker, m) in enumerate(ordered):
        if m.get("floor_strike") is None:
            continue
        strike, close_ms, yes = float(m["floor_strike"]), vs.parse_ts(m["close_time"]), m["result"] == "yes"
        qs = sorted(quotes.get(ticker, []), key=lambda q: q[0])
        qts = [q[0] for q in qs]
        win_lo = bisect.bisect_left(ts, close_ms - 60_000)
        for secs in range(SCAN_START_S, SCAN_END_S - 1, -STEP_S):
            t_ms = close_ms - secs * 1000
            spot, sigma = ep.spot_at(ts, vals, t_ms), ep.sigma_per_sqrt_s(ts, vals, t_ms)
            if spot is None or sigma is None:
                continue
            known_mean = None
            if secs < 60:
                seg = vals[win_lo:bisect.bisect_right(ts, t_ms)]
                if not seg:
                    continue
                known_mean = sum(seg) / len(seg)
            i = bisect.bisect_right(qts, t_ms) - 1
            if i < 0 or t_ms - qts[i] > ep.QUOTE_MAX_AGE_S * 1000:
                continue
            _, bid, ask = qs[i]
            if bid is None or ask is None:
                continue
            past = ep.spot_at(ts, vals, t_ms - 1_800_000)
            ret30 = (spot - past) / past * 1e4 if past else None          # trailing 30-minute move, in basis points
            rows.append({"mkt": idx, "ticker": ticker, "close_ms": close_ms, "secs_left": secs, "spot": spot, "strike": strike,
                         "sigma": sigma, "p": model_prob(spot, strike, sigma, secs, known_mean),
                         "bid": bid, "ask": ask, "mid": (bid + ask) / 2, "ret30": ret30, "quote_age_s": (t_ms - qts[i]) / 1000.0,
                         "outcome": 1 if yes else 0})
    return rows


# ---------- statistics (clustered by market) ----------
def cluster_stat(per_market: list):
    """(n_markets, mean, standard_error) of per-market values."""
    n = len(per_market)
    if n == 0:
        return 0, None, None
    return n, mean(per_market), (stdev(per_market) / math.sqrt(n) if n > 1 else None)


def per_market_mean(rows: list, f) -> list:
    groups: dict = {}
    for r in rows:
        groups.setdefault(r["mkt"], []).append(f(r))
    return [mean(v) for v in groups.values()]


def pm(m, se, scale=1.0, nd=1, unit="") -> str:
    if m is None:
        return "n/a"
    s = f"{m * scale:+.{nd}f}{unit}"
    return s + (f" +/- {se * scale:.{nd}f}{unit}" if se is not None else "")


def calibration(rows: list) -> list:
    out = []
    for lo, hi in zip(MID_EDGES[:-1], MID_EDGES[1:]):
        rs = [r for r in rows if lo <= r["mid"] < hi]
        n, g, se = cluster_stat(per_market_mean(rs, lambda r: r["outcome"] - r["mid"]))
        out.append({"lo": lo, "hi": min(hi, 1.0), "rows": len(rs), "markets": n,
                    "avg_mid": mean(r["mid"] for r in rs) if rs else None,
                    "win_rate": mean(r["outcome"] for r in rs) if rs else None, "gap": g, "se": se})
    return out


def brier_delta(rows: list, alt) -> tuple:
    """Per-market-clustered improvement of `alt(row)` over the market mid: mean(mid_err^2 - alt_err^2)."""
    return cluster_stat(per_market_mean(rows, lambda r: (r["mid"] - r["outcome"]) ** 2 - (alt(r) - r["outcome"]) ** 2))


def fit_blend_weight(rows: list) -> float:
    """w in [0,1] minimising the Brier score of w*model + (1-w)*mid (closed form)."""
    num = sum((r["mid"] - r["outcome"]) * (r["p"] - r["mid"]) for r in rows)
    den = sum((r["p"] - r["mid"]) ** 2 for r in rows)
    return 0.0 if den == 0 else min(1.0, max(0.0, -num / den))


def blend_out_of_sample(rows: list):
    """Fit the blend weight on one half of the markets, score on the other (both ways). Returns (w_a, w_b, stat)."""
    ids = sorted({r["mkt"] for r in rows})
    if len(ids) < 4:
        return None
    cut = ids[len(ids) // 2]
    a, b = [r for r in rows if r["mkt"] < cut], [r for r in rows if r["mkt"] >= cut]
    wa, wb = fit_blend_weight(a), fit_blend_weight(b)
    scored = [(r, wb) for r in a] + [(r, wa) for r in b]            # each half scored with the OTHER half's weight
    per: dict = {}
    for r, w in scored:
        per.setdefault(r["mkt"], []).append((r["mid"] - r["outcome"]) ** 2 - (w * r["p"] + (1 - w) * r["mid"] - r["outcome"]) ** 2)
    return wa, wb, cluster_stat([mean(v) for v in per.values()])


def split_halves(rows: list):
    """Rows of the first and second half of the markets in time order, or None if there are too few markets."""
    ids = sorted({r["mkt"] for r in rows})
    if len(ids) < 4:
        return None
    cut = ids[len(ids) // 2]
    return [r for r in rows if r["mkt"] < cut], [r for r in rows if r["mkt"] >= cut]


def gap_by_block(rows: list, block_hours: float = 6.0, lo: float = 0.15, hi: float = 0.85) -> list:
    """Calibration gap (actual Yes rate minus mid, mids in [lo, hi)) per block of clock time, by market close time.
    Returns [(block_start_utc_text, markets, gap, se)] so a stretch of unusual market behaviour stands out."""
    size = int(block_hours * 3600 * 1000)
    blocks: dict = {}
    for r in rows:
        if lo <= r["mid"] < hi and r.get("close_ms") is not None:
            blocks.setdefault(r["close_ms"] // size, []).append(r)
    out = []
    for b in sorted(blocks):
        n, g, se = cluster_stat(per_market_mean(blocks[b], lambda r: r["outcome"] - r["mid"]))
        start = datetime.fromtimestamp(b * size / 1000, tz=timezone.utc).strftime("%m-%d %H:%MZ")
        out.append((start, n, g, se))
    return out


def filter_since(rows: list, since_ms: int | None) -> list:
    return rows if since_ms is None else [r for r in rows if r.get("close_ms", 0) >= since_ms]


def trades_by_block(rows: list, min_edge: float, block_hours: float = 6.0) -> list:
    """[(block_start_utc_text, [(pnl, side)])]: the same one-trade-per-market simulation, per block of close time."""
    size = int(block_hours * 3600 * 1000)
    blocks: dict = {}
    for r in rows:
        if r.get("close_ms") is not None:
            blocks.setdefault(r["close_ms"] // size, []).append(r)
    return [(datetime.fromtimestamp(b * size / 1000, tz=timezone.utc).strftime("%m-%d %H:%MZ"),
             first_signal_trades(blocks[b], min_edge)) for b in sorted(blocks)]


def momentum_groups(rows: list):
    """Does the trailing 30-minute BTC move predict the outcome beyond the market price?
    Rows are split into thirds by that move; per third: (label, markets, avg move in bp, gap, se) where
    gap = actual Yes rate minus mid. Persistence that the market does not price shows as a negative gap in the
    'down' third and a positive gap in the 'up' third."""
    rs = [r for r in rows if r.get("ret30") is not None]
    if len(rs) < 30:
        return None
    srt = sorted(r["ret30"] for r in rs)
    lo, hi = srt[len(srt) // 3], srt[2 * len(srt) // 3]
    out = []
    for label, keep in (("trailing 30 min down", lambda x: x < lo), ("roughly flat", lambda x: lo <= x < hi),
                        ("trailing 30 min up", lambda x: x >= hi)):
        g = [r for r in rs if keep(r["ret30"])]
        n, m, se = cluster_stat(per_market_mean(g, lambda r: r["outcome"] - r["mid"]))
        out.append((label, n, mean(r["ret30"] for r in g) if g else None, m, se))
    return out


def first_signal_trades(rows: list, min_edge: float, secs_range=None, age_range=None) -> list:
    """One simulated trade per market: the first row (in time order) whose edge beats `min_edge`.
    Returns (pnl, side) pairs, side being "yes" or "no"."""
    done, trades = set(), []
    for r in rows:
        if r["mkt"] in done:
            continue
        if secs_range and not (secs_range[1] <= r["secs_left"] <= secs_range[0]):
            continue
        if age_range and not (age_range[0] <= r["quote_age_s"] < age_range[1]):
            continue
        t = ep.best_trade(r, min_edge)
        if t:
            done.add(r["mkt"])
            trades.append((ep.trade_pnl(r, t[0], t[1]), t[0]))
    return trades


def first_signal_pnls(rows: list, min_edge: float, secs_range=None, age_range=None) -> list:
    return [p for p, _ in first_signal_trades(rows, min_edge, secs_range, age_range)]


def trade_line(label: str, trades: list) -> str:
    """`trades` is a list of (pnl, side) pairs, or plain pnls."""
    pnls = [t[0] if isinstance(t, tuple) else t for t in trades]
    n = len(pnls)
    if n == 0:
        return f"  {label:<26} no trades"
    se = stdev(pnls) / math.sqrt(n) if n > 1 else None
    sides = [t[1] for t in trades if isinstance(t, tuple)]
    split = f"  bought No in {sides.count('no')}/{n}" if sides else ""
    return f"  {label:<26} {n:>4} trades  {pm(mean(pnls), se, 100, 1, 'c'):<20} win {sum(p > 0 for p in pnls)}/{n}{split}"


def gap_by_half(rows: list, lo: float = 0.15, hi: float = 0.85):
    """Calibration gap (actual Yes rate minus mid) for mids in [lo, hi), for the first and second half of the
    markets in time order. A real bias should show up in both halves; a market regime shows up in one."""
    ids = sorted({r["mkt"] for r in rows})
    if len(ids) < 4:
        return None
    cut = ids[len(ids) // 2]
    out = []
    for half in ([r for r in rows if r["mkt"] < cut], [r for r in rows if r["mkt"] >= cut]):
        rs = [r for r in half if lo <= r["mid"] < hi]
        out.append(cluster_stat(per_market_mean(rs, lambda r: r["outcome"] - r["mid"])))
    return out


# ---------- report ----------
def summarize(rows: list, info: dict, min_edge: float = 0.03) -> str:
    n_mk = len({r["mkt"] for r in rows})
    out = [f"Settled markets: {info.get('markets')}   BRTI ticks: {info.get('ticks')}   usable quotes: {info.get('quotes')}"]
    if info.get("yes_markets") is not None:
        out.append(f"Yes won {info['yes_markets']} of {info['markets']} settled markets "
                   f"({100.0 * info['yes_markets'] / max(info['markets'], 1):.0f}%).   "
                   f"BTC moved from {info['btc_first']:,.0f} to {info['btc_last']:,.0f} over the sample.")
    out += [
           f"Sample rows (every {STEP_S} s, {SCAN_START_S // 60} min to {SCAN_END_S} s before close, two-sided quote needed): "
           f"{len(rows)} rows from {n_mk} markets"]
    if not rows:
        out.append("\nNo rows could be built (missing ticks or quotes).")
        return "\n".join(out)

    out.append("\n1. Is Kalshi's mid-price calibrated?  (gap = actual Yes rate minus mid, in cents; +/- is the std error across markets)")
    for c in calibration(rows):
        if c["rows"] == 0:
            continue
        out.append(f"  mid {c['lo'] * 100:>3.0f}-{c['hi'] * 100:>3.0f}c  rows {c['rows']:>5}  markets {c['markets']:>3}  "
                   f"avg mid {c['avg_mid'] * 100:>5.1f}c  actual Yes {c['win_rate'] * 100:>5.1f}%  gap {pm(c['gap'], c['se'], 100, 1, 'c')}"
                   + ("   (few markets: ignore)" if c["markets"] < 20 else ""))
    halves = gap_by_half(rows)
    if halves:
        out.append("  same gap for mids 15-85c, first half / second half of the markets: "
                   f"{pm(halves[0][1], halves[0][2], 100, 1, 'c')} ({halves[0][0]} markets)  /  "
                   f"{pm(halves[1][1], halves[1][2], 100, 1, 'c')} ({halves[1][0]} markets)")

    blocks = gap_by_block(rows)
    if blocks:
        out.append("  calibration gap (mids 15-85c) by 6-hour block of close time, UTC:")
        for start, n, g, se in blocks:
            out.append(f"    {start}  markets {n:>3}  gap {pm(g, se, 100, 1, 'c')}")
    out.append("\n2. Does the model beat the market price?  (Brier gain = market error minus model error; positive = model better)")
    n, g, se = brier_delta(rows, lambda r: r["p"])
    out.append(f"  all rows: model {mean((r['p'] - r['outcome']) ** 2 for r in rows):.4f}  market {mean((r['mid'] - r['outcome']) ** 2 for r in rows):.4f}"
               f"  gain {pm(g, se, 1000, 1, 'e-3')} over {n} markets")
    for hi, lo in TIME_BUCKETS:
        rs = [r for r in rows if lo <= r["secs_left"] <= hi]
        if rs:
            n, g, se = brier_delta(rs, lambda r: r["p"])
            out.append(f"  {hi // 60:>2}:{hi % 60:02d} to {lo // 60:>2}:{lo % 60:02d} left: gain {pm(g, se, 1000, 1, 'e-3')}  ({n} markets)")
    halves2 = split_halves(rows)
    if halves2:
        parts = []
        for h in halves2:
            n, g, se = brier_delta(h, lambda r: r["p"])
            parts.append(f"{pm(g, se, 1000, 1, 'e-3')} ({n} markets)")
        out.append(f"  model Brier gain, first half / second half of the markets: {parts[0]}  /  {parts[1]}")
    oos = blend_out_of_sample(rows)
    if oos:
        wa, wb, (n, g, se) = oos
        out.append(f"  blend weight on the model, fitted on each half of the markets: {wa:.2f} and {wb:.2f}")
        out.append(f"  out-of-sample Brier gain of that blend over the market alone: {pm(g, se, 1000, 1, 'e-3')} ({n} markets)")

    out.append(f"\n3. Simulated taker trades, one per market (buy at the ask when edge after fee exceeds the threshold)")
    for t in THRESHOLDS:
        out.append(trade_line(f"edge > {t * 100:.0f}c, whole window", first_signal_trades(rows, t)))
    whole = first_signal_trades(rows, min_edge)
    out.append(f"  by side bought (edge > {min_edge * 100:.0f}c, whole window):")
    for side in ("yes", "no"):
        out.append(trade_line(f"bought {side.capitalize()}", [t for t in whole if t[1] == side]))
    if halves2:
        out.append(f"  by half of the markets (edge > {min_edge * 100:.0f}c, whole window):")
        for label, h in (("first half", halves2[0]), ("second half", halves2[1])):
            out.append(trade_line(label, first_signal_trades(h, min_edge)))
    blocks_t = trades_by_block(rows, min_edge)
    if len(blocks_t) > 1:
        out.append(f"  by 6-hour block of close time (edge > {min_edge * 100:.0f}c):")
        for start, tr in blocks_t:
            out.append(trade_line(start, tr))
    out.append(f"  by time left (edge > {min_edge * 100:.0f}c):")
    for hi, lo in TIME_BUCKETS:
        out.append(trade_line(f"{hi // 60}:{hi % 60:02d} to {lo // 60}:{lo % 60:02d} left", first_signal_trades(rows, min_edge, secs_range=(hi, lo))))
    out.append(f"  by quote age (edge > {min_edge * 100:.0f}c; older quotes may be stale):")
    for lo, hi in AGE_BUCKETS:
        out.append(trade_line(f"quote {lo}-{hi} s old", first_signal_trades(rows, min_edge, age_range=(lo, hi))))
    mom = momentum_groups(rows)
    if mom:
        out.append("\n4. Momentum check (EXPLORATORY: idea formed after seeing the data, so only a run with --since on later data counts)")
        out.append("   gap = actual Yes rate minus Kalshi's mid-price, by the BTC move over the 30 minutes before each sample")
        for label, n, avg, g, se in mom:
            out.append(f"  {label:<22} markets {n:>3}  avg move {avg:>+7.1f} bp  gap {pm(g, se, 100, 1, 'c')}")
    out.append("\nCaution: a few hundred markets cannot show an edge of a cent or two. Look for results several std errors from zero "
               "that appear in BOTH halves of the data, then confirm with realistic fills before any real money.")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.getenv("LOG_DB", "kalshi_log.sqlite"))
    ap.add_argument("--archive-dir", default=None, help="default: an 'archive' folder next to --db")
    ap.add_argument("--min-edge", type=float, default=0.03, help="edge threshold for the by-time and by-age tables (dollars)")
    ap.add_argument("--cache", help="gzipped JSON file to save/reuse the loaded data (fast reruns)")
    ap.add_argument("--refresh", action="store_true", help="rebuild --cache from the database")
    ap.add_argument("--csv", help="also write the sample rows to this file")
    ap.add_argument("--since", help="only use markets closing at or after this UTC time, e.g. 2026-10-03T00:00 "
                                    "(use it to test an idea on data logged AFTER the idea was formed)")
    a = ap.parse_args(argv)
    since_ms = None
    if a.since:
        since_ms = int(datetime.fromisoformat(a.since).replace(tzinfo=timezone.utc).timestamp() * 1000)
    archive = vs.resolve_archive_dir(a.db, a.archive_dir)
    use_cache = bool(a.cache and Path(a.cache).exists() and not a.refresh)
    if not use_cache and not Path(a.db).is_file():
        print(f"ERROR: no database at {a.db}. Pass --db /var/lib/kalshi-bot/kalshi_log.sqlite", file=sys.stderr)
        return 2
    print(f"Read: {'cache ' + a.cache if use_cache else a.db + ' + archives in ' + str(archive)}")
    markets, ts, vals, quotes, info = load_inputs(a.db, archive, a.cache, a.refresh)
    rows = filter_since(build_rows(markets, ts, vals, quotes), since_ms)
    if since_ms is not None:
        print(f"Only markets closing at or after {a.since} UTC.")
    if ts:
        info = {**info, "yes_markets": sum(m.get("result") == "yes" for m in markets.values()),
                "btc_first": vals[0], "btc_last": vals[-1]}
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
