"""edge_probe tests: model maths, quote parsing, and a synthetic end-to-end run."""
import gzip
import json
from datetime import datetime, timezone

import edge_probe as ep
import kalshi_logger as kl


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_fair_value_basics():
    assert abs(ep.fair_value(100.0, 100.0, 2.0, 300) - 0.5) < 1e-9
    assert ep.fair_value(105.0, 100.0, 0.1, 300) > 0.95
    assert ep.fair_value(95.0, 100.0, 0.1, 300) < 0.05
    assert ep.fair_value(100.5, 100.0, 2.0, 600) < ep.fair_value(100.5, 100.0, 2.0, 180)   # less time, more certain
    assert ep.fair_value(101.0, 100.0, 0.0, 300) == 1.0 and ep.fair_value(99.0, 100.0, 0.0, 300) == 0.0


def test_taker_fee_peaks_at_fifty_cents():
    assert abs(ep.taker_fee(0.5) - 0.0175) < 1e-12
    assert ep.taker_fee(0.9) < ep.taker_fee(0.5) and abs(ep.taker_fee(0.2) - ep.taker_fee(0.8)) < 1e-12


def test_parse_quote_cents_dollars_and_bad_input():
    frame = lambda msg: json.dumps({"type": "ticker", "msg": msg})
    assert ep.parse_quote(frame({"yes_bid": 50, "yes_ask": 52})) == (0.5, 0.52)
    assert ep.parse_quote(frame({"yes_bid_dollars": "0.4800", "yes_ask_dollars": "0.5000"})) == (0.48, 0.5)
    assert ep.parse_quote(frame({"price": 50})) is None
    assert ep.parse_quote("not json") is None


def test_best_trade_and_pnl():
    r = {"p": 0.9, "bid": 0.50, "ask": 0.52, "outcome": 1}
    side, price, edge = ep.best_trade(r, 0.03)
    assert side == "yes" and price == 0.52 and edge > 0.3
    assert abs(ep.trade_pnl(r, side, price) - (1 - 0.52 - ep.taker_fee(0.52))) < 1e-12
    assert ep.best_trade({"p": 0.51, "bid": 0.50, "ask": 0.52, "outcome": 1}, 0.03) is None   # inside the spread
    low = {"p": 0.1, "bid": 0.50, "ask": 0.52, "outcome": 0}
    assert ep.best_trade(low, 0.03)[0] == "no" and ep.trade_pnl(low, "no", 0.5) > 0


def build(tmp_path, with_quotes=True):
    db_path = tmp_path / "t.sqlite"
    s = kl.Store(str(db_path))
    t0 = ms("2026-10-01T20:50:00Z")
    brti = [(t0 + i * 1000 + 50, "cfbenchmarks_value", t0 + i * 1000, None, None, 101.0 + (i % 2) * 0.01,
             None, None, None, None) for i in range(26 * 60)]
    raw = [(1, 1, "rest", "market_after_close", "T-A", json.dumps({"market": {
        "open_time": "2026-10-01T21:00:00Z", "close_time": "2026-10-01T21:15:00Z", "floor_strike": 100.0,
        "expiration_value": "101.00", "result": "yes", "strike_type": "greater_or_equal"}}))]
    if with_quotes:
        for iso in ("21:04:30", "21:07:30", "21:09:30", "21:11:30"):
            raw.append((ms(f"2026-10-01T{iso}Z"), 2, "ws", "ticker", "T-A",
                        json.dumps({"type": "ticker", "msg": {"market_ticker": "T-A", "yes_bid": 50, "yes_ask": 52}})))
    s.write(raw, brti)
    s.db.close()
    return str(db_path)


def test_end_to_end_synthetic_market(tmp_path):
    db = build(tmp_path)
    rows, info = ep.analyze(db, tmp_path / "archive")
    assert info["markets"] == 1 and info["parsed"] == 4
    assert [r["secs_left"] for r in rows] == [600, 420, 300, 180]
    assert all(r["p"] > 0.99 and r["bid"] == 0.5 and r["ask"] == 0.52 for r in rows)   # spot ~101 vs strike 100
    text = ep.summarize(rows, info)
    assert "first signal per market" in text and "1 trade, pnl" in text
    assert "pnl +" in text


def test_missing_quotes_prints_parser_hint(tmp_path):
    db = build(tmp_path, with_quotes=False)
    rows, info = ep.analyze(db, tmp_path / "archive")
    assert "No usable yes_bid/yes_ask" in ep.summarize(rows, info)


def test_cli_reads_archived_ticker_rows(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("ARCHIVE_DIR", raising=False)
    db = build(tmp_path, with_quotes=False)
    arch = tmp_path / "archive"
    arch.mkdir()
    with gzip.open(arch / "raw_20261001T21.jsonl.gz", "wt") as f:       # compact separators, like retention.py writes
        f.write(json.dumps({"id": 50, "recv_ms": ms("2026-10-01T21:04:30Z"), "mono_ns": 1, "src": "ws", "kind": "ticker",
                            "ticker": "T-A", "payload": json.dumps({"msg": {"yes_bid": 50, "yes_ask": 52}})},
                           separators=(",", ":")) + "\n")
    assert ep.main(["--db", db]) == 0
    out = capsys.readouterr().out
    assert "Read:" in out and "usable quotes: 1 of 1" in out
