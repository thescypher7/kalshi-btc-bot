"""edge_scan tests: late-window model, clustered statistics, row building, blend, cache, and a synthetic end-to-end run."""
import json
from datetime import datetime, timezone

import edge_probe as ep
import edge_scan as es
import kalshi_logger as kl


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_late_model_is_continuous_at_60_seconds_and_sharpens_as_close_nears():
    a = ep.fair_value(100.4, 100.0, 0.1, 60)
    b = es.fair_value_late(100.4, 100.4, 100.0, 0.1, 60)       # nothing known yet at exactly 60 s left
    assert abs(a - b) < 1e-9
    near = es.fair_value_late(100.4, 100.4, 100.0, 0.1, 10)
    assert near > b                                            # same lead, less time: more certain
    assert es.fair_value_late(100.9, 101.0, 100.0, 0.1, 30) > 0.99 and es.fair_value_late(99.1, 99.0, 100.0, 0.1, 30) < 0.01
    # known low prices can pull the average below the strike even though spot is above it
    assert es.fair_value_late(98.0, 100.2, 100.0, 0.05, 15) < 0.01


def test_cluster_stat_and_formatting():
    assert es.cluster_stat([]) == (0, None, None)
    n, m, se = es.cluster_stat([1.0, 3.0])
    assert n == 2 and m == 2.0 and abs(se - 1.0) < 1e-12
    assert es.pm(0.012, 0.005, 100, 1, "c") == "+1.2c +/- 0.5c" and es.pm(None, None) == "n/a"


def rows_for(markets, **kw):
    out = []
    for i, (p, mid, y) in enumerate(markets):
        for k in range(3):
            out.append({"mkt": i, "ticker": f"T{i}", "secs_left": 300 - 10 * k, "p": p, "mid": mid, "bid": mid - 0.01,
                        "ask": mid + 0.01, "quote_age_s": 1.0, "outcome": y, **kw})
    return out


def test_blend_weight_prefers_the_better_forecaster():
    good = rows_for([(0.9, 0.5, 1), (0.1, 0.5, 0)] * 4)         # model right, market clueless
    assert es.fit_blend_weight(good) == 1.0
    bad = rows_for([(0.9, 0.5, 0), (0.1, 0.5, 1)] * 4)          # model wrong
    assert es.fit_blend_weight(bad) == 0.0
    wa, wb, (n, gain, se) = es.blend_out_of_sample(good)
    assert wa == 1.0 and wb == 1.0 and n == 8 and gain > 0.1
    assert es.blend_out_of_sample(rows_for([(0.5, 0.5, 1)])) is None      # too few markets


def test_first_signal_is_one_trade_per_market_and_filters_apply():
    rows = rows_for([(0.9, 0.5, 1), (0.9, 0.5, 0)])
    pnls = es.first_signal_pnls(rows, 0.03)
    assert len(pnls) == 2 and pnls[0] > 0 > pnls[1]             # a winner and a loser, one each
    assert es.first_signal_pnls(rows, 0.03, secs_range=(100, 10)) == []        # no rows in that time range
    assert len(es.first_signal_pnls(rows, 0.03, age_range=(0, 5))) == 2
    assert es.first_signal_pnls(rows, 0.03, age_range=(5, 30)) == []
    assert es.first_signal_pnls(rows, 0.5) == []                                # edge never that big


def test_calibration_buckets():
    rows = rows_for([(0.9, 0.92, 1), (0.9, 0.92, 0), (0.1, 0.08, 0)])
    cal = [c for c in es.calibration(rows) if c["rows"]]
    assert [(c["lo"], c["rows"]) for c in cal] == [(0.05, 3), (0.85, 6)]
    hi = cal[1]
    assert abs(hi["win_rate"] - 0.5) < 1e-9 and hi["gap"] < 0           # 50% actual vs 92% priced


def build_db(tmp_path):
    s = kl.Store(str(tmp_path / "t.sqlite"))
    t0 = ms("2026-10-01T20:50:00Z")
    brti = [(t0 + i * 1000 + 50, "cfbenchmarks_value", t0 + i * 1000, None, None, 101.0 + (i % 2) * 0.01,
             None, None, None, None) for i in range(26 * 60)]
    raw = [(1, 1, "rest", "market_after_close", "T-A", json.dumps({"market": {
        "open_time": "2026-10-01T21:00:00Z", "close_time": "2026-10-01T21:15:00Z", "floor_strike": 100.0,
        "expiration_value": "101.00", "result": "yes", "strike_type": "greater_or_equal"}}))]
    for k in range(31):
        raw.append((ms("2026-10-01T21:00:00Z") + k * 30_000, 2 + k, "ws", "ticker", "T-A",
                    json.dumps({"type": "ticker", "msg": {"yes_bid": 50, "yes_ask": 52}})))
    s.write(raw, brti)
    s.db.close()
    return str(tmp_path / "t.sqlite")


def test_end_to_end_synthetic(tmp_path):
    db = build_db(tmp_path)
    markets, ts, vals, quotes, info = es.load_inputs(db, tmp_path / "archive")
    rows = es.build_rows(markets, ts, vals, quotes)
    assert len(rows) == 60 and {r["secs_left"] for r in rows} == set(range(10, 601, 10))
    assert all(r["p"] > 0.99 and r["mid"] == 0.51 and r["outcome"] == 1 for r in rows)
    text = es.summarize(rows, info)
    for section in ("1. Is Kalshi's mid-price calibrated", "2. Does the model beat", "3. Simulated taker trades"):
        assert section in text
    assert "1 trades" in text or "   1 trades" in text
    assert "rows from 1 markets" in text


def test_cache_roundtrip_works_without_the_database(tmp_path):
    db = build_db(tmp_path)
    cache = str(tmp_path / "c.json.gz")
    first = es.load_inputs(db, tmp_path / "archive", cache)
    (tmp_path / "t.sqlite").unlink()
    second = es.load_inputs(db, tmp_path / "archive", cache)
    assert second[4]["from_cache"] is True
    assert second[0] == first[0] and second[1] == first[1] and second[2] == first[2]
    assert second[3] == first[3] and all(isinstance(q, tuple) for q in second[3]["T-A"])


def test_cli_runs_from_cache_and_refuses_missing_database(tmp_path, capsys):
    db = build_db(tmp_path)
    cache = str(tmp_path / "c.json.gz")
    assert es.main(["--db", db, "--cache", cache]) == 0
    assert "Sample rows" in capsys.readouterr().out
    assert es.main(["--db", str(tmp_path / "nope.sqlite")]) == 2
    assert "no database at" in capsys.readouterr().err


def test_quote_lookup_survives_two_quotes_in_the_same_millisecond():
    quotes = {"T": [(1000, None, 0.5), (1000, 0.4, None), (2000, 0.45, 0.55)]}
    assert ep.quote_at(quotes, "T", 1500)[:2] in ((None, 0.5), (0.4, None))
    assert ep.quote_at(quotes, "T", 2500)[:2] == (0.45, 0.55)


def test_gap_by_half_separates_a_regime_from_a_steady_bias():
    # first 4 markets all lose, last 4 all win, every mid is 50c: a regime, not a steady bias
    regime = rows_for([(0.5, 0.5, 0)] * 4 + [(0.5, 0.5, 1)] * 4)
    (n1, g1, _), (n2, g2, _) = es.gap_by_half(regime)
    assert (n1, n2) == (4, 4) and g1 < -0.4 and g2 > 0.4
    assert es.gap_by_half(rows_for([(0.5, 0.5, 1)])) is None


def test_trades_report_which_side_was_bought_and_base_rate_is_printed():
    rows = rows_for([(0.1, 0.5, 0), (0.1, 0.5, 0), (0.9, 0.5, 1)])
    trades = es.first_signal_trades(rows, 0.03)
    assert [side for _, side in trades] == ["no", "no", "yes"]
    assert "bought No in 2/3" in es.trade_line("x", trades)
    text = es.summarize(rows, {"markets": 3, "ticks": 1, "quotes": 1, "yes_markets": 1, "btc_first": 100000.0, "btc_last": 99000.0})
    assert "Yes won 1 of 3 settled markets (33%)" in text and "from 100,000 to 99,000" in text
    assert "first half / second half" in text or "same gap" not in text      # needs >= 4 markets for the halves line


H = 3600 * 1000


def test_gap_by_block_localises_a_bad_stretch():
    rows = []
    for i, y in enumerate([1, 0, 1, 0, 0, 0, 0, 0]):            # 4 markets in block 1, then 4 losing markets in block 2
        for k in range(2):
            rows.append({"mkt": i, "ticker": f"T{i}", "close_ms": (i // 4) * 6 * H + i * 1000, "secs_left": 300 - k,
                         "p": 0.5, "mid": 0.5, "bid": 0.49, "ask": 0.51, "quote_age_s": 1.0, "outcome": y})
    blocks = es.gap_by_block(rows)
    assert [b[1] for b in blocks] == [4, 4] and abs(blocks[0][2]) < 1e-9 and abs(blocks[1][2] + 0.5) < 1e-9
    assert blocks[0][0].endswith("00:00Z") and blocks[1][0].endswith("06:00Z")


def test_report_has_time_split_sections():
    rows = rows_for([(0.9, 0.5, 1), (0.1, 0.5, 0), (0.9, 0.5, 1), (0.1, 0.5, 0)] * 2)
    for r in rows:
        r["close_ms"] = r["mkt"] * 4 * H
    text = es.summarize(rows, {"markets": 8, "ticks": 1, "quotes": 1})
    for needle in ("by 6-hour block", "model Brier gain, first half / second half", "by side bought", "bought Yes", "bought No",
                   "by half of the markets", "first half", "second half"):
        assert needle in text, needle
    assert es.split_halves(rows_for([(0.5, 0.5, 1)])) is None
