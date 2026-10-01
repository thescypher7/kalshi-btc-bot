"""Offline tests: no network, no credentials. Run with: pytest -q"""
import asyncio
import base64
import json
import time

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import kalshi_logger as kl

# Example message taken from Kalshi's cfbenchmarks_value channel docs.
EXAMPLE = {
    "type": "cfbenchmarks_value",
    "sending_ts_ms": 1669149841234,
    "sid": 1,
    "seq": 42,
    "msg": {
        "index_id": "BRTI",
        "received_at": 1710000000123,
        "data": '{"type":"value","id":"BRTI","time":1710000000123,"value":"68000.12"}',
        "avg_60s_data": {
            "value": "68000.12000000", "window_size": 3,
            "window_start_ts_ms": 1709999940123, "window_end_ts_exclusive": 1710000000123,
        },
        "last_60s_windowed_average_15min": {
            "value": "68000.23000000", "window_size": 14,
            "window_start_ts_ms": 1709999980000, "window_end_ts_exclusive": 1710000000123,
        },
    },
}


# ---------- parsing ----------
def test_parse_brti_full():
    row = kl.parse_brti("cfbenchmarks_value", 1710000000200, EXAMPLE)
    recv_ms, channel, src_ts, k_recv, sending, value, avg60, n60, avg15, n15 = row
    assert (recv_ms, channel) == (1710000000200, "cfbenchmarks_value")
    assert (src_ts, k_recv, sending) == (1710000000123, 1710000000123, 1669149841234)
    assert (value, avg60, n60, avg15, n15) == (68000.12, 68000.12, 3, 68000.23, 14)


def test_parse_brti_degrades_gracefully():
    msg = json.loads(json.dumps(EXAMPLE))
    del msg["msg"]["last_60s_windowed_average_15min"]
    msg["msg"]["data"] = "not json"
    row = kl.parse_brti("cfbenchmarks_value", 1, msg)
    assert row[2] is None and row[5] is None          # source ts and value unknown
    assert row[6] == 68000.12 and row[7] == 3         # Kalshi's 60s average still captured
    assert row[8] is None and row[9] is None          # 15-min average absent outside final minute


# ---------- signing ----------
@pytest.fixture(params=["rsa", "ed25519"])
def key(request, monkeypatch):
    k = (rsa.generate_private_key(public_exponent=65537, key_size=2048)
         if request.param == "rsa" else Ed25519PrivateKey.generate())
    monkeypatch.setattr(kl, "PRIVATE_KEY", k)
    return k


def _verify(key, headers, method, path):
    message = (headers["KALSHI-ACCESS-TIMESTAMP"] + method + path).encode()
    sig = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    pub = key.public_key()
    if isinstance(key, Ed25519PrivateKey):
        pub.verify(sig, message)
    else:
        pub.verify(sig, message,
                   padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                   hashes.SHA256())


def test_signature_verifies_over_timestamp_method_path(key):
    h = kl.auth_headers("GET", "/trade-api/ws/v2")
    assert set(h) == {"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-TIMESTAMP", "KALSHI-ACCESS-SIGNATURE"}
    _verify(key, h, "GET", "/trade-api/ws/v2")


def test_query_string_is_not_signed(key):
    h = kl.auth_headers("GET", "/trade-api/v2/markets?status=open")
    _verify(key, h, "GET", "/trade-api/v2/markets")


# ---------- storage ----------
def test_store_roundtrip():
    s = kl.Store(":memory:")
    s.brti.append(kl.parse_brti("cfbenchmarks_value", 1, EXAMPLE))
    s.log("ws", "orderbook_delta", "KXBTC15M-TEST", "{}")
    s.write(*s.drain())
    assert s.db.execute("select count(*) from brti").fetchone() == (1,)
    assert s.db.execute("select kind, ticker from raw").fetchall() == [("orderbook_delta", "KXBTC15M-TEST")]
    assert s.drain() == ([], [])


# ---------- websocket session logic (fake socket) ----------
class FakeWS:
    def __init__(self, frames=()):
        self.frames, self.sent = list(frames), []

    async def send(self, message):
        self.sent.append(json.loads(message))

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for f in self.frames:
            yield f


def test_session_routes_frames_and_tags_tickers():
    frames = [
        json.dumps({"id": 3, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 7}}),
        json.dumps({"id": 3, "type": "subscribed", "msg": {"channel": "ticker", "sid": 8}}),
        json.dumps({"type": "orderbook_snapshot", "sid": 7, "seq": 1,
                    "msg": {"market_ticker": "KXBTC15M-X", "yes_dollars_fp": [["0.50", "10.00"]]}}),
        json.dumps({"type": "ticker", "sid": 8, "seq": 1, "msg": {"market_ticker": "KXBTC15M-X"}}),
        json.dumps(EXAMPLE),
        "this is not json",
    ]

    async def run():
        store = kl.Store(":memory:")
        ws = FakeWS(frames)
        sess = kl.Session(ws, store)
        await sess.start()                          # command ids 1 and 2: the two BRTI channels
        await sess.subscribe_market("KXBTC15M-X")   # command id 3
        await sess.reader()
        return store, ws, sess

    store, ws, sess = asyncio.run(run())
    assert [(m["id"], m["params"]["channels"]) for m in ws.sent] == [
        (1, ["cfbenchmarks_value"]), (2, ["cfbenchmarks_value_5hz"]), (3, ["orderbook_delta", "ticker"])]
    assert sess.sid_ticker == {7: "KXBTC15M-X", 8: "KXBTC15M-X"}
    kinds = [(r[3], r[4]) for r in store.raw]
    assert ("orderbook_snapshot", "KXBTC15M-X") in kinds and ("ticker", "KXBTC15M-X") in kinds
    assert ("bad_json", None) in kinds
    assert len(store.brti) == 1 and store.brti[0][5] == 68000.12


def test_sync_loop_subscribes_open_and_unsubscribes_expired(monkeypatch):
    monkeypatch.setattr(kl, "MARKETS", {"OPEN-1": time.time() + 600})

    async def run():
        ws = FakeWS()
        sess = kl.Session(ws, kl.Store(":memory:"))
        sess.subscribed, sess.sid_ticker = {"OLD-1"}, {9: "OLD-1"}   # OLD-1 is not in MARKETS any more
        stop = asyncio.Event()
        task = asyncio.create_task(sess.sync_loop(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await task
        return ws.sent, sess.subscribed

    sent, subscribed = asyncio.run(run())
    cmds = [(m["cmd"], m["params"]) for m in sent]
    assert ("subscribe", {"channels": ["orderbook_delta", "ticker"], "market_tickers": ["OPEN-1"]}) in cmds
    assert ("unsubscribe", {"sids": [9]}) in cmds
    assert subscribed == {"OPEN-1"}
