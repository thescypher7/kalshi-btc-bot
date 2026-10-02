#!/usr/bin/env python3
"""
Kalshi KXBTC15M logger (sketch).

Records into one SQLite file, each row stamped with LOCAL receive time:
  * CF Benchmarks BRTI feed: 1/s channel and 5Hz channel (raw frame + parsed value,
    Kalshi's trailing-60s average, and the final-minute quarter-hour average)
  * orderbook snapshots/deltas and ticker updates for the currently open KXBTC15M markets
  * market metadata (REST) and the settled result once the market resolves

Every raw frame is stored verbatim in table `raw`, so a schema surprise never loses data.
Parsed BRTI values go to table `brti` for fast analysis.

Setup:
    pip install "websockets>=14" httpx cryptography
    export KALSHI_KEY_ID=...                       # your API key id
    export KALSHI_PRIVATE_KEY_PATH=/path/key.pem   # your private key
    python kalshi_logger.py

Run it on a machine with NTP/chrony time sync: you will compare local timestamps
against Kalshi's and (later) exchange feeds' timestamps.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import signal
import sqlite3
import time
from datetime import datetime

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
WS_URL = os.getenv("KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2")
REST_BASE = os.getenv("KALSHI_REST_BASE", "https://api.elections.kalshi.com")  # verify in Kalshi docs
SERIES = os.getenv("KALSHI_SERIES", "KXBTC15M")
DB_PATH = os.getenv("LOG_DB", "kalshi_log.sqlite")
INDEX_ID = "BRTI"
WS_SIGN_PATH = "/trade-api/ws/v2"
REST_MARKETS = "/trade-api/v2/markets"

PRIVATE_KEY = None  # loaded in main()
MARKETS: dict[str, float] = {}  # open ticker -> close time (unix seconds)
DONE: set[str] = set()          # tickers whose settlement result has been recorded

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw (
    recv_ms INTEGER NOT NULL, mono_ns INTEGER NOT NULL,
    src TEXT NOT NULL, kind TEXT, ticker TEXT, payload TEXT);
CREATE INDEX IF NOT EXISTS raw_t  ON raw (recv_ms);
CREATE INDEX IF NOT EXISTS raw_tk ON raw (ticker, recv_ms);
CREATE TABLE IF NOT EXISTS brti (
    recv_ms INTEGER NOT NULL, channel TEXT, source_ts_ms INTEGER, kalshi_recv_ms INTEGER,
    sending_ts_ms INTEGER, value REAL, avg60 REAL, avg60_n INTEGER, avg15 REAL, avg15_n INTEGER);
CREATE INDEX IF NOT EXISTS brti_t ON brti (recv_ms);
"""


# ---------- helpers ----------
def now_ms() -> int:
    return int(time.time() * 1000)


def _num(x, cast=float):
    try:
        return cast(x)
    except (TypeError, ValueError):
        return None


def parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


async def nap(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except asyncio.TimeoutError:
        pass


def sign_text(text: str) -> str:
    message = text.encode()
    if isinstance(PRIVATE_KEY, Ed25519PrivateKey):
        sig = PRIVATE_KEY.sign(message)
    else:  # RSA-PSS, as in Kalshi's quick start
        sig = PRIVATE_KEY.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    return base64.b64encode(sig).decode()


def auth_headers(method: str, path: str) -> dict:
    ts = str(now_ms())
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": sign_text(ts + method + path.split("?")[0]),
    }


# ---------- storage ----------
class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.raw: list[tuple] = []
        self.brti: list[tuple] = []

    def log(self, src: str, kind: str, ticker, payload: str) -> None:
        self.raw.append((now_ms(), time.monotonic_ns(), src, kind, ticker, payload))

    def write(self, raw: list, brti: list) -> None:
        try:
            if raw:
                self.db.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?)", raw)
            if brti:
                self.db.executemany("INSERT INTO brti VALUES (?,?,?,?,?,?,?,?,?,?)", brti)
            self.db.commit()
        except sqlite3.Error:
            self.db.rollback()   # never leave half a batch pending; the caller requeues all of it
            raise

    def drain(self):
        raw, self.raw = self.raw, []
        brti, self.brti = self.brti, []
        return raw, brti

    def requeue(self, raw: list, brti: list) -> None:
        """Put rows back at the front after a failed write so they are retried, not lost."""
        self.raw[:0] = raw
        self.brti[:0] = brti


def parse_brti(channel: str, recv_ms: int, d: dict) -> tuple:
    """Pull the BRTI value and Kalshi's averages out of a cfbenchmarks_value message."""
    m = d.get("msg") or {}
    src_ts = val = None
    try:
        frame = json.loads(m.get("data") or "{}")
        src_ts, val = _num(frame.get("time"), int), _num(frame.get("value"))
    except ValueError:
        pass
    a60 = m.get("avg_60s_data") or {}
    a15 = m.get("last_60s_windowed_average_15min") or {}
    return (
        recv_ms, channel, src_ts, _num(m.get("received_at"), int), _num(d.get("sending_ts_ms"), int),
        val, _num(a60.get("value")), _num(a60.get("window_size"), int),
        _num(a15.get("value")), _num(a15.get("window_size"), int),
    )


async def flusher(store: Store, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await nap(stop, 1.0)
        raw, brti = store.drain()
        try:
            await asyncio.to_thread(store.write, raw, brti)
        except sqlite3.Error as e:   # e.g. "database is locked" while the retention job runs
            store.requeue(raw, brti)
            print(f"[logger] db write failed ({e!r}); {len(raw)} raw / {len(brti)} brti rows requeued", flush=True)


# ---------- REST: market discovery and settlement ----------
async def rest_get(http: httpx.AsyncClient, path: str, params: dict | None = None):
    r = await http.get(REST_BASE + path, params=params, headers=auth_headers("GET", path))
    r.raise_for_status()
    return r.json(), r.text


async def discover_loop(http, store: Store, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            data, text = await rest_get(http, REST_MARKETS,
                                        {"series_ticker": SERIES, "status": "open", "limit": 20})
            store.log("rest", "markets_open", None, text)
            for mk in data.get("markets", []):
                t = mk.get("ticker")
                if t and t not in DONE and mk.get("close_time"):
                    MARKETS.setdefault(t, parse_ts(mk["close_time"]))
        except Exception as e:  # keep going; the error is logged
            store.log("rest", "discover_error", None, repr(e))
        await nap(stop, 20)


async def settle_loop(http, store: Store, stop: asyncio.Event) -> None:
    attempts: dict[str, int] = {}
    while not stop.is_set():
        for t, close_ts in list(MARKETS.items()):
            if time.time() < close_ts + 15:
                continue
            attempts[t] = attempts.get(t, 0) + 1
            try:
                data, text = await rest_get(http, f"{REST_MARKETS}/{t}")
                store.log("rest", "market_after_close", t, text)
                if (data.get("market") or {}).get("result") in ("yes", "no"):
                    DONE.add(t)
                    MARKETS.pop(t, None)
            except Exception as e:
                store.log("rest", "settle_error", t, repr(e))
            if attempts[t] >= 60:  # give up after ~15 min
                MARKETS.pop(t, None)
        await nap(stop, 15)


# ---------- websocket ----------
class Session:
    def __init__(self, ws, store: Store):
        self.ws, self.store = ws, store
        self._id = 0
        self.cmd_ticker: dict[int, str | None] = {}
        self.sid_ticker: dict[int, str | None] = {}
        self.subscribed: set[str] = set()

    async def send(self, cmd: str, params: dict, ticker: str | None = None) -> None:
        self._id += 1
        self.cmd_ticker[self._id] = ticker
        await self.ws.send(json.dumps({"id": self._id, "cmd": cmd, "params": params}))

    async def start(self) -> None:
        # Separate subscribes so one channel being unavailable doesn't block the other.
        for ch in ("cfbenchmarks_value", "cfbenchmarks_value_5hz"):
            await self.send("subscribe", {"channels": [ch], "index_ids": [INDEX_ID]})

    async def subscribe_market(self, t: str) -> None:
        self.subscribed.add(t)
        await self.send("subscribe",
                        {"channels": ["orderbook_delta", "ticker"], "market_tickers": [t]}, ticker=t)

    async def unsubscribe_market(self, t: str) -> None:
        self.subscribed.discard(t)
        sids = [s for s, tk in self.sid_ticker.items() if tk == t]
        if sids:
            await self.send("unsubscribe", {"sids": sids})
            for s in sids:
                self.sid_ticker.pop(s, None)

    async def reader(self) -> None:
        async for frame in self.ws:
            recv_ms, mono = now_ms(), time.monotonic_ns()
            text = frame if isinstance(frame, str) else frame.decode()
            try:
                d = json.loads(text)
            except ValueError:
                self.store.raw.append((recv_ms, mono, "ws", "bad_json", None, text[:2000]))
                continue
            typ = d.get("type")
            msg = d.get("msg") if isinstance(d.get("msg"), dict) else {}
            if typ == "subscribed":
                self.sid_ticker[msg.get("sid")] = self.cmd_ticker.get(d.get("id"))
            ticker = msg.get("market_ticker") or self.sid_ticker.get(d.get("sid") or msg.get("sid"))
            self.store.raw.append((recv_ms, mono, "ws", typ, ticker, text))
            if typ and typ.startswith("cfbenchmarks_value") and "indexlist" not in typ:
                self.store.brti.append(parse_brti(typ, recv_ms, d))

    async def sync_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = time.time()
            for t, close_ts in list(MARKETS.items()):
                if t not in self.subscribed and now < close_ts + 30:
                    await self.subscribe_market(t)
            for t in list(self.subscribed):
                close_ts = MARKETS.get(t)
                if close_ts is None or now > close_ts + 30:
                    await self.unsubscribe_market(t)
            await nap(stop, 5)


async def ws_loop(store: Store, stop: asyncio.Event) -> None:
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(
                WS_URL, additional_headers=auth_headers("GET", WS_SIGN_PATH), max_size=None
            ) as ws:
                backoff = 1
                store.log("ws", "connected", None, WS_URL)
                sess = Session(ws, store)
                await sess.start()
                tasks = [asyncio.create_task(sess.reader()),
                         asyncio.create_task(sess.sync_loop(stop)),
                         asyncio.create_task(stop.wait())]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for t in done:
                    t.result()  # re-raise a reader error so we reconnect
        except Exception as e:
            store.log("ws", "disconnect", None, repr(e))
        await nap(stop, backoff)
        backoff = min(backoff * 2, 30)


async def main() -> None:
    global PRIVATE_KEY
    if not KEY_ID or not KEY_PATH:
        raise SystemExit("Set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH")
    with open(KEY_PATH, "rb") as f:
        PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    store = Store(DB_PATH)
    async with httpx.AsyncClient(timeout=10) as http:
        await asyncio.gather(
            flusher(store, stop),
            discover_loop(http, store, stop),
            settle_loop(http, store, stop),
            ws_loop(store, stop),
        )
    store.write(*store.drain())  # final flush


if __name__ == "__main__":
    asyncio.run(main())
