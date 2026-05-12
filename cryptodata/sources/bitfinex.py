"""Bitfinex spot adapter.

WS docs:   https://docs.bitfinex.com/docs/ws-public
REST docs: https://docs.bitfinex.com/reference/rest-public-trades

Bitfinex symbols are ``tBTCUSD``, ``tETHUST`` (USDT is ``UST``). The mapping is held
in ``symbols.toml``. The WS protocol is channel-id based: a subscribe response gives
a ``chanId`` you then have to track. We keep a small ``chanId -> canonical`` map per
connection.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import aiohttp
import orjson
import websockets

from cryptodata.core.symbols import to_canonical, to_native
from cryptodata.paths import load_venues
from cryptodata.sources.base import BookSnapshot, Funding, OpenInterest, Quote, Trade, ns_now

_TF = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "6h": "6h", "1d": "1D"}


class BitfinexSpot:
    name = "bitfinex"

    def __init__(self) -> None:
        cfg = load_venues()["bitfinex"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        natives = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            chan: dict[int, str] = {}
            for n in natives:
                await ws.send(orjson.dumps({"event": "subscribe", "channel": "trades", "symbol": n}).decode())
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if isinstance(msg, dict):
                    if msg.get("event") == "subscribed" and msg.get("channel") == "trades":
                        canonical = to_canonical(msg.get("symbol", ""), self.name) or to_canonical(msg.get("pair", ""), self.name)
                        if canonical:
                            chan[int(msg["chanId"])] = canonical
                    continue
                # data: [chanId, "te"|"tu", [id, mts, amount, price]]  (or snapshot list)
                if not isinstance(msg, list) or len(msg) < 2:
                    continue
                cid = int(msg[0])
                canonical = chan.get(cid)
                if canonical is None:
                    continue
                recv_ns = ns_now()
                payload = msg[2] if len(msg) > 2 else msg[1]
                rows = payload if (isinstance(payload, list) and payload and isinstance(payload[0], list)) else [payload]
                tag = msg[1] if isinstance(msg[1], str) else "te"
                if tag not in ("te", "tu") and not isinstance(msg[1], list):
                    # 'hb' heartbeat etc.
                    continue
                for r in rows:
                    if not isinstance(r, list) or len(r) < 4:
                        continue
                    tid, mts, amount, price = r[0], r[1], float(r[2]), float(r[3])
                    yield Trade(
                        ts_ns=int(mts) * 1_000_000,
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        price=price,
                        size=abs(amount),
                        side=+1 if amount > 0 else -1,
                        trade_id=str(tid),
                    )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        natives = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            chan: dict[int, str] = {}
            for n in natives:
                await ws.send(orjson.dumps({"event": "subscribe", "channel": "ticker", "symbol": n}).decode())
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if isinstance(msg, dict):
                    if msg.get("event") == "subscribed" and msg.get("channel") == "ticker":
                        canonical = to_canonical(msg.get("symbol", ""), self.name) or to_canonical(msg.get("pair", ""), self.name)
                        if canonical:
                            chan[int(msg["chanId"])] = canonical
                    continue
                if not isinstance(msg, list) or len(msg) < 2 or not isinstance(msg[1], list):
                    continue
                canonical = chan.get(int(msg[0]))
                if canonical is None:
                    continue
                t = msg[1]  # [BID, BID_SIZE, ASK, ASK_SIZE, ...]
                if len(t) < 4:
                    continue
                recv_ns = ns_now()
                yield Quote(
                    ts_ns=recv_ns, recv_ns=recv_ns, symbol=canonical, venue=self.name,
                    bid_px=float(t[0]), bid_sz=float(t[1]), ask_px=float(t[2]), ask_sz=float(t[3]),
                )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError("Bitfinex L2 snapshot ingest not enabled in v1.")

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("Bitfinex covered as spot-only.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        start_ms, end_ms = start_ns // 1_000_000, end_ns // 1_000_000
        cur = start_ms
        async with aiohttp.ClientSession() as sess:
            for _ in range(2000):
                params = {"limit": 10000, "sort": 1, "start": cur, "end": end_ms}
                async with sess.get(f"{self.rest_base}/v2/trades/{native}/hist", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                for r in rows:  # [id, mts, amount, price]
                    ts_ns = int(r[1]) * 1_000_000
                    if ts_ns > end_ns:
                        break
                    amount = float(r[2])
                    out.append(Trade(
                        ts_ns=ts_ns, recv_ns=ts_ns, symbol=symbol, venue=self.name,
                        price=float(r[3]), size=abs(amount), side=+1 if amount > 0 else -1, trade_id=str(r[0]),
                    ))
                new_cur = int(rows[-1][1]) + 1
                if new_cur <= cur or len(rows) < 10000:
                    break
                cur = new_cur
                await asyncio.sleep(0.3)
        out.sort(key=lambda t: t.ts_ns)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        tf = _TF.get(interval, "1m")
        out: list[dict] = []
        start_ms, end_ms = start_ns // 1_000_000, end_ns // 1_000_000
        cur = start_ms
        async with aiohttp.ClientSession() as sess:
            for _ in range(5000):
                params = {"limit": 10000, "sort": 1, "start": cur, "end": end_ms}
                async with sess.get(f"{self.rest_base}/v2/candles/trade:{tf}:{native}/hist", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                for r in rows:  # [mts, open, close, high, low, volume]
                    ts_ns = int(r[0]) * 1_000_000
                    if ts_ns > end_ns:
                        break
                    out.append({"ts_ns": ts_ns, "open": float(r[1]), "close": float(r[2]),
                                "high": float(r[3]), "low": float(r[4]), "volume": float(r[5]), "trades": 0})
                new_cur = int(rows[-1][0]) + 1
                if new_cur <= cur or len(rows) < 10000:
                    break
                cur = new_cur
                await asyncio.sleep(0.3)
        out.sort(key=lambda r: r["ts_ns"])
        return out

    async def _connect(self):
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024) as ws:
                    backoff = 1.0
                    yield ws
            except (TimeoutError, websockets.ConnectionClosed, OSError):
                pass
            await asyncio.sleep(min(backoff, 60.0))
            backoff = min(backoff * 2, 60.0)
