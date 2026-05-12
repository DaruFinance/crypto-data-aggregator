"""Gemini spot adapter.

WS docs:   https://docs.gemini.com/websocket-api/#market-data-version-2
REST docs: https://docs.gemini.com/rest-api/

Native symbols are lowercase concatenations (``btcusd``, ``ethusd``). The v2 market
data WS multiplexes many symbols on one socket.
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

_TF = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1hr", "6h": "6hr", "1d": "1day"}


class GeminiSpot:
    name = "gemini"

    def __init__(self) -> None:
        cfg = load_venues()["gemini"]
        self.ws_url = cfg["ws_url"]            # wss://api.gemini.com/v2/marketdata
        self.rest_base = cfg["rest_base"]

    async def _subscribe(self, ws, symbols_native: list[str]) -> None:
        await ws.send(orjson.dumps({
            "type": "subscribe",
            "subscriptions": [{"name": "l2", "symbols": [s.upper() for s in symbols_native]}],
        }).decode())

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, native)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("type") != "trade":
                    continue
                recv_ns = ns_now()
                sym = msg.get("symbol", "")
                canonical = to_canonical(sym, self.name) or to_canonical(sym.lower(), self.name)
                if canonical is None:
                    continue
                # `timestamp` is ms epoch on v2 trade events.
                yield Trade(
                    ts_ns=int(msg.get("timestamp", recv_ns // 1_000_000)) * 1_000_000,
                    recv_ns=recv_ns,
                    symbol=canonical,
                    venue=self.name,
                    price=float(msg["price"]),
                    size=float(msg["quantity"]),
                    side=+1 if str(msg.get("side", "")).lower() == "buy" else -1,
                    trade_id=str(msg.get("event_id", "")),
                )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        # v2 l2 channel sends incremental updates; track top-of-book per symbol.
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        best: dict[str, dict[str, float]] = {}
        async for ws in self._connect():
            await self._subscribe(ws, native)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                t = msg.get("type")
                if t not in ("l2_updates",):
                    continue
                sym = msg.get("symbol", "")
                canonical = to_canonical(sym, self.name) or to_canonical(sym.lower(), self.name)
                if canonical is None:
                    continue
                b = best.setdefault(sym, {"bid": 0.0, "ask": 0.0, "bid_sz": 0.0, "ask_sz": 0.0})
                # `changes`: [side, price, qty]; treat the best as max bid / min ask we see.
                for side, price, qty in msg.get("changes", []):
                    px, q = float(price), float(qty)
                    if q == 0.0:
                        continue
                    if side == "buy" and px >= b["bid"]:
                        b["bid"], b["bid_sz"] = px, q
                    elif side == "sell" and (b["ask"] == 0.0 or px <= b["ask"]):
                        b["ask"], b["ask_sz"] = px, q
                if b["bid"] > 0 and b["ask"] > 0:
                    recv_ns = ns_now()
                    yield Quote(
                        ts_ns=recv_ns, recv_ns=recv_ns, symbol=canonical, venue=self.name,
                        bid_px=b["bid"], ask_px=b["ask"], bid_sz=b["bid_sz"], ask_sz=b["ask_sz"],
                    )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError("Gemini L2 snapshot ingest not enabled in v1.")

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("Gemini covered as spot-only.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        since_ms = start_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            for _ in range(2000):
                params = {"since": since_ms, "limit_trades": 500}
                async with sess.get(f"{self.rest_base}/v1/trades/{native}", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                # Gemini returns newest-first.
                rows = sorted(rows, key=lambda r: r["timestampms"])
                for r in rows:
                    ts_ns = int(r["timestampms"]) * 1_000_000
                    if ts_ns < start_ns:
                        continue
                    if ts_ns > end_ns:
                        out.sort(key=lambda t: t.ts_ns)
                        return out
                    out.append(Trade(
                        ts_ns=ts_ns, recv_ns=ts_ns, symbol=symbol, venue=self.name,
                        price=float(r["price"]), size=float(r["amount"]),
                        side=+1 if str(r.get("type", "")).lower() == "buy" else -1, trade_id=str(r.get("tid", "")),
                    ))
                new_since = int(rows[-1]["timestampms"]) + 1
                if new_since <= since_ms:
                    break
                since_ms = new_since
                await asyncio.sleep(0.2)
        out.sort(key=lambda t: t.ts_ns)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        tf = _TF.get(interval, "1m")
        async with aiohttp.ClientSession() as sess, sess.get(f"{self.rest_base}/v2/candles/{native}/{tf}") as resp:
            resp.raise_for_status()
            rows = await resp.json()
        out = []
        for r in rows:  # [timeMs, open, high, low, close, volume]
            ts_ns = int(r[0]) * 1_000_000
            if ts_ns < start_ns or ts_ns > end_ns:
                continue
            out.append({"ts_ns": ts_ns, "open": float(r[1]), "high": float(r[2]),
                        "low": float(r[3]), "close": float(r[4]), "volume": float(r[5]), "trades": 0})
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
