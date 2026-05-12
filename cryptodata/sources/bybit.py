"""Bybit V5 linear perp adapter (funding + open interest + perp trades).

Docs: https://bybit-exchange.github.io/docs/v5/ws/connect
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


def _ms_to_ns(ms) -> int:
    return int(ms) * 1_000_000


class BybitPerp:
    name = "bybit"

    def __init__(self) -> None:
        cfg = load_venues()["bybit"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def _subscribe(self, ws, topics: list[str]) -> None:
        await ws.send(orjson.dumps({"op": "subscribe", "args": topics}).decode())

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        topics = [f"publicTrade.{n}" for n in native]
        async for ws in self._connect():
            await self._subscribe(ws, topics)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if not msg.get("topic", "").startswith("publicTrade."):
                    continue
                recv_ns = ns_now()
                for t in msg.get("data", []):
                    canonical = to_canonical(t["s"], self.name)
                    if canonical is None:
                        continue
                    side = +1 if t.get("S") == "Buy" else -1
                    yield Trade(
                        ts_ns=_ms_to_ns(t["T"]),
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        price=float(t["p"]),
                        size=float(t["v"]),
                        side=side,
                        trade_id=str(t.get("i", "")),
                    )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        # Bybit doesn't have a separate BBO channel; use orderbook.1 (top of book)
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        topics = [f"orderbook.1.{n}" for n in native]
        async for ws in self._connect():
            await self._subscribe(ws, topics)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                topic = msg.get("topic", "")
                if not topic.startswith("orderbook.1."):
                    continue
                recv_ns = ns_now()
                data = msg.get("data") or {}
                bids = data.get("b") or []
                asks = data.get("a") or []
                if not bids or not asks:
                    continue
                canonical = to_canonical(data.get("s", ""), self.name)
                if canonical is None:
                    continue
                yield Quote(
                    ts_ns=_ms_to_ns(msg.get("ts", recv_ns // 1_000_000)),
                    recv_ns=recv_ns,
                    symbol=canonical,
                    venue=self.name,
                    bid_px=float(bids[0][0]),
                    ask_px=float(asks[0][0]),
                    bid_sz=float(bids[0][1]),
                    ask_sz=float(asks[0][1]),
                )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError("Bybit L2 snapshot ingest not enabled in v1 (Binance + Coinbase only).")

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        """Bybit publishes funding rate on tickers.<symbol> channel."""
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        topics = [f"tickers.{n}" for n in native]
        async for ws in self._connect():
            await self._subscribe(ws, topics)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                topic = msg.get("topic", "")
                if not topic.startswith("tickers."):
                    continue
                d = msg.get("data") or {}
                if "fundingRate" not in d:
                    continue
                recv_ns = ns_now()
                canonical = to_canonical(d.get("symbol", ""), self.name)
                if canonical is None:
                    continue
                # Bybit settlement is on `nextFundingTime` ms epoch.
                next_ms = int(d.get("nextFundingTime") or 0)
                yield Funding(
                    ts_ns=_ms_to_ns(msg.get("ts", recv_ns // 1_000_000)),
                    recv_ns=recv_ns,
                    symbol=canonical,
                    venue=self.name,
                    funding_rate=float(d["fundingRate"]),
                    mark_price=float(d["markPrice"]) if d.get("markPrice") else None,
                    next_ts_ns=_ms_to_ns(next_ms) if next_ms else None,
                )

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        # Bybit OI not on WS; poll via REST every 60s. Wrapped here as a stream.
        native = list({to_native(s, self.name) for s in symbols if to_native(s, self.name)})
        while True:
            recv_ns = ns_now()
            try:
                async with aiohttp.ClientSession() as sess:
                    for n in native:
                        params = {"category": "linear", "symbol": n, "intervalTime": "5min", "limit": 1}
                        async with sess.get(f"{self.rest_base}/v5/market/open-interest", params=params) as resp:
                            if resp.status != 200:
                                continue
                            body = await resp.json()
                        rows = (body.get("result") or {}).get("list") or []
                        if not rows:
                            continue
                        canonical = to_canonical(n, self.name)
                        if canonical is None:
                            continue
                        r = rows[0]
                        yield OpenInterest(
                            ts_ns=_ms_to_ns(r["timestamp"]),
                            recv_ns=recv_ns,
                            symbol=canonical,
                            venue=self.name,
                            oi_base=float(r["openInterest"]),
                            oi_quote=None,
                        )
            except Exception:
                pass
            await asyncio.sleep(60)

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        cursor_ns = start_ns
        async with aiohttp.ClientSession() as sess:
            while cursor_ns < end_ns:
                params = {
                    "category": "linear", "symbol": native,
                    "start": cursor_ns // 1_000_000, "end": end_ns // 1_000_000, "limit": 1000,
                }
                async with sess.get(f"{self.rest_base}/v5/market/recent-trade", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                rows = (body.get("result") or {}).get("list") or []
                if not rows:
                    break
                for r in rows:
                    ts_ns = _ms_to_ns(r["time"])
                    out.append(Trade(
                        ts_ns=ts_ns,
                        recv_ns=ts_ns,
                        symbol=symbol,
                        venue=self.name,
                        price=float(r["price"]),
                        size=float(r["size"]),
                        side=+1 if r["side"] == "Buy" else -1,
                        trade_id=str(r.get("execId", "")),
                    ))
                if len(rows) < 1000:
                    break
                cursor_ns = _ms_to_ns(rows[-1]["time"]) + 1_000_000
                await asyncio.sleep(0.05)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Funding] = []
        async with aiohttp.ClientSession() as sess:
            params = {"category": "linear", "symbol": native, "startTime": start_ns // 1_000_000, "endTime": end_ns // 1_000_000, "limit": 200}
            async with sess.get(f"{self.rest_base}/v5/market/funding/history", params=params) as resp:
                resp.raise_for_status()
                body = await resp.json()
            for r in (body.get("result") or {}).get("list", []):
                ts_ns = _ms_to_ns(r["fundingRateTimestamp"])
                out.append(Funding(
                    ts_ns=ts_ns,
                    recv_ns=ts_ns,
                    symbol=symbol,
                    venue=self.name,
                    funding_rate=float(r["fundingRate"]),
                ))
        return out

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[dict] = []
        cursor_ms = start_ns // 1_000_000
        end_ms = end_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            while cursor_ms < end_ms:
                params = {"category": "linear", "symbol": native, "interval": interval, "start": cursor_ms, "end": end_ms, "limit": 1000}
                async with sess.get(f"{self.rest_base}/v5/market/kline", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                rows = (body.get("result") or {}).get("list") or []
                if not rows:
                    break
                # Bybit returns newest first
                for r in reversed(rows):
                    out.append({
                        "ts_ns": _ms_to_ns(r[0]),
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "trades": 0,
                    })
                if len(rows) < 1000:
                    break
                cursor_ms = int(rows[0][0]) + 60_000
                await asyncio.sleep(0.05)
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
