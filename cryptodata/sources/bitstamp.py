"""Bitstamp spot adapter.

WS docs:   https://www.bitstamp.net/websocket/v2/
REST docs: https://www.bitstamp.net/api/

Native pairs are lowercase concatenations (``btcusd``, ``ethusd``). Bitstamp's public
trade REST endpoint only returns a rolling window ("minute"/"hour"/"day"), so the
backfill here is best-effort: ``fetch_trades`` returns whatever the last-24h window
exposes that falls inside the requested range, and ``fetch_klines`` uses the OHLC
endpoint which *does* support an explicit ``start``/``end``.
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

_STEP = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


class BitstampSpot:
    name = "bitstamp"

    def __init__(self) -> None:
        cfg = load_venues()["bitstamp"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        pairs = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            for p in pairs:
                await ws.send(orjson.dumps({"event": "bts:subscribe", "data": {"channel": f"live_trades_{p}"}}).decode())
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("event") != "trade":
                    continue
                chan = msg.get("channel", "")
                native = chan.replace("live_trades_", "")
                canonical = to_canonical(native, self.name)
                if canonical is None:
                    continue
                d = msg.get("data") or {}
                recv_ns = ns_now()
                # Bitstamp `microtimestamp` is µs since epoch.
                ts_us = int(d.get("microtimestamp") or (int(d.get("timestamp", 0)) * 1_000_000))
                yield Trade(
                    ts_ns=ts_us * 1_000,
                    recv_ns=recv_ns,
                    symbol=canonical,
                    venue=self.name,
                    price=float(d["price"]),
                    size=float(d["amount"]),
                    side=+1 if int(d.get("type", 0)) == 0 else -1,   # 0 = buy, 1 = sell
                    trade_id=str(d.get("id", "")),
                )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        # Bitstamp has no dedicated BBO channel; derive from order_book (top level).
        pairs = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            for p in pairs:
                await ws.send(orjson.dumps({"event": "bts:subscribe", "data": {"channel": f"order_book_{p}"}}).decode())
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("event") != "data":
                    continue
                native = msg.get("channel", "").replace("order_book_", "")
                canonical = to_canonical(native, self.name)
                if canonical is None:
                    continue
                d = msg.get("data") or {}
                bids, asks = d.get("bids") or [], d.get("asks") or []
                if not bids or not asks:
                    continue
                recv_ns = ns_now()
                ts_us = int(d.get("microtimestamp") or (int(d.get("timestamp", 0)) * 1_000_000))
                yield Quote(
                    ts_ns=ts_us * 1_000,
                    recv_ns=recv_ns,
                    symbol=canonical,
                    venue=self.name,
                    bid_px=float(bids[0][0]),
                    ask_px=float(asks[0][0]),
                    bid_sz=float(bids[0][1]),
                    ask_sz=float(asks[0][1]),
                )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError("Bitstamp L2 snapshot ingest not enabled in v1.")

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("Bitstamp is spot-only.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        async with aiohttp.ClientSession() as sess, \
                sess.get(f"{self.rest_base}/api/v2/transactions/{native}/", params={"time": "day"}) as resp:
            resp.raise_for_status()
            rows = await resp.json()
        for r in rows:
            ts_ns = int(r["date"]) * 1_000_000_000
            if ts_ns < start_ns or ts_ns > end_ns:
                continue
            out.append(Trade(
                ts_ns=ts_ns, recv_ns=ts_ns, symbol=symbol, venue=self.name,
                price=float(r["price"]), size=float(r["amount"]),
                side=+1 if int(r.get("type", 0)) == 0 else -1, trade_id=str(r.get("tid", "")),
            ))
        out.sort(key=lambda t: t.ts_ns)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        step = _STEP.get(interval, 60)
        out: list[dict] = []
        cur = start_ns // 1_000_000_000
        end_s = end_ns // 1_000_000_000
        async with aiohttp.ClientSession() as sess:
            while cur < end_s:
                params = {"step": step, "limit": 1000, "start": cur, "end": min(cur + step * 1000, end_s)}
                async with sess.get(f"{self.rest_base}/api/v2/ohlc/{native}/", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                rows = (body.get("data") or {}).get("ohlc") or []
                if not rows:
                    break
                for r in rows:
                    out.append({
                        "ts_ns": int(r["timestamp"]) * 1_000_000_000,
                        "open": float(r["open"]), "high": float(r["high"]),
                        "low": float(r["low"]), "close": float(r["close"]),
                        "volume": float(r["volume"]), "trades": 0,
                    })
                last_ts = int(rows[-1]["timestamp"])
                if last_ts <= cur:
                    break
                cur = last_ts + step
                await asyncio.sleep(0.2)
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
