"""OKX spot adapter.

WS docs:   https://www.okx.com/docs-v5/en/#websocket-api
REST docs: https://www.okx.com/docs-v5/en/#rest-api-market-data

OKX uses a single public WS endpoint with channel subscriptions. Native instrument
ids for spot are ``BASE-QUOTE`` (already our canonical shape for the majors), but the
mapping is still resolved through ``symbols.toml`` so it stays explicit.
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

_BAR_REST = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}


class OKXSpot:
    name = "okx"

    def __init__(self) -> None:
        cfg = load_venues()["okx"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def _subscribe(self, ws, channel: str, inst_ids: list[str]) -> None:
        args = [{"channel": channel, "instId": i} for i in inst_ids]
        await ws.send(orjson.dumps({"op": "subscribe", "args": args}).decode())

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        inst = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, "trades", inst)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if (msg.get("arg") or {}).get("channel") != "trades":
                    continue
                recv_ns = ns_now()
                for t in msg.get("data", []):
                    canonical = to_canonical(t["instId"], self.name)
                    if canonical is None:
                        continue
                    yield Trade(
                        ts_ns=int(t["ts"]) * 1_000_000,
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        price=float(t["px"]),
                        size=float(t["sz"]),
                        side=+1 if t.get("side") == "buy" else -1,
                        trade_id=str(t.get("tradeId", "")),
                    )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        inst = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, "bbo-tbt", inst)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if (msg.get("arg") or {}).get("channel") != "bbo-tbt":
                    continue
                recv_ns = ns_now()
                inst_id = (msg.get("arg") or {}).get("instId")
                canonical = to_canonical(inst_id, self.name) if inst_id else None
                if canonical is None:
                    continue
                for d in msg.get("data", []):
                    bids, asks = d.get("bids") or [], d.get("asks") or []
                    if not bids or not asks:
                        continue
                    yield Quote(
                        ts_ns=int(d["ts"]) * 1_000_000,
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        bid_px=float(bids[0][0]),
                        ask_px=float(asks[0][0]),
                        bid_sz=float(bids[0][1]),
                        ask_sz=float(asks[0][1]),
                    )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError("OKX L2 snapshot ingest not enabled in v1.")

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("OKX spot has no funding; perps are out of v1 OKX scope.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        """OKX `history-trades` paginates *backwards* by tradeId. We page from the
        present back to ``start_ns`` and keep what falls inside the window."""
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        after = None  # tradeId cursor; older than this id
        async with aiohttp.ClientSession() as sess:
            for _ in range(2000):  # hard page cap
                params = {"instId": native, "limit": 100}
                if after:
                    params["after"] = after
                async with sess.get(f"{self.rest_base}/api/v5/market/history-trades", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                rows = body.get("data") or []
                if not rows:
                    break
                stop = False
                for r in rows:
                    ts_ns = int(r["ts"]) * 1_000_000
                    if ts_ns < start_ns:
                        stop = True
                        continue
                    if ts_ns > end_ns:
                        continue
                    out.append(Trade(
                        ts_ns=ts_ns, recv_ns=ts_ns, symbol=symbol, venue=self.name,
                        price=float(r["px"]), size=float(r["sz"]),
                        side=+1 if r.get("side") == "buy" else -1, trade_id=str(r.get("tradeId", "")),
                    ))
                after = rows[-1]["tradeId"]
                if stop:
                    break
                await asyncio.sleep(0.1)
        out.sort(key=lambda t: t.ts_ns)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        bar = _BAR_REST.get(interval, "1m")
        out: list[dict] = []
        # OKX `history-candles` paginates backwards via `after` (ms ts), <= 100 per call.
        after_ms = end_ns // 1_000_000
        start_ms = start_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            for _ in range(5000):
                params = {"instId": native, "bar": bar, "limit": 100, "after": after_ms}
                async with sess.get(f"{self.rest_base}/api/v5/market/history-candles", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                rows = body.get("data") or []
                if not rows:
                    break
                for r in rows:  # [ts, o, h, l, c, vol, volCcy, ...]
                    ts_ms = int(r[0])
                    if ts_ms < start_ms:
                        continue
                    out.append({
                        "ts_ns": ts_ms * 1_000_000,
                        "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]),
                        "volume": float(r[5]), "trades": 0,
                    })
                after_ms = int(rows[-1][0])
                if after_ms <= start_ms:
                    break
                await asyncio.sleep(0.1)
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
