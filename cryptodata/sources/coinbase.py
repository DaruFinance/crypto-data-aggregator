"""Coinbase Advanced Trade adapter.

Docs: https://docs.cloud.coinbase.com/advanced-trade-api/docs/ws-overview
WS endpoint serves all channels; subscribe to `market_trades`, `ticker`, `level2`.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aiohttp
import orjson
import websockets

from cryptodata.core.symbols import to_canonical, to_native
from cryptodata.paths import load_venues
from cryptodata.sources.base import BookSnapshot, Funding, OpenInterest, Quote, Trade, ns_now


def _iso_to_ns(iso: str) -> int:
    """Parse RFC3339/ISO8601 with optional 'Z' or +HH:MM."""
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000_000)


class Coinbase:
    name = "coinbase"

    def __init__(self) -> None:
        cfg = load_venues()["coinbase"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def _subscribe(self, ws, product_ids: list[str], channel: str) -> None:
        msg = {"type": "subscribe", "channel": channel, "product_ids": product_ids}
        await ws.send(orjson.dumps(msg).decode())

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        product_ids = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, product_ids, "market_trades")
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "market_trades":
                    continue
                recv_ns = ns_now()
                for ev in msg.get("events", []):
                    for t in ev.get("trades", []):
                        canonical = to_canonical(t["product_id"], self.name)
                        if canonical is None:
                            continue
                        side = +1 if t["side"].upper() == "BUY" else -1
                        yield Trade(
                            ts_ns=_iso_to_ns(t["time"]),
                            recv_ns=recv_ns,
                            symbol=canonical,
                            venue=self.name,
                            price=float(t["price"]),
                            size=float(t["size"]),
                            side=side,
                            trade_id=str(t.get("trade_id", "")),
                        )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        product_ids = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, product_ids, "ticker")
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "ticker":
                    continue
                recv_ns = ns_now()
                for ev in msg.get("events", []):
                    for tk in ev.get("tickers", []):
                        canonical = to_canonical(tk["product_id"], self.name)
                        if canonical is None:
                            continue
                        # ticker channel publishes mid + spread but bid/ask px are present
                        bid = float(tk.get("best_bid") or tk.get("price") or 0.0)
                        ask = float(tk.get("best_ask") or tk.get("price") or 0.0)
                        bid_sz = float(tk.get("best_bid_quantity") or 0.0)
                        ask_sz = float(tk.get("best_ask_quantity") or 0.0)
                        ts = msg.get("timestamp")
                        yield Quote(
                            ts_ns=_iso_to_ns(ts) if ts else recv_ns,
                            recv_ns=recv_ns,
                            symbol=canonical,
                            venue=self.name,
                            bid_px=bid,
                            ask_px=ask,
                            bid_sz=bid_sz,
                            ask_sz=ask_sz,
                        )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        # Coinbase publishes l2_data as snapshot + updates. v1: emit a snapshot every N events
        # by rebuilding the top-depth book from accumulated state.
        product_ids = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        books: dict[str, dict[str, dict[float, float]]] = {
            p: {"bid": {}, "offer": {}} for p in product_ids
        }
        last_emit_ns: dict[str, int] = dict.fromkeys(product_ids, 0)
        EMIT_INTERVAL_NS = 5_000_000_000  # 5 seconds
        async for ws in self._connect():
            await self._subscribe(ws, product_ids, "level2")
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "l2_data":
                    continue
                recv_ns = ns_now()
                for ev in msg.get("events", []):
                    pid = ev.get("product_id")
                    if pid not in books:
                        continue
                    for u in ev.get("updates", []):
                        side = u["side"]
                        px = float(u["price_level"])
                        sz = float(u["new_quantity"])
                        if sz == 0.0:
                            books[pid][side].pop(px, None)
                        else:
                            books[pid][side][px] = sz
                    if recv_ns - last_emit_ns[pid] < EMIT_INTERVAL_NS:
                        continue
                    last_emit_ns[pid] = recv_ns
                    canonical = to_canonical(pid, self.name)
                    if canonical is None:
                        continue
                    bids_sorted = sorted(books[pid]["bid"].items(), reverse=True)[:depth]
                    asks_sorted = sorted(books[pid]["offer"].items())[:depth]
                    yield BookSnapshot(
                        ts_ns=recv_ns,
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        depth=depth,
                        bids=[{"px": p, "sz": s} for p, s in bids_sorted],
                        asks=[{"px": p, "sz": s} for p, s in asks_sorted],
                    )

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("Coinbase has no perp funding in this adapter.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError("Coinbase has no perp OI in this adapter.")

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        """REST historical trades. Coinbase only exposes the most recent ~1000 by default;
        deep history requires the public market-data endpoints with cursor pagination."""
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        cursor = None
        async with aiohttp.ClientSession() as sess:
            while True:
                url = f"{self.rest_base}/products/{native}/trades"
                params = {"limit": 1000}
                if cursor:
                    params["before"] = cursor
                async with sess.get(url, params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                stop = False
                for r in rows:
                    ts_ns = _iso_to_ns(r["time"])
                    if ts_ns < start_ns:
                        stop = True
                        break
                    if ts_ns > end_ns:
                        continue
                    side = +1 if r["side"].upper() == "BUY" else -1
                    out.append(Trade(
                        ts_ns=ts_ns,
                        recv_ns=ts_ns,
                        symbol=symbol,
                        venue=self.name,
                        price=float(r["price"]),
                        size=float(r["size"]),
                        side=side,
                        trade_id=str(r.get("trade_id", "")),
                    ))
                if stop or len(rows) < 1000:
                    break
                cursor = str(rows[-1]["trade_id"])
                await asyncio.sleep(0.15)
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        granularity = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}.get(interval, 60)
        out: list[dict] = []
        # Coinbase returns at most 300 candles per call
        step_ns = granularity * 300 * 1_000_000_000
        cur = start_ns
        async with aiohttp.ClientSession() as sess:
            while cur < end_ns:
                stop_ns = min(cur + step_ns, end_ns)
                params = {
                    "start": datetime.fromtimestamp(cur / 1e9, tz=UTC).isoformat(),
                    "end": datetime.fromtimestamp(stop_ns / 1e9, tz=UTC).isoformat(),
                    "granularity": granularity,
                }
                url = f"{self.rest_base}/products/{native}/candles"
                async with sess.get(url, params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                for r in rows:
                    out.append({
                        "ts_ns": int(r[0]) * 1_000_000_000,
                        "low": float(r[1]),
                        "high": float(r[2]),
                        "open": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "trades": 0,
                    })
                cur = stop_ns
                await asyncio.sleep(0.15)
        return out

    async def _connect(self):
        """Async generator yielding a connected WS. Reconnects with backoff."""
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
