"""Binance Spot adapter.

WS docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
REST docs: https://developers.binance.com/docs/binance-spot-api-docs/rest-api

We use the combined-stream endpoint so a single WS carries trades+bookTicker+depth
for many symbols. Symbol multiplexing fan-in happens here, fan-out to per-symbol
canonical rows happens in the parse path.
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


def _ms_to_ns(ms: int) -> int:
    return int(ms) * 1_000_000


class BinanceSpot:
    name = "binance"

    def __init__(self) -> None:
        cfg = load_venues()["binance"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    # ---------- streams ----------

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        streams = "/".join(f"{n.lower()}@trade" for n in native)
        url = f"{self.ws_url}?streams={streams}"
        async for msg in self._ws_messages(url):
            data = msg.get("data") or msg
            if data.get("e") != "trade":
                continue
            recv_ns = ns_now()
            canonical = to_canonical(data["s"], self.name)
            if canonical is None:
                continue
            yield Trade(
                ts_ns=_ms_to_ns(data["T"]),
                recv_ns=recv_ns,
                symbol=canonical,
                venue=self.name,
                price=float(data["p"]),
                size=float(data["q"]),
                # Binance: "m" = true means buyer is the market maker => sell aggressor
                side=-1 if data.get("m") else +1,
                trade_id=str(data["t"]),
            )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        streams = "/".join(f"{n.lower()}@bookTicker" for n in native)
        url = f"{self.ws_url}?streams={streams}"
        async for msg in self._ws_messages(url):
            data = msg.get("data") or msg
            # bookTicker frames don't carry an event-type field on the combined stream
            if "s" not in data or "b" not in data:
                continue
            recv_ns = ns_now()
            canonical = to_canonical(data["s"], self.name)
            if canonical is None:
                continue
            # bookTicker has no exchange timestamp; use recv_ns - 1ms as ts proxy
            yield Quote(
                ts_ns=recv_ns - 1_000_000,
                recv_ns=recv_ns,
                symbol=canonical,
                venue=self.name,
                bid_px=float(data["b"]),
                ask_px=float(data["a"]),
                bid_sz=float(data["B"]),
                ask_sz=float(data["A"]),
            )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        # Binance partial-book stream at fixed depths 5/10/20, updated every 100ms.
        # We pick the smallest depth >= request and downsample to snapshot_interval.
        valid_depths = [5, 10, 20]
        d = next((x for x in valid_depths if x >= depth), 20)
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        streams = "/".join(f"{n.lower()}@depth{d}@100ms" for n in native)
        url = f"{self.ws_url}?streams={streams}"
        async for msg in self._ws_messages(url):
            data = msg.get("data") or msg
            if "bids" not in data:
                continue
            stream = msg.get("stream", "")
            native_sym = stream.split("@")[0].upper() if "@" in stream else None
            canonical = to_canonical(native_sym, self.name) if native_sym else None
            if canonical is None:
                continue
            recv_ns = ns_now()
            yield BookSnapshot(
                ts_ns=recv_ns,    # partial book stream has no event ts
                recv_ns=recv_ns,
                symbol=canonical,
                venue=self.name,
                depth=d,
                bids=[{"px": float(p), "sz": float(q)} for p, q in data["bids"]],
                asks=[{"px": float(p), "sz": float(q)} for p, q in data["asks"]],
            )

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        raise NotImplementedError("Binance Spot has no funding; use BinanceFutures.")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError("Binance Spot has no open interest; use BinanceFutures.")

    # ---------- REST ----------

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        """Backfill aggTrades (compact, includes aggressor side)."""
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        cursor_ms = start_ns // 1_000_000
        end_ms = end_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            while cursor_ms < end_ms:
                params = {"symbol": native, "startTime": cursor_ms, "endTime": end_ms, "limit": 1000}
                async with sess.get(f"{self.rest_base}/api/v3/aggTrades", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                for r in rows:
                    out.append(Trade(
                        ts_ns=_ms_to_ns(r["T"]),
                        recv_ns=_ms_to_ns(r["T"]),  # no separate recv on backfill
                        symbol=symbol,
                        venue=self.name,
                        price=float(r["p"]),
                        size=float(r["q"]),
                        side=-1 if r["m"] else +1,
                        trade_id=str(r["a"]),
                    ))
                # Advance past the last trade time + 1ms to avoid duplicates.
                cursor_ms = rows[-1]["T"] + 1
                if len(rows) < 1000:
                    break
                await asyncio.sleep(0.05)   # gentle rate limiting
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError("Binance Spot has no funding; use BinanceFutures.")

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        """REST kline backfill for symbols that lack trade history (or as a sanity reference)."""
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[dict] = []
        cursor_ms = start_ns // 1_000_000
        end_ms = end_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            while cursor_ms < end_ms:
                params = {
                    "symbol": native,
                    "interval": interval,
                    "startTime": cursor_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                }
                async with sess.get(f"{self.rest_base}/api/v3/klines", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                for r in rows:
                    out.append({
                        "ts_ns": _ms_to_ns(r[0]),
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "trades": int(r[8]),
                    })
                cursor_ms = rows[-1][0] + 60_000   # 1m step; works for finer intervals too thanks to limit
                if len(rows) < 1000:
                    break
                await asyncio.sleep(0.05)
        return out

    # ---------- WS plumbing ----------

    async def _ws_messages(self, url: str):
        """Connect to a Binance combined-stream WS and yield decoded JSON messages.

        Reconnect on disconnect with exponential backoff capped at 60s.
        Caller controls cancellation via task cancellation.
        """
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024) as ws:
                    backoff = 1.0
                    async for raw in ws:
                        try:
                            yield orjson.loads(raw)
                        except orjson.JSONDecodeError:
                            continue
            except (TimeoutError, websockets.ConnectionClosed, OSError):
                await asyncio.sleep(min(backoff, 60.0))
                backoff = min(backoff * 2, 60.0)
