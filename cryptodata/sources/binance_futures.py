"""Binance USDT-M futures adapter — funding + open interest only (v1 scope).

Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
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


class BinanceFutures:
    name = "binance_futures"

    def __init__(self) -> None:
        cfg = load_venues()["binance_futures"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        raise NotImplementedError("v1 doesn't ingest Binance perp trades (Bybit covers perp trades).")

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        raise NotImplementedError

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        raise NotImplementedError

    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]:
        """Binance Futures publishes funding on the markPrice stream every 1s."""
        native = [to_native(s, self.name) for s in symbols if to_native(s, self.name)]
        streams = "/".join(f"{n.lower()}@markPrice@1s" for n in native)
        url = f"{self.ws_url}?streams={streams}"
        async for msg in self._ws_messages(url):
            data = msg.get("data") or msg
            if data.get("e") != "markPriceUpdate":
                continue
            recv_ns = ns_now()
            canonical = to_canonical(data["s"], self.name)
            if canonical is None:
                continue
            yield Funding(
                ts_ns=_ms_to_ns(data["E"]),
                recv_ns=recv_ns,
                symbol=canonical,
                venue=self.name,
                funding_rate=float(data["r"]),
                mark_price=float(data["p"]),
                next_ts_ns=_ms_to_ns(data["T"]) if data.get("T") else None,
            )

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        """Binance OI is REST-only; poll every 60s."""
        native = list({to_native(s, self.name) for s in symbols if to_native(s, self.name)})
        while True:
            recv_ns = ns_now()
            try:
                async with aiohttp.ClientSession() as sess:
                    for n in native:
                        async with sess.get(f"{self.rest_base}/fapi/v1/openInterest", params={"symbol": n}) as resp:
                            if resp.status != 200:
                                continue
                            body = await resp.json()
                        canonical = to_canonical(n, self.name)
                        if canonical is None:
                            continue
                        yield OpenInterest(
                            ts_ns=_ms_to_ns(body["time"]),
                            recv_ns=recv_ns,
                            symbol=canonical,
                            venue=self.name,
                            oi_base=float(body["openInterest"]),
                            oi_quote=None,
                        )
            except Exception:
                pass
            await asyncio.sleep(60)

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        raise NotImplementedError

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Funding] = []
        cursor_ms = start_ns // 1_000_000
        end_ms = end_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            while cursor_ms < end_ms:
                params = {"symbol": native, "startTime": cursor_ms, "endTime": end_ms, "limit": 1000}
                async with sess.get(f"{self.rest_base}/fapi/v1/fundingRate", params=params) as resp:
                    resp.raise_for_status()
                    rows = await resp.json()
                if not rows:
                    break
                for r in rows:
                    ts_ns = _ms_to_ns(r["fundingTime"])
                    out.append(Funding(
                        ts_ns=ts_ns,
                        recv_ns=ts_ns,
                        symbol=symbol,
                        venue=self.name,
                        funding_rate=float(r["fundingRate"]),
                    ))
                if len(rows) < 1000:
                    break
                cursor_ms = rows[-1]["fundingTime"] + 1
                await asyncio.sleep(0.1)
        return out

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[dict] = []
        cursor_ms = start_ns // 1_000_000
        end_ms = end_ns // 1_000_000
        async with aiohttp.ClientSession() as sess:
            while cursor_ms < end_ms:
                params = {"symbol": native, "interval": interval, "startTime": cursor_ms, "endTime": end_ms, "limit": 1500}
                async with sess.get(f"{self.rest_base}/fapi/v1/klines", params=params) as resp:
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
                if len(rows) < 1500:
                    break
                cursor_ms = rows[-1][0] + 60_000
                await asyncio.sleep(0.05)
        return out

    async def _ws_messages(self, url: str):
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
