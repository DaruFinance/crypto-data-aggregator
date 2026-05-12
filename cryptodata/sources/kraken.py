"""Kraken Spot adapter (WS v2).

Docs: https://docs.kraken.com/api/docs/websocket-v2/
Channels used: `trade`, `ticker`, `book`.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aiohttp
import orjson
import websockets

from cryptodata.core.symbols import symbols_for_venue, to_native
from cryptodata.paths import load_venues
from cryptodata.sources.base import BookSnapshot, Funding, OpenInterest, Quote, Trade, ns_now


def _kraken_ws_pair(canonical: str) -> str:
    """Kraken WS v2 identifies pairs as ``BASE/QUOTE`` (it migrated off the legacy
    ``XBT``/``X.../Z...`` REST altnames). Our canonical form is already ``BASE-QUOTE``,
    so the WS form is just a slash swap."""
    return canonical.replace("-PERP", "").replace("-", "/")


def _iso_to_ns(iso: str) -> int:
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000_000)


class Kraken:
    name = "kraken"

    def __init__(self) -> None:
        cfg = load_venues()["kraken"]
        self.ws_url = cfg["ws_url"]
        self.rest_base = cfg["rest_base"]
        # WS v2 returns BASE/QUOTE; map it back to canonical. (REST still uses the
        # `XBTUSD`-style altnames held in symbols.toml — those are resolved by to_native.)
        self._ws_pair_to_canonical: dict[str, str] = {}
        for canonical, _native in symbols_for_venue(self.name):
            self._ws_pair_to_canonical[_kraken_ws_pair(canonical)] = canonical
            self._ws_pair_to_canonical[_kraken_ws_pair(canonical).replace("/", "")] = canonical

    def _canon(self, ws_pair: str) -> str | None:
        return self._ws_pair_to_canonical.get(ws_pair) or self._ws_pair_to_canonical.get(ws_pair.replace("/", ""))

    async def _subscribe(self, ws, channel: str, symbols: list[str], **extra) -> None:
        params = {"channel": channel, "symbol": symbols}
        params.update(extra)
        await ws.send(orjson.dumps({"method": "subscribe", "params": params}).decode())

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]:
        ws_pairs = [_kraken_ws_pair(s) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, "trade", ws_pairs)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "trade":
                    continue
                recv_ns = ns_now()
                for t in msg.get("data", []):
                    canonical = self._canon(t.get("symbol", ""))
                    if canonical is None:
                        continue
                    side = +1 if t.get("side") == "buy" else -1
                    yield Trade(
                        ts_ns=_iso_to_ns(t["timestamp"]),
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        price=float(t["price"]),
                        size=float(t["qty"]),
                        side=side,
                        trade_id=str(t.get("trade_id", "")),
                    )

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]:
        ws_pairs = [_kraken_ws_pair(s) for s in symbols if to_native(s, self.name)]
        async for ws in self._connect():
            await self._subscribe(ws, "ticker", ws_pairs)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "ticker":
                    continue
                recv_ns = ns_now()
                for tk in msg.get("data", []):
                    canonical = self._canon(tk.get("symbol", ""))
                    if canonical is None:
                        continue
                    yield Quote(
                        ts_ns=recv_ns,
                        recv_ns=recv_ns,
                        symbol=canonical,
                        venue=self.name,
                        bid_px=float(tk.get("bid", 0.0)),
                        ask_px=float(tk.get("ask", 0.0)),
                        bid_sz=float(tk.get("bid_qty", 0.0)),
                        ask_sz=float(tk.get("ask_qty", 0.0)),
                    )

    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]:
        # Kraken WS book channel: depth ∈ {10, 25, 100, 500, 1000}
        d = next((x for x in [10, 25, 100, 500, 1000] if x >= depth), 25)
        ws_pairs = [_kraken_ws_pair(s) for s in symbols if to_native(s, self.name)]
        books: dict[str, dict[str, dict[float, float]]] = {}
        last_emit_ns: dict[str, int] = {}
        EMIT_INTERVAL_NS = 5_000_000_000
        async for ws in self._connect():
            await self._subscribe(ws, "book", ws_pairs, depth=d)
            async for raw in ws:
                try:
                    msg = orjson.loads(raw)
                except orjson.JSONDecodeError:
                    continue
                if msg.get("channel") != "book":
                    continue
                recv_ns = ns_now()
                msg_type = msg.get("type")
                for b in msg.get("data", []):
                    pair = b.get("symbol", "")
                    canonical = self._canon(pair)
                    if canonical is None:
                        continue
                    if pair not in books:
                        books[pair] = {"bid": {}, "ask": {}}
                        last_emit_ns[pair] = 0
                    if msg_type == "snapshot":
                        books[pair]["bid"].clear()
                        books[pair]["ask"].clear()
                    for side_key in ("bids", "asks"):
                        for lvl in b.get(side_key, []):
                            px = float(lvl["price"])
                            sz = float(lvl["qty"])
                            side = "bid" if side_key == "bids" else "ask"
                            if sz == 0.0:
                                books[pair][side].pop(px, None)
                            else:
                                books[pair][side][px] = sz
                    if recv_ns - last_emit_ns[pair] < EMIT_INTERVAL_NS:
                        continue
                    last_emit_ns[pair] = recv_ns
                    bids_sorted = sorted(books[pair]["bid"].items(), reverse=True)[:depth]
                    asks_sorted = sorted(books[pair]["ask"].items())[:depth]
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
        raise NotImplementedError("Kraken funding not in v1 (use Bybit / Binance Futures).")

    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]:
        raise NotImplementedError

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        out: list[Trade] = []
        since = start_ns
        async with aiohttp.ClientSession() as sess:
            while since < end_ns:
                params = {"pair": native, "since": since}
                async with sess.get(f"{self.rest_base}/0/public/Trades", params=params) as resp:
                    resp.raise_for_status()
                    body = await resp.json()
                result = body.get("result") or {}
                # Result has a `last` cursor + a single key with the trade list.
                trade_list = next((v for k, v in result.items() if k != "last"), [])
                last_cursor = int(result.get("last", since))
                if not trade_list:
                    break
                for r in trade_list:
                    # r = [price, volume, time, side, ord_type, misc, trade_id]
                    ts_ns = int(float(r[2]) * 1_000_000_000)
                    if ts_ns > end_ns:
                        break
                    side = +1 if r[3] == "b" else -1
                    out.append(Trade(
                        ts_ns=ts_ns,
                        recv_ns=ts_ns,
                        symbol=symbol,
                        venue=self.name,
                        price=float(r[0]),
                        size=float(r[1]),
                        side=side,
                        trade_id=str(r[6]) if len(r) > 6 else None,
                    ))
                if last_cursor <= since:
                    break
                since = last_cursor
                await asyncio.sleep(1.5)   # Kraken is rate-limit sensitive
        return out

    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]:
        raise NotImplementedError

    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]:
        native = to_native(symbol, self.name)
        if not native:
            return []
        minutes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}.get(interval, 1)
        out: list[dict] = []
        since_s = start_ns // 1_000_000_000
        async with aiohttp.ClientSession() as sess:
            params = {"pair": native, "interval": minutes, "since": since_s}
            async with sess.get(f"{self.rest_base}/0/public/OHLC", params=params) as resp:
                resp.raise_for_status()
                body = await resp.json()
            result = body.get("result") or {}
            rows = next((v for k, v in result.items() if k != "last"), [])
            for r in rows:
                ts_ns = int(r[0]) * 1_000_000_000
                if ts_ns < start_ns or ts_ns > end_ns:
                    continue
                out.append({
                    "ts_ns": ts_ns,
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": float(r[6]),
                    "trades": int(r[7]) if len(r) > 7 else 0,
                })
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
