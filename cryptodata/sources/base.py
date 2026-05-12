"""The `Source` protocol — every venue adapter implements this.

Streams yield normalized rows ready to hand to the parquet writer. Adapters are
responsible for venue-native symbol translation (via `cryptodata.core.symbols`),
timestamp normalization (to UTC ns), and side normalization (+1 buy, -1 sell,
0 unknown).

Adapters do NOT decide what to ingest — that's the orchestrator's job. They expose
all the streams their venue supports; the orchestrator selects which to subscribe to
based on `config/ingest.toml`.
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


def ns_now() -> int:
    return time.time_ns()


# ----- normalized row types -----

@dataclass(slots=True)
class Trade:
    ts_ns: int
    recv_ns: int
    symbol: str
    venue: str
    price: float
    size: float
    side: int  # +1, -1, 0
    trade_id: str | None = None
    ingested_at_ns: int = field(default_factory=ns_now)

    def to_row(self) -> dict:
        return {
            "ts_ns": int(self.ts_ns),
            "recv_ns": int(self.recv_ns),
            "symbol": self.symbol,
            "venue": self.venue,
            "price": float(self.price),
            "size": float(self.size),
            "side": int(self.side),
            "trade_id": self.trade_id,
            "ingested_at_ns": int(self.ingested_at_ns),
        }


@dataclass(slots=True)
class Quote:
    ts_ns: int
    recv_ns: int
    symbol: str
    venue: str
    bid_px: float
    ask_px: float
    bid_sz: float
    ask_sz: float
    ingested_at_ns: int = field(default_factory=ns_now)

    def to_row(self) -> dict:
        return {
            "ts_ns": int(self.ts_ns),
            "recv_ns": int(self.recv_ns),
            "symbol": self.symbol,
            "venue": self.venue,
            "bid_px": float(self.bid_px),
            "ask_px": float(self.ask_px),
            "bid_sz": float(self.bid_sz),
            "ask_sz": float(self.ask_sz),
            "ingested_at_ns": int(self.ingested_at_ns),
        }


@dataclass(slots=True)
class BookSnapshot:
    ts_ns: int
    recv_ns: int
    symbol: str
    venue: str
    depth: int
    bids: list[dict]  # [{"px": float, "sz": float}, ...]
    asks: list[dict]
    ingested_at_ns: int = field(default_factory=ns_now)

    def to_row(self) -> dict:
        return {
            "ts_ns": int(self.ts_ns),
            "recv_ns": int(self.recv_ns),
            "symbol": self.symbol,
            "venue": self.venue,
            "depth": int(self.depth),
            "bids": self.bids,
            "asks": self.asks,
            "ingested_at_ns": int(self.ingested_at_ns),
        }


@dataclass(slots=True)
class Funding:
    ts_ns: int
    recv_ns: int
    symbol: str
    venue: str
    funding_rate: float
    mark_price: float | None = None
    next_ts_ns: int | None = None
    ingested_at_ns: int = field(default_factory=ns_now)

    def to_row(self) -> dict:
        return {
            "ts_ns": int(self.ts_ns),
            "recv_ns": int(self.recv_ns),
            "symbol": self.symbol,
            "venue": self.venue,
            "funding_rate": float(self.funding_rate),
            "mark_price": float(self.mark_price) if self.mark_price is not None else None,
            "next_ts_ns": int(self.next_ts_ns) if self.next_ts_ns is not None else None,
            "ingested_at_ns": int(self.ingested_at_ns),
        }


@dataclass(slots=True)
class OpenInterest:
    ts_ns: int
    recv_ns: int
    symbol: str
    venue: str
    oi_base: float | None
    oi_quote: float | None
    ingested_at_ns: int = field(default_factory=ns_now)

    def to_row(self) -> dict:
        return {
            "ts_ns": int(self.ts_ns),
            "recv_ns": int(self.recv_ns),
            "symbol": self.symbol,
            "venue": self.venue,
            "oi_base": float(self.oi_base) if self.oi_base is not None else None,
            "oi_quote": float(self.oi_quote) if self.oi_quote is not None else None,
            "ingested_at_ns": int(self.ingested_at_ns),
        }


# ----- protocol -----

@runtime_checkable
class Source(Protocol):
    """Every venue adapter implements this. Streams that the venue doesn't support
    may raise NotImplementedError."""

    name: str

    async def stream_trades(self, symbols: list[str]) -> AsyncIterator[Trade]: ...
    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[Quote]: ...
    async def stream_book(self, symbols: list[str], depth: int) -> AsyncIterator[BookSnapshot]: ...
    async def stream_funding(self, symbols: list[str]) -> AsyncIterator[Funding]: ...
    async def stream_open_interest(self, symbols: list[str]) -> AsyncIterator[OpenInterest]: ...

    async def fetch_trades(self, symbol: str, start_ns: int, end_ns: int) -> list[Trade]: ...
    async def fetch_funding(self, symbol: str, start_ns: int, end_ns: int) -> list[Funding]: ...
    async def fetch_klines(self, symbol: str, start_ns: int, end_ns: int, interval: str = "1m") -> list[dict]: ...
