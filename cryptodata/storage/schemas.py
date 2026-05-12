"""Canonical pyarrow schemas for every table in the dataset.

Timestamps are stored as int64 UTC nanoseconds. Conversion to pandas
datetime64[ns, UTC] happens at the query boundary.
"""
from __future__ import annotations

import pyarrow as pa

# ----- raw -----

TRADES_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("recv_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("size", pa.float64(), nullable=False),
    pa.field("side", pa.int8(), nullable=False),       # +1 buy aggressor, -1 sell, 0 unknown
    pa.field("trade_id", pa.string(), nullable=True),
    pa.field("ingested_at_ns", pa.int64(), nullable=False),
])

QUOTES_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("recv_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("bid_px", pa.float64(), nullable=False),
    pa.field("ask_px", pa.float64(), nullable=False),
    pa.field("bid_sz", pa.float64(), nullable=False),
    pa.field("ask_sz", pa.float64(), nullable=False),
    pa.field("ingested_at_ns", pa.int64(), nullable=False),
])

_LEVEL_STRUCT = pa.struct([
    pa.field("px", pa.float64()),
    pa.field("sz", pa.float64()),
])

BOOK_L2_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("recv_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("depth", pa.int16(), nullable=False),
    pa.field("bids", pa.list_(_LEVEL_STRUCT), nullable=False),
    pa.field("asks", pa.list_(_LEVEL_STRUCT), nullable=False),
    pa.field("ingested_at_ns", pa.int64(), nullable=False),
])

FUNDING_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("recv_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("funding_rate", pa.float64(), nullable=False),
    pa.field("mark_price", pa.float64(), nullable=True),
    pa.field("next_ts_ns", pa.int64(), nullable=True),
    pa.field("ingested_at_ns", pa.int64(), nullable=False),
])

OPEN_INTEREST_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("recv_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("oi_base", pa.float64(), nullable=True),
    pa.field("oi_quote", pa.float64(), nullable=True),
    pa.field("ingested_at_ns", pa.int64(), nullable=False),
])

# ----- derived -----

BARS_1S_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),     # left edge of the second
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),    # 'agg' for cross-venue bars
    pa.field("open", pa.float64(), nullable=False),
    pa.field("high", pa.float64(), nullable=False),
    pa.field("low", pa.float64(), nullable=False),
    pa.field("close", pa.float64(), nullable=False),
    pa.field("volume", pa.float64(), nullable=False),
    pa.field("vwap", pa.float64(), nullable=False),
    pa.field("trades", pa.int32(), nullable=False),
    pa.field("sources_mask", pa.int32(), nullable=False),  # populated for venue='agg', 0 otherwise
    # When this bar row was written (epoch ns). Enables point-in-time / "as-of" reads
    # of the derived layer. Nullable for compatibility with v0 parts written before
    # this column existed.
    pa.field("ingested_at_ns", pa.int64(), nullable=True),
])

# Reference klines from a single venue (REST-backfilled OHLCV at venue-native
# granularity). Kept separate from derived 1s bars so the consolidated tape never
# silently mixes our own per-second roll-ups with a venue's published candles.
BARS_REF_SCHEMA = pa.schema([
    pa.field("ts_ns", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("interval", pa.string(), nullable=False),  # '1m', '5m', '1h', ...
    pa.field("open", pa.float64(), nullable=False),
    pa.field("high", pa.float64(), nullable=False),
    pa.field("low", pa.float64(), nullable=False),
    pa.field("close", pa.float64(), nullable=False),
    pa.field("volume", pa.float64(), nullable=False),
    pa.field("trades", pa.int32(), nullable=False),
    pa.field("ingested_at_ns", pa.int64(), nullable=True),
])

SCHEMAS: dict[str, pa.Schema] = {
    "trades": TRADES_SCHEMA,
    "quotes": QUOTES_SCHEMA,
    "book_l2_snapshot": BOOK_L2_SNAPSHOT_SCHEMA,
    "funding": FUNDING_SCHEMA,
    "open_interest": OPEN_INTEREST_SCHEMA,
    "bars_1s": BARS_1S_SCHEMA,
    "bars_ref": BARS_REF_SCHEMA,
}


def schema_for(table: str) -> pa.Schema:
    if table not in SCHEMAS:
        raise KeyError(f"unknown table: {table!r}; known: {sorted(SCHEMAS)}")
    return SCHEMAS[table]
