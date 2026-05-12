"""Batched partitioned Parquet writer.

One writer instance is responsible for one (table, symbol, venue) partition.
Rows are accumulated in memory and flushed when either the row count or wall-clock
threshold is exceeded. Files are rotated hourly so a crash never loses more than
the last partial batch.

Design choices:
- One file per (table, symbol, venue, date, rotation-window). Compacted nightly.
- ZSTD compression level 6 — good size/speed tradeoff for time-series.
- Dictionary encoding for `symbol` and `venue` (huge wins given low cardinality).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from cryptodata.paths import partition_path
from cryptodata.storage.schemas import schema_for


def ns_now() -> int:
    return time.time_ns()


def date_str(ts_ns: int) -> str:
    """UTC date for the given epoch-ns timestamp."""
    secs = ts_ns // 1_000_000_000
    return time.strftime("%Y-%m-%d", time.gmtime(secs))


def hour_bucket(ts_ns: int) -> int:
    """Hour-since-epoch bucket — used as the rotation key inside a date partition."""
    return ts_ns // (3_600 * 1_000_000_000)


@dataclass
class _Buffer:
    rows: list[dict[str, Any]] = field(default_factory=list)
    opened_at_ns: int = field(default_factory=ns_now)
    hour: int = -1


class PartitionedParquetWriter:
    """Thread-safe writer that buckets rows by (table, symbol, venue, date, hour).

    Call `append(table, row)` for each row; call `flush_all()` periodically (or
    when batch thresholds fire) to write buffered rows out to Parquet.
    """

    def __init__(self, *, batch_rows: int = 50_000, batch_seconds: int = 60) -> None:
        self.batch_rows = batch_rows
        self.batch_seconds = batch_seconds
        self._buffers: dict[tuple[str, str, str, int], _Buffer] = {}
        self._lock = Lock()

    # ----- public API -----

    def append(self, table: str, row: dict[str, Any]) -> None:
        ts_ns = row["ts_ns"]
        symbol = row["symbol"]
        venue = row["venue"]
        hour = hour_bucket(ts_ns)
        key = (table, symbol, venue, hour)
        with self._lock:
            buf = self._buffers.get(key)
            if buf is None:
                buf = _Buffer(hour=hour)
                self._buffers[key] = buf
            buf.rows.append(row)
            should_flush = (
                len(buf.rows) >= self.batch_rows
                or (ns_now() - buf.opened_at_ns) >= self.batch_seconds * 1_000_000_000
            )
        if should_flush:
            self._flush_key(key)

    def append_many(self, table: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.append(table, row)

    def flush_all(self) -> None:
        with self._lock:
            keys = list(self._buffers.keys())
        for key in keys:
            self._flush_key(key)

    # ----- internals -----

    def _flush_key(self, key: tuple[str, str, str, int]) -> None:
        with self._lock:
            buf = self._buffers.pop(key, None)
        if buf is None or not buf.rows:
            return
        table, symbol, venue, hour = key
        rows = buf.rows
        schema = schema_for(table)
        # Pivot list-of-dicts into column arrays, in schema order, casting as we go.
        columns: dict[str, list[Any]] = {f.name: [] for f in schema}
        for r in rows:
            for f in schema:
                columns[f.name].append(r.get(f.name))
        arrays = [pa.array(columns[f.name], type=f.type) for f in schema]
        table_obj = pa.Table.from_arrays(arrays, schema=schema)
        # Determine the date bucket from the first row (all rows in this hour share a date).
        date = date_str(rows[0]["ts_ns"])
        part_dir = partition_path(table, symbol, venue, date)
        part_dir.mkdir(parents=True, exist_ok=True)
        # Unique part name = hour bucket + monotonic suffix.
        suffix = ns_now()
        part_file = part_dir / f"part-h{hour % 24:02d}-{suffix}.parquet"
        pq.write_table(
            table_obj,
            part_file,
            compression="zstd",
            compression_level=6,
            use_dictionary=["symbol", "venue"],
        )


def write_dataframe(table: str, df_or_rows: list[dict[str, Any]] | pa.Table, *, symbol: str, venue: str, date: str) -> Path:
    """Eager one-shot write — used by backfill and the build_bars_1s script.

    Returns the file path written. Always creates a new part with a monotonic suffix
    so this can be called repeatedly without overwriting.
    """
    schema = schema_for(table)
    if isinstance(df_or_rows, list):
        columns: dict[str, list[Any]] = {f.name: [] for f in schema}
        for r in df_or_rows:
            for f in schema:
                columns[f.name].append(r.get(f.name))
        arrays = [pa.array(columns[f.name], type=f.type) for f in schema]
        tab = pa.Table.from_arrays(arrays, schema=schema)
    else:
        tab = df_or_rows.cast(schema)
    part_dir = partition_path(table, symbol, venue, date)
    part_dir.mkdir(parents=True, exist_ok=True)
    suffix = ns_now()
    part_file = part_dir / f"part-{suffix}.parquet"
    pq.write_table(
        tab,
        part_file,
        compression="zstd",
        compression_level=6,
        use_dictionary=["symbol", "venue"],
    )
    return part_file
