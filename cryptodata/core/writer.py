"""Async-friendly wrapper around PartitionedParquetWriter.

The writer is itself synchronous (uses threading.Lock). The async wrapper
exists so workers can push rows without blocking the event loop on the
small CPU-bound serialization steps. For v1 we just call .append directly;
if profiling shows the writer holding the loop, switch to a thread executor.
"""
from __future__ import annotations

import asyncio
from typing import Any

from cryptodata.paths import load_ingest
from cryptodata.storage.parquet import PartitionedParquetWriter


class AsyncWriter:
    def __init__(self) -> None:
        cfg = load_ingest().get("writer", {})
        self._writer = PartitionedParquetWriter(
            batch_rows=int(cfg.get("batch_rows", 50_000)),
            batch_seconds=int(cfg.get("batch_seconds", 60)),
        )
        self._flush_interval = int(cfg.get("batch_seconds", 60))
        self._stop = asyncio.Event()
        self._flush_task: asyncio.Task | None = None

    def append(self, table: str, row: dict[str, Any]) -> None:
        self._writer.append(table, row)

    async def run(self) -> None:
        """Periodic flusher — call as a long-lived background task."""
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._flush_interval)
                except TimeoutError:
                    self._writer.flush_all()
        finally:
            self._writer.flush_all()

    def stop(self) -> None:
        self._stop.set()
