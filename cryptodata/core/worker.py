"""IngestWorker — one supervised task per (venue, stream).

Each worker subscribes to one stream type on one venue (with N symbols multiplexed),
normalizes incoming rows, hands them to the writer, and updates the health tracker.

The worker *supervises itself*: if the underlying stream raises, the worker logs,
backs off, and reconnects. It only exits on cancellation or on a permanent
``NotImplementedError`` (the venue doesn't support that stream). This is the fix
for the original v0 bug where a single transient exception killed a venue feed for
the lifetime of the process.

Workers don't manage rate limits — adapters handle reconnection internally for WS;
rate limits only apply to REST (backfill/reconcile).
"""
from __future__ import annotations

import asyncio
import logging
import random

from cryptodata.core.health import TRACKER
from cryptodata.core.writer import AsyncWriter
from cryptodata.obs import metrics
from cryptodata.sources.base import Source

log = logging.getLogger("cryptodata.worker")


_STREAM_TO_TABLE = {
    "trades": "trades",
    "quotes": "quotes",
    "book_l2_snapshot": "book_l2_snapshot",
    "funding": "funding",
    "open_interest": "open_interest",
}

# Restart backoff bounds (seconds). Jittered to avoid synchronized reconnect storms.
_MIN_BACKOFF = 1.0
_MAX_BACKOFF = 60.0


class IngestWorker:
    def __init__(self, source: Source, stream: str, symbols: list[str], writer: AsyncWriter, **kwargs) -> None:
        self.source = source
        self.stream = stream
        self.symbols = symbols
        self.writer = writer
        self.kwargs = kwargs
        self.restarts = 0

    async def run(self) -> None:
        """Run forever, restarting the underlying stream on any error.

        Returns only on cancellation or when the venue permanently lacks the stream.
        """
        table = _STREAM_TO_TABLE[self.stream]
        log.info("worker.start venue=%s stream=%s symbols=%d", self.source.name, self.stream, len(self.symbols))
        backoff = _MIN_BACKOFF
        while True:
            try:
                async for item in self._make_stream():
                    row = item.to_row()
                    self.writer.append(table, row)
                    TRACKER.record(self.source.name, row["symbol"], self.stream)
                # A clean stream exhaustion is unexpected for a live feed — treat as a restartable event.
                log.warning("worker.stream_ended venue=%s stream=%s — restarting", self.source.name, self.stream)
            except asyncio.CancelledError:
                log.info("worker.cancel venue=%s stream=%s", self.source.name, self.stream)
                raise
            except NotImplementedError:
                log.info("worker.unsupported venue=%s stream=%s — worker exits", self.source.name, self.stream)
                return
            except Exception:
                log.exception("worker.crash venue=%s stream=%s restarts=%d", self.source.name, self.stream, self.restarts)
            # Backoff before restart, with jitter.
            self.restarts += 1
            metrics.record_restart(self.source.name, self.stream)
            sleep_s = min(backoff, _MAX_BACKOFF) * (0.5 + random.random())
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2, _MAX_BACKOFF)

    def _make_stream(self):
        if self.stream == "trades":
            return self.source.stream_trades(self.symbols)
        if self.stream == "quotes":
            return self.source.stream_quotes(self.symbols)
        if self.stream == "book_l2_snapshot":
            depth = self.kwargs.get("depth", 20)
            return self.source.stream_book(self.symbols, depth)
        if self.stream == "funding":
            return self.source.stream_funding(self.symbols)
        if self.stream == "open_interest":
            return self.source.stream_open_interest(self.symbols)
        raise ValueError(f"unknown stream: {self.stream}")
