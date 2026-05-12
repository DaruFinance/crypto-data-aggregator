"""Orchestrator — spawns one IngestWorker per (venue, stream) per ingest config.

Usage from a script:

    asyncio.run(run_ingest_forever())
"""
from __future__ import annotations

import asyncio
import logging

from cryptodata.core.corrections import seed_symbol_map
from cryptodata.core.health_writer import run_health_writer
from cryptodata.core.reconciler import run_reconciler_loop
from cryptodata.core.symbols import symbols_for_venue
from cryptodata.core.worker import IngestWorker
from cryptodata.core.writer import AsyncWriter
from cryptodata.obs.metrics import start_exporter
from cryptodata.paths import load_ingest
from cryptodata.sources.registry import make_source
from cryptodata.storage.duckdb_views import init_db

log = logging.getLogger("cryptodata.ingest")


def _resolve_symbols_for(venue: str, scope) -> list[str]:
    """`scope` may be: True (all symbols this venue carries), a list of canonical
    symbols, or False/None (no symbols)."""
    if scope is True:
        return [c for c, _ in symbols_for_venue(venue)]
    if isinstance(scope, list):
        venue_syms = {c for c, _ in symbols_for_venue(venue)}
        return [s for s in scope if s in venue_syms]
    return []


async def run_ingest_forever() -> None:
    init_db()   # ensure tables / views exist
    seed_symbol_map()   # keep point-in-time reference data current with config
    cfg = load_ingest()
    streams_cfg = cfg.get("streams", {})
    book_cfg = cfg.get("book", {})
    book_depth = int(book_cfg.get("levels", 20))
    start_exporter(int(cfg.get("metrics", {}).get("port", 9464)))

    writer = AsyncWriter()
    tasks: list[asyncio.Task] = [asyncio.create_task(writer.run(), name="writer")]

    for venue, streams in streams_cfg.items():
        try:
            source = make_source(venue)
        except KeyError:
            log.warning("ingest.skip_unknown_venue venue=%s", venue)
            continue
        for stream, scope in streams.items():
            symbols = _resolve_symbols_for(venue, scope)
            if not symbols:
                continue
            kwargs: dict = {}
            if stream == "book_l2_snapshot":
                kwargs["depth"] = book_depth
            worker = IngestWorker(source, stream, symbols, writer, **kwargs)
            tasks.append(asyncio.create_task(worker.run(), name=f"{venue}.{stream}"))

    # Background services
    tasks.append(asyncio.create_task(run_health_writer(), name="health_writer"))
    tasks.append(asyncio.create_task(run_reconciler_loop(list(streams_cfg)), name="reconciler"))

    log.info("ingest.started workers=%d", len(tasks))
    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.info("ingest.shutdown")
    finally:
        writer.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
