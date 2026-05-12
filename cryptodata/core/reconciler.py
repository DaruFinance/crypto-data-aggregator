"""REST-based gap-fill reconciler.

After every WS reconnect or at a fixed hourly cadence, scan the trades table for
gaps (no rows for >2 minutes during what should be active hours) and fetch the
missing range via REST. The reconciler is intentionally conservative: it never
backfills past a venue's REST history window.
"""
from __future__ import annotations

import asyncio
import logging
import time

from cryptodata.core.symbols import symbols_for_venue
from cryptodata.sources.base import Source
from cryptodata.sources.registry import make_source
from cryptodata.storage.duckdb_views import connect
from cryptodata.storage.parquet import date_str, write_dataframe

log = logging.getLogger("cryptodata.reconciler")


async def reconcile_venue_trades(source: Source, lookback_minutes: int = 120) -> int:
    """Fetch trades for the last `lookback_minutes` and write any missing ones.

    Returns number of rows backfilled. v1 is symbol-by-symbol; future versions can
    parallelize within the venue's rate-limit budget.
    """
    backfilled = 0
    now_ns = time.time_ns()
    start_ns = now_ns - lookback_minutes * 60 * 1_000_000_000
    for canonical, _native in symbols_for_venue(source.name):
        # Check what we already have for this (venue, symbol) in the window
        with connect() as con:
            try:
                existing = con.execute(
                    "SELECT MAX(ts_ns) FROM trades WHERE venue = ? AND symbol = ? AND ts_ns >= ?",
                    [source.name, canonical, start_ns],
                ).fetchone()
            except Exception:
                existing = (None,)
        last_seen = existing[0] if existing and existing[0] is not None else start_ns
        if now_ns - last_seen < 60 * 1_000_000_000:
            continue   # nothing meaningfully missing
        try:
            trades = await source.fetch_trades(canonical, last_seen + 1, now_ns)
        except NotImplementedError:
            continue
        except Exception:
            log.exception("reconcile.fetch_trades venue=%s symbol=%s", source.name, canonical)
            continue
        if not trades:
            continue
        # Group by date and write
        by_date: dict[str, list[dict]] = {}
        for t in trades:
            by_date.setdefault(date_str(t.ts_ns), []).append(t.to_row())
        for date, rows in by_date.items():
            write_dataframe("trades", rows, symbol=canonical, venue=source.name, date=date)
            backfilled += len(rows)
        log.info("reconcile venue=%s symbol=%s backfilled=%d", source.name, canonical, len(trades))
    return backfilled


async def run_reconciler_loop(venue_names: list[str], interval_seconds: int = 3600) -> None:
    sources = [make_source(v) for v in venue_names]
    while True:
        for s in sources:
            try:
                await reconcile_venue_trades(s)
            except Exception:
                log.exception("reconciler.loop venue=%s", s.name)
        await asyncio.sleep(interval_seconds)
