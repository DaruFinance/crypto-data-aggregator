"""Periodic dumper from in-memory HealthTracker into the venue_status DuckDB table."""
from __future__ import annotations

import asyncio
import time

from cryptodata.core.health import TRACKER
from cryptodata.obs import metrics
from cryptodata.paths import load_ingest
from cryptodata.storage.duckdb_views import connect


async def run_health_writer() -> None:
    cfg = load_ingest().get("health", {})
    interval = int(cfg.get("check_interval_seconds", 60))
    recent = int(cfg.get("trade_rate_window_seconds", 300))
    baseline = int(cfg.get("trade_rate_baseline_seconds", 3600))
    threshold = float(cfg.get("down_threshold_ratio", 0.10))
    while True:
        await asyncio.sleep(interval)
        report = TRACKER.evaluate(
            recent_window_s=recent, baseline_window_s=baseline, threshold_ratio=threshold,
        )
        ts_ns = time.time_ns()
        rows = []
        for (venue, symbol, stream), s in report.items():
            rows.append((ts_ns, venue, symbol, stream, bool(s["up"]), s["recent_per_s"], None))
            metrics.set_venue_up(venue, stream, symbol, bool(s["up"]))
        if not rows:
            continue
        with connect() as con:
            con.executemany(
                "INSERT OR REPLACE INTO venue_status (ts_ns, venue, symbol, stream, up, coverage_pct, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
