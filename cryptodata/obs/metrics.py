"""Prometheus metrics exporter.

``prometheus_client`` is an *optional* dependency (``pip install -e ".[obs]"``). If
it isn't installed, every function here degrades to a no-op so importing this module
never breaks ingest. When it is installed, :func:`start_exporter` opens an HTTP
endpoint and :func:`record_*` helpers update counters/gauges that the ingest workers
and health writer call into.
"""
from __future__ import annotations

import logging

log = logging.getLogger("cryptodata.metrics")

try:  # pragma: no cover - optional dep
    from prometheus_client import Counter, Gauge, start_http_server
    _HAVE_PROM = True
except Exception:  # pragma: no cover
    _HAVE_PROM = False


if _HAVE_PROM:
    ROWS_INGESTED = Counter("cryptodata_rows_ingested_total", "Rows ingested", ["venue", "stream", "symbol"])
    WORKER_RESTARTS = Counter("cryptodata_worker_restarts_total", "Worker restarts", ["venue", "stream"])
    PARQUET_FILES_WRITTEN = Counter("cryptodata_parquet_files_written_total", "Parquet part files written", ["table"])
    VENUE_UP = Gauge("cryptodata_venue_up", "1 if a (venue, stream, symbol) feed is healthy", ["venue", "stream", "symbol"])
    RECV_LAG_MS = Gauge("cryptodata_recv_lag_ms", "Most recent receive lag (ms)", ["venue", "stream", "symbol"])
    DATASET_QUALITY_SCORE = Gauge("cryptodata_quality_score", "Latest data-quality score", ["symbol", "venue"])


def start_exporter(port: int = 9464) -> bool:
    """Start the HTTP metrics endpoint. Returns True if it actually started."""
    if not _HAVE_PROM:
        log.info("metrics.disabled prometheus_client not installed; install extra 'obs' to enable")
        return False
    start_http_server(port)
    log.info("metrics.started port=%d path=/metrics", port)
    return True


def record_row(venue: str, stream: str, symbol: str) -> None:
    if _HAVE_PROM:
        ROWS_INGESTED.labels(venue, stream, symbol).inc()


def record_restart(venue: str, stream: str) -> None:
    if _HAVE_PROM:
        WORKER_RESTARTS.labels(venue, stream).inc()


def record_parquet_file(table: str) -> None:
    if _HAVE_PROM:
        PARQUET_FILES_WRITTEN.labels(table).inc()


def set_venue_up(venue: str, stream: str, symbol: str, up: bool, recv_lag_ms: float | None = None) -> None:
    if _HAVE_PROM:
        VENUE_UP.labels(venue, stream, symbol).set(1.0 if up else 0.0)
        if recv_lag_ms is not None:
            RECV_LAG_MS.labels(venue, stream, symbol).set(recv_lag_ms)


def set_quality_score(symbol: str, venue: str, score: float) -> None:
    if _HAVE_PROM:
        DATASET_QUALITY_SCORE.labels(symbol, venue).set(score)
