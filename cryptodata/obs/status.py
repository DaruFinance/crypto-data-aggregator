"""`cda-status` — a one-screen health/coverage/quality summary for ops.

Reads the metadata tables (``venue_status``, ``coverage``, ``daily_quality``,
``corrections``) and the parquet tree, and prints:
  - per-venue last-seen / up-down,
  - dataset coverage totals (rows, bytes, date span) per table,
  - latest data-quality grade per (symbol, venue),
  - count of open / recent corrections.
"""
from __future__ import annotations

import sys
import time

import pandas as pd

from cryptodata.storage.duckdb_views import connect


def _safe_df(con, sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return con.execute(sql, params or []).fetchdf()
    except Exception:
        return pd.DataFrame()


def status_report() -> dict:
    now_ns = time.time_ns()
    with connect() as con:
        venue_status = _safe_df(
            con,
            "SELECT venue, stream, symbol, up, ts_ns FROM venue_status "
            "WHERE ts_ns = (SELECT MAX(ts_ns) FROM venue_status) ORDER BY venue, stream, symbol",
        )
        coverage = _safe_df(
            con,
            "SELECT table_name, COUNT(*) AS partitions, SUM(rows) AS rows, "
            "SUM(bytes_on_disk) AS bytes, MIN(date) AS first_date, MAX(date) AS last_date, "
            "COUNT(DISTINCT symbol) AS symbols, COUNT(DISTINCT venue) AS venues "
            "FROM coverage GROUP BY table_name ORDER BY table_name",
        )
        quality = _safe_df(
            con,
            "SELECT symbol, venue, score, grade, completeness_pct, n_critical, date "
            "FROM daily_quality WHERE (symbol, venue, date) IN "
            "(SELECT symbol, venue, MAX(date) FROM daily_quality GROUP BY symbol, venue) "
            "ORDER BY symbol, venue",
        )
        corrections = _safe_df(
            con,
            "SELECT kind, severity, COUNT(*) AS n FROM corrections GROUP BY kind, severity ORDER BY severity DESC, kind",
        )
        bars_span = _safe_df(con, "SELECT MIN(ts_ns) AS lo, MAX(ts_ns) AS hi, COUNT(*) AS n FROM bars_1s")
    return {
        "generated_at_ns": now_ns,
        "venue_status": venue_status.to_dict("records"),
        "coverage": coverage.to_dict("records"),
        "quality": quality.to_dict("records"),
        "corrections": corrections.to_dict("records"),
        "bars_span": bars_span.to_dict("records")[0] if not bars_span.empty else {},
    }


def _fmt_bytes(n) -> str:
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def print_status(file=sys.stdout) -> None:
    rep = status_report()
    p = lambda *a: print(*a, file=file)  # noqa: E731
    p("=" * 72)
    p("crypto-data-aggregator — status")
    p("=" * 72)

    cov = rep["coverage"]
    if cov:
        p("\nCoverage (from the `coverage` metadata table):")
        p(f"  {'table':<20}{'partitions':>11}{'rows':>16}{'on disk':>14}{'symbols':>9}{'venues':>8}  dates")
        for r in cov:
            p(f"  {r['table_name']:<20}{int(r['partitions'] or 0):>11,}{int(r['rows'] or 0):>16,}"
              f"{_fmt_bytes(r['bytes']):>14}{int(r['symbols'] or 0):>9}{int(r['venues'] or 0):>8}  "
              f"{r['first_date']}..{r['last_date']}")
    else:
        p("\nCoverage: (none — run `cda-coverage` after a build)")

    vs = rep["venue_status"]
    if vs:
        p("\nVenue feeds (latest health snapshot):")
        for r in vs:
            flag = "UP " if r["up"] else "DOWN"
            p(f"  [{flag}] {r['venue']:<16} {r['stream']:<18} {r['symbol']}")
    else:
        p("\nVenue feeds: (no health snapshots — only relevant during live ingest)")

    q = rep["quality"]
    if q:
        p("\nData quality (latest per symbol/venue):")
        p(f"  {'symbol':<14}{'venue':<14}{'grade':>6}{'score':>8}{'complete%':>11}{'crit':>6}  date")
        for r in q:
            p(f"  {r['symbol']:<14}{r['venue']:<14}{r['grade']:>6}{r['score']:>8.1f}"
              f"{r['completeness_pct']:>11.2f}{int(r['n_critical']):>6}  {r['date']}")
    else:
        p("\nData quality: (none — run `cda-validate` after a build)")

    cr = rep["corrections"]
    if cr:
        p("\nCorrections log:")
        for r in cr:
            p(f"  {r['severity']:<9} {r['kind']:<14} {int(r['n'])}")
    p("")
