"""``cda-status`` — a one-screen "what is this project, and how good is the data" report.

It reads, in order of preference:
  1. the live metadata tables in the DuckDB file (``coverage``, ``venue_status``,
     ``daily_quality``, ``corrections``) — populated by a live ingest and the
     ``cda-coverage`` / ``cda-validate`` jobs;
  2. if those are empty (e.g. a fresh checkout, before anything has been built), the
     committed metadata artifacts under ``data/meta/`` (``coverage.json``,
     ``quality/<date>.json``, ``dataset_manifest.json``) — so a clone immediately shows
     the dataset that ships with the repo.

It prints: a short description of the project; the build manifest headline; dataset
coverage totals per table (partitions / rows / bytes / symbol & venue counts / date
span); the latest live-feed health snapshot if any; the latest data-quality grade per
``(symbol, venue)``; the corrections-log roll-up; and a "next steps" footer with the
commands to run the test suite, build a sample dataset, and query it. Read-only.
"""
from __future__ import annotations

import json
import sys
import time

import pandas as pd

from cryptodata.paths import META_ROOT
from cryptodata.storage.duckdb_views import connect


def _safe_df(con, sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return con.execute(sql, params or []).fetchdf()
    except Exception:
        return pd.DataFrame()


def _meta(name: str):
    """Load a ``data/meta/<name>`` JSON artifact, or ``None`` if it isn't there."""
    p = META_ROOT / name
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _latest_quality_cards() -> list[dict]:
    qdir = META_ROOT / "quality"
    files = sorted(qdir.glob("*.json")) if qdir.exists() else []
    if not files:
        return []
    try:
        return json.loads(files[-1].read_text())
    except Exception:
        return []


def status_report() -> dict:
    """Assemble the status report dict. ``source`` is ``"duckdb"`` if the live metadata
    tables had data, else ``"meta files"`` if the report was built from ``data/meta/``."""
    now_ns = time.time_ns()
    with connect() as con:
        venue_status = _safe_df(
            con,
            "SELECT venue, stream, symbol, up, ts_ns FROM venue_status "
            "WHERE ts_ns = (SELECT MAX(ts_ns) FROM venue_status) ORDER BY venue, stream, symbol",
        )
        coverage_db = _safe_df(
            con,
            'SELECT table_name, COUNT(*) AS partitions, SUM("rows") AS "rows", '
            'SUM(bytes_on_disk) AS bytes, MIN(date) AS first_date, MAX(date) AS last_date, '
            "COUNT(DISTINCT symbol) AS symbols, COUNT(DISTINCT venue) AS venues "
            "FROM coverage GROUP BY table_name ORDER BY table_name",
        )
        quality_db = _safe_df(
            con,
            "SELECT symbol, venue, score, grade, completeness_pct, n_critical, date "
            "FROM daily_quality WHERE (symbol, venue, date) IN "
            "(SELECT symbol, venue, MAX(date) FROM daily_quality GROUP BY symbol, venue) "
            "ORDER BY symbol, venue",
        )
        corrections_db = _safe_df(
            con,
            "SELECT kind, severity, COUNT(*) AS n FROM corrections GROUP BY kind, severity ORDER BY severity DESC, kind",
        )
        bars_span = _safe_df(con, "SELECT MIN(ts_ns) AS lo, MAX(ts_ns) AS hi, COUNT(*) AS n FROM bars_1s")

    manifest = _meta("dataset_manifest.json") or {}
    quality_summary = _meta("quality_summary.json") or {}
    db_had_data = (not coverage_db.empty) or (not quality_db.empty)

    coverage = coverage_db.to_dict("records") if not coverage_db.empty else []
    quality = quality_db.to_dict("records") if not quality_db.empty else []
    corrections = corrections_db.to_dict("records") if not corrections_db.empty else []

    if not coverage:
        by_table = (_meta("coverage.json") or {}).get("summary", {}).get("by_table", [])
        coverage = [{
            "table_name": t.get("table_name"), "partitions": t.get("partitions"), "rows": t.get("rows"),
            "bytes": t.get("bytes"), "first_date": t.get("first_date"), "last_date": t.get("last_date"),
            "symbols": t.get("symbols"), "venues": t.get("venues"),
        } for t in by_table]

    if not quality:
        quality = [{
            "symbol": c["symbol"], "venue": c["venue"], "score": c["score"], "grade": c["grade"],
            "completeness_pct": c.get("completeness_pct", 0.0), "n_critical": c.get("n_critical", 0), "date": c["date"],
        } for c in _latest_quality_cards()]

    if not corrections and manifest.get("corrections_logged"):
        corrections = [{"kind": "(logged during last build)", "severity": "info", "n": manifest["corrections_logged"]}]

    source = "duckdb metadata tables" if db_had_data else ("data/meta/ artifacts (shipped with the repo)" if coverage or quality else "empty — nothing built yet")

    return {
        "generated_at_ns": now_ns,
        "source": source,
        "manifest": manifest,
        "quality_summary": quality_summary,
        "venue_status": venue_status.to_dict("records"),
        "coverage": coverage,
        "quality": quality,
        "corrections": corrections,
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
    p("=" * 78)
    p("crypto-data-aggregator — multi-venue crypto market data: a cross-venue consolidated")
    p("tape (trade-level VWAP, robust outlier filtering, full provenance), perp funding +")
    p("open interest, L2 book snapshots, a data-quality scorecard, and a point-in-time")
    p("pandas-first query API over partitioned Parquet (+ DuckDB views).")
    p("=" * 78)
    p(f"(report source: {rep['source']})")

    m = rep["manifest"]
    if m:
        p(f"\nDataset (built {m.get('built_at', '?')}): {m.get('note', '')}")
        b1 = m.get("bars_1s", {})
        if b1:
            p(f"  1s bars: {b1.get('rows', 0):,} rows over {b1.get('window_utc', ['?', '?'])[0]} .. {b1.get('window_utc', ['?', '?'])[1]}")
            avp = b1.get("agg_tape_contributing_venues_per_symbol", {})
            if avp:
                p(f"  consolidated tape: {', '.join(f'{s}={n}v' for s, n in avp.items())}")
        p(f"  symbol_map: {m.get('symbol_map_mappings', '?')} (canonical, venue) mappings; "
          f"corrections logged: {m.get('corrections_logged', '?')}; total rows: {m.get('total_rows', 0):,}")

    cov = rep["coverage"]
    if cov:
        p("\nCoverage (rows = record counts on disk):")
        p(f"  {'table':<20}{'partitions':>11}{'rows':>16}{'on disk':>14}{'symbols':>9}{'venues':>8}  dates")
        for r in cov:
            p(f"  {str(r['table_name']):<20}{int(r['partitions'] or 0):>11,}{int(r['rows'] or 0):>16,}"
              f"{_fmt_bytes(r['bytes']):>14}{int(r['symbols'] or 0):>9}{int(r['venues'] or 0):>8}  "
              f"{r['first_date']}..{r['last_date']}")
    else:
        p("\nCoverage: (none yet — run `cda-build-dataset` then `cda-coverage`)")

    vs = rep["venue_status"]
    if vs:
        p("\nLive feed health (latest snapshot):")
        for r in vs:
            p(f"  [{'UP ' if r['up'] else 'DOWN'}] {r['venue']:<16} {r['stream']:<18} {r['symbol']}")
    else:
        p("\nLive feed health: (no snapshots — only populated while `cda-ingest` is running)")

    q = rep["quality"]
    if q:
        qs = rep["quality_summary"]
        head = ""
        if qs:
            head = f"  —  {qs.get('slices', len(q))} slices, mean {qs.get('mean_score', 0):.1f}, " \
                   f"{qs.get('total_critical_issues', 0)} critical issue(s); " \
                   f"grades {qs.get('grade_counts', {})}"
        p(f"\nData quality (latest per symbol/venue){head}:")
        p(f"  {'symbol':<14}{'venue':<14}{'grade':>6}{'score':>8}{'complete%':>11}{'crit':>6}  date")
        for r in sorted(q, key=lambda x: (x["symbol"], x["venue"])):
            p(f"  {r['symbol']:<14}{r['venue']:<14}{str(r['grade']):>6}{float(r['score']):>8.1f}"
              f"{float(r['completeness_pct']):>11.2f}{int(r['n_critical']):>6}  {r['date']}")
    else:
        p("\nData quality: (none yet — run `cda-build-dataset` then `cda-validate`)")

    cr = rep["corrections"]
    if cr:
        p("\nCorrections / lineage log:")
        for r in cr:
            p(f"  {str(r['severity']):<9} {str(r['kind']):<28} {int(r['n'])}")

    p("\nNext steps:")
    p("  pytest -q                                         # 48 tests: invariants, tape, quality, parsing, …")
    p("  cda-build-dataset --trades-minutes 5 --reference-days 1   # backfill real data → build → quality → coverage")
    p("  cda-validate                                      # data-quality scorecards (non-zero exit on a failing slice)")
    p('  python -c "from cryptodata import get_bars; print(get_bars(\'BTC-USD\',\'2026-01-01\',\'2030-01-01\',\'1m\').tail())"')
    p("  see README.md and docs/{METHODOLOGY,ARCHITECTURE,DATA_DICTIONARY,OPERATIONS,SLA,BENCHMARKS}.md")
    p("")
