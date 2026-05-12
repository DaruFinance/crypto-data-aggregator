"""Per-(symbol, venue, date) data-quality scorecard.

The score answers: *"how well did we do our job on this slice?"* — **not** "how active
was the market", which is a property of the instrument, not of the data.

    base   = 100                            for a single-venue series — it captures a
                                            bar for every second the venue actually
                                            traded; we can't do better than the venue's
                                            own activity, so a thin venue is not a defect.
           = agg_coverage_of_union_pct       for the consolidated ``agg`` series — what
                                            fraction of the seconds where *any*
                                            contributing spot venue traded does the
                                            consolidated tape also cover. A correct tape
                                            captures ~100% of that union (the only
                                            "misses" are seconds where every trade was a
                                            filtered outlier, which is correct to drop).
                                            This is the metric that actually grades the
                                            consolidation; it does **not** punish the
                                            tape for the USD market being thinner than
                                            the USDT market over a short window.
    score  = base − Σ issue_weights, clamped to [0, 100]
             weights: info 0 · minor 2 · major 10 · critical 30, per distinct issue.

The raw second-by-second *density* of each series is still reported as
``completeness_pct`` (and ``day_coverage_pct``) — it's an informational metric, not a
penalty. So a thin pair can read e.g. "grade A · density 7%": the tape/feed is correct,
the market is just quiet.

Grades: A ≥ 95, B ≥ 85, C ≥ 70, D ≥ 50, F < 50.

Scores, grades and the issue list are persisted to the ``daily_quality`` DuckDB
table and mirrored to ``data/meta/quality/<date>.json`` so the report is queryable
*and* diffable in git.
"""
from __future__ import annotations

import json
import time
from collections import Counter

import numpy as np
import pandas as pd

from cryptodata.paths import META_ROOT, ensure_dirs
from cryptodata.quality.checks import Issue, Severity, run_checks
from cryptodata.storage.duckdb_views import connect

_ISSUE_WEIGHT = {Severity.INFO: 0.0, Severity.MINOR: 2.0, Severity.MAJOR: 10.0, Severity.CRITICAL: 30.0}
_FULL_DAY_SECONDS = 86_400


def grade_for(score: float) -> str:
    if score >= 95:
        return "A"
    if score >= 85:
        return "B"
    if score >= 70:
        return "C"
    if score >= 50:
        return "D"
    return "F"


# --------------------------------------------------------------------------- #
# loaders
# --------------------------------------------------------------------------- #

def _bars(con, symbol: str, venue: str, date: str) -> pd.DataFrame:
    try:
        return con.execute(
            "SELECT ts_ns, open, high, low, close, volume, vwap, trades, sources_mask "
            "FROM bars_1s WHERE symbol = ? AND venue = ? AND date = ? ORDER BY ts_ns",
            [symbol, venue, date],
        ).fetchdf()
    except Exception:
        return pd.DataFrame()


def _trades(con, symbol: str, venue: str, date: str) -> pd.DataFrame:
    try:
        return con.execute(
            "SELECT ts_ns, recv_ns, price, size, side, trade_id "
            "FROM trades WHERE symbol = ? AND venue = ? AND date = ? ORDER BY ts_ns",
            [symbol, venue, date],
        ).fetchdf()
    except Exception:
        return pd.DataFrame()


def _quotes(con, symbol: str, venue: str, date: str) -> pd.DataFrame:
    try:
        return con.execute(
            "SELECT ts_ns, bid_px, ask_px, bid_sz, ask_sz "
            "FROM quotes WHERE symbol = ? AND venue = ? AND date = ? ORDER BY ts_ns",
            [symbol, venue, date],
        ).fetchdf()
    except Exception:
        return pd.DataFrame()


def _venues_for(con, table: str, symbol: str, date: str) -> list[str]:
    try:
        rows = con.execute(
            f"SELECT DISTINCT venue FROM {table} WHERE symbol = ? AND date = ?", [symbol, date]
        ).fetchall()
        return sorted(r[0] for r in rows)
    except Exception:
        return []


# --------------------------------------------------------------------------- #
# scoring
# --------------------------------------------------------------------------- #

def _completeness(bars: pd.DataFrame) -> tuple[float, int, int, int, int]:
    """Returns (completeness_pct, n_bars, expected_bars, max_gap_seconds, span_seconds)."""
    if bars.empty:
        return 0.0, 0, 0, 0, 0
    ts = bars["ts_ns"].to_numpy()
    n = len(ts)
    span_s = int((ts.max() - ts.min()) // 1_000_000_000) + 1
    expected = span_s
    completeness = 100.0 * min(1.0, n / expected) if expected else 0.0
    gaps_s = np.diff(np.sort(ts)) / 1e9
    max_gap = int(gaps_s.max()) if gaps_s.size else 0
    return round(completeness, 4), int(n), int(expected), max_gap, span_s


def _agg_coverage_of_union(agg_bars: pd.DataFrame, per_venue_bars: dict[str, pd.DataFrame]) -> tuple[float | None, int]:
    """For the ``agg`` series: what fraction of the seconds where *any* contributing
    venue had a bar does the agg also have a bar? Returns ``(pct, n_union_seconds)``.

    ``None`` if there's no per-venue context to compare against (then the caller falls
    back to a clean baseline — the consolidated tape can't be held to a standard we
    can't measure).
    """
    union: set[int] = set()
    for df in per_venue_bars.values():
        if not df.empty:
            union |= set((df["ts_ns"].to_numpy() // 1_000_000_000).tolist())
    if not union:
        return None, 0
    agg_secs: set[int] = set()
    if not agg_bars.empty:
        agg_secs = set((agg_bars["ts_ns"].to_numpy() // 1_000_000_000).tolist())
    return round(100.0 * len(agg_secs & union) / len(union), 2), len(union)


def _score(base: float, issues: list[Issue]) -> tuple[float, str]:
    score = base
    for iss in issues:
        score -= _ISSUE_WEIGHT.get(iss.severity, 0.0)
    score = max(0.0, min(100.0, score))
    return round(score, 2), grade_for(score)


def score_day(symbol: str, venue: str, date) -> dict:
    """Compute (don't persist) the scorecard dict for one (symbol, venue, date)."""
    date = str(date)
    with connect() as con:
        bars = _bars(con, symbol, venue, date)
        trades = _trades(con, symbol, venue, date)
        quotes = _quotes(con, symbol, venue, date)
        per_venue_bars: dict[str, pd.DataFrame] = {}
        agg_bars = pd.DataFrame()
        if venue == "agg":
            for v in _venues_for(con, "bars_1s", symbol, date):
                if v == "agg":
                    agg_bars = bars
                else:
                    per_venue_bars[v] = _bars(con, symbol, v, date)

    issues = run_checks(
        bars=bars, trades=trades, quotes=quotes,
        agg_bars=agg_bars if venue == "agg" else None,
        per_venue_bars=per_venue_bars if venue == "agg" else None,
    )
    completeness, n_bars, expected, max_gap, span_s = _completeness(bars)   # raw density (informational)

    # Score base: a single venue captures all of its own activity → 100. The agg tape is
    # graded on how much of the union of contributing venues it captures (≈100 when
    # correct) — never on how thin the underlying market happens to be.
    agg_cov_pct: float | None = None
    union_secs = 0
    if venue == "agg":
        agg_cov_pct, union_secs = _agg_coverage_of_union(agg_bars if not agg_bars.empty else bars, per_venue_bars)
        base = agg_cov_pct if agg_cov_pct is not None else 100.0
        score_basis = "coverage_of_contributing_venues"
    else:
        base = 100.0
        score_basis = "clean_baseline"

    score, grade = _score(base, issues)
    sev_counts = Counter(i.severity.label for i in issues)
    return {
        "date": date,
        "symbol": symbol,
        "venue": venue,
        "score": score,
        "grade": grade,
        "score_basis": score_basis,
        "score_base_pct": round(base, 2),
        "agg_coverage_of_union_pct": agg_cov_pct,   # null for single-venue series
        "union_seconds": int(union_secs) if venue == "agg" else None,
        "bars": n_bars,
        "expected_bars": expected,
        "completeness_pct": completeness,           # raw bar density over [first, last]
        "max_gap_seconds": max_gap,
        "span_seconds": span_s,
        "day_coverage_pct": round(100.0 * span_s / _FULL_DAY_SECONDS, 2) if span_s else 0.0,
        "n_trades": int(len(trades)),
        "n_quotes": int(len(quotes)),
        "n_issues": len(issues),
        "n_critical": int(sev_counts.get("critical", 0)),
        "severity_counts": dict(sev_counts),
        "issues": [i.to_dict() for i in issues],
        "computed_at_ns": time.time_ns(),
    }


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

def write_daily_quality(symbols: list[str] | None = None, dates: list[str] | None = None) -> pd.DataFrame:
    """Compute and persist scorecards for every (symbol, venue, date) that has bars.

    If ``symbols``/``dates`` are omitted, every (symbol, date) present in ``bars_1s``
    is scored. Returns the scorecard DataFrame.
    """
    ensure_dirs()
    with connect() as con:
        try:
            combos = con.execute(
                "SELECT DISTINCT symbol, venue, date FROM bars_1s ORDER BY date, symbol, venue"
            ).fetchall()
        except Exception:
            combos = []
    # DuckDB infers the hive `date=YYYY-MM-DD` partition column as a DATE; coerce to str
    # so scorecards stay JSON-clean and the VARCHAR `date` columns get strings.
    combos = [(s, v, str(d)) for s, v, d in combos]
    if symbols:
        combos = [c for c in combos if c[0] in set(symbols)]
    if dates:
        combos = [c for c in combos if c[2] in set(dates)]

    rows = []
    for symbol, venue, date in combos:
        card = score_day(symbol, venue, date)
        rows.append(card)

    if not rows:
        return pd.DataFrame()

    # persist to DuckDB
    with connect() as con:
        con.executemany(
            """INSERT OR REPLACE INTO daily_quality
               (date, symbol, venue, score, grade, score_base_pct, bars, expected_bars,
                completeness_pct, max_gap_seconds, n_issues, n_critical, issues_json, computed_at_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(r["date"], r["symbol"], r["venue"], r["score"], r["grade"], r["score_base_pct"], r["bars"],
              r["expected_bars"], r["completeness_pct"], r["max_gap_seconds"], r["n_issues"],
              r["n_critical"], json.dumps(r["issues"]), r["computed_at_ns"]) for r in rows],
        )

    # mirror to JSON, one file per date
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    qdir = META_ROOT / "quality"
    qdir.mkdir(parents=True, exist_ok=True)
    for date, cards in by_date.items():
        (qdir / f"{date}.json").write_text(json.dumps(cards, indent=2, sort_keys=True, default=str))
    # and a rolled-up index
    summary = quality_summary_from_rows(rows)
    (META_ROOT / "quality_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str))

    return pd.DataFrame([{k: v for k, v in r.items() if k not in ("issues", "severity_counts")} for r in rows])


def quality_summary_from_rows(rows: list[dict]) -> dict:
    if not rows:
        return {"slices": 0}
    scores = [r["score"] for r in rows]
    grades = Counter(r["grade"] for r in rows)
    crit = sum(r["n_critical"] for r in rows)
    return {
        "slices": len(rows),
        "symbols": sorted({r["symbol"] for r in rows}),
        "venues": sorted({r["venue"] for r in rows}),
        "dates": sorted({r["date"] for r in rows}),
        "mean_score": round(sum(scores) / len(scores), 2),
        "min_score": round(min(scores), 2),
        "grade_counts": dict(grades),
        "total_critical_issues": int(crit),
        "computed_at_ns": time.time_ns(),
    }


def quality_summary() -> dict:
    """Return the rolled-up quality summary from the persisted table."""
    with connect() as con:
        try:
            df = con.execute("SELECT * FROM daily_quality").fetchdf()
        except Exception:
            return {"slices": 0}
    if df.empty:
        return {"slices": 0}
    rows = df.to_dict("records")
    for r in rows:  # adapt column names
        r["n_critical"] = int(r.get("n_critical", 0))
    return quality_summary_from_rows(rows)
