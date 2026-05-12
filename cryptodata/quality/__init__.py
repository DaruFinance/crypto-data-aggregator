"""Data-quality layer: invariant checks, gap detection, cross-venue consistency,
and a per-(symbol, venue, date) scorecard.

Public surface:
    from cryptodata.quality import run_checks, score_day, write_daily_quality
"""
from cryptodata.quality.checks import Issue, Severity, run_checks
from cryptodata.quality.report import (
    grade_for,
    quality_summary,
    score_day,
    write_daily_quality,
)

__all__ = [
    "Issue",
    "Severity",
    "run_checks",
    "grade_for",
    "score_day",
    "write_daily_quality",
    "quality_summary",
]
