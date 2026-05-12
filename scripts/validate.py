"""Run the data-quality suite over the dataset and persist scorecards.

Computes a per-(symbol, venue, date) scorecard via `cryptodata.quality`, writes it to
the `daily_quality` DuckDB table and `data/meta/quality/<date>.json`, prints a summary
table, and exits non-zero if any slice has a CRITICAL issue (so CI can gate on it).

Usage:
    python -m scripts.validate                       # all slices in bars_1s
    python -m scripts.validate --symbol BTC-USD      # restrict
    python -m scripts.validate --fail-on minor       # stricter gate
"""
from __future__ import annotations

import argparse
import logging
import sys

from cryptodata.quality import write_daily_quality

log = logging.getLogger("validate")
_FAIL_ORDER = {"info": 0, "minor": 1, "major": 2, "critical": 3}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", action="append", help="restrict to symbol(s); repeatable")
    p.add_argument("--date", action="append", help="restrict to date(s) YYYY-MM-DD; repeatable")
    p.add_argument("--fail-on", default="critical", choices=list(_FAIL_ORDER), help="lowest severity that fails the run")
    args = p.parse_args()

    df = write_daily_quality(symbols=args.symbol, dates=args.date)
    if df.empty:
        print("validate: no bars_1s data to score (build the dataset first).")
        return 0

    df = df.sort_values(["score", "symbol", "venue", "date"])
    print(f"\n{'symbol':<14}{'venue':<14}{'date':<12}{'grade':>6}{'score':>8}{'complete%':>11}{'issues':>8}{'crit':>6}")
    print("-" * 79)
    for r in df.itertuples(index=False):
        print(f"{r.symbol:<14}{r.venue:<14}{r.date:<12}{r.grade:>6}{r.score:>8.1f}"
              f"{r.completeness_pct:>11.2f}{int(r.n_issues):>8}{int(r.n_critical):>6}")
    print("-" * 79)
    print(f"slices={len(df)}  mean_score={df['score'].mean():.2f}  min_score={df['score'].min():.2f}  "
          f"critical_slices={(df['n_critical'] > 0).sum()}")

    threshold = _FAIL_ORDER[args.fail_on]
    # We persisted issues_json per slice; re-derive worst severity per slice from n_critical
    # for the gate (critical is the only one tracked as a count column; for stricter gates
    # we re-read the issue list).
    if threshold <= _FAIL_ORDER["critical"] and (df["n_critical"] > 0).any():
        bad = df[df["n_critical"] > 0][["symbol", "venue", "date"]].to_dict("records")
        print(f"\nFAIL: {len(bad)} slice(s) have CRITICAL data-quality issues: {bad}")
        return 1
    if threshold < _FAIL_ORDER["critical"]:
        import json

        from cryptodata.storage.duckdb_views import connect
        with connect() as con:
            rows = con.execute("SELECT symbol, venue, date, issues_json FROM daily_quality").fetchall()
        offenders = []
        for sym, ven, date, issues_json in rows:
            for iss in json.loads(issues_json or "[]"):
                if _FAIL_ORDER.get(iss["severity"], 0) >= threshold:
                    offenders.append({"symbol": sym, "venue": ven, "date": date, "check": iss["check"], "severity": iss["severity"]})
        if offenders:
            print(f"\nFAIL: {len(offenders)} issue(s) at or above severity '{args.fail_on}':")
            for o in offenders[:25]:
                print(f"  {o['symbol']} {o['venue']} {o['date']}  {o['check']} [{o['severity']}]")
            return 1
    print("\nOK: no issues at or above the failure threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
