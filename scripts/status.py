"""``cda-status`` — one-screen operational dashboard.

Reads the metadata tables (``coverage``, ``venue_status``, ``daily_quality``,
``corrections``) plus the Parquet tree and prints: dataset coverage totals per table
(partitions, rows, bytes, symbol/venue counts, date span); the latest live-feed health
snapshot per ``(venue, stream, symbol)``; the most recent data-quality grade per
``(symbol, venue)``; and a roll-up of the corrections log. Use ``--json`` for the raw
report (handy for piping into other tooling or alerting). Read-only — safe to run any
time, including against a live ingest.
"""
from __future__ import annotations

import argparse
import json
import sys

from cryptodata.obs.status import print_status, status_report


def main() -> int:
    p = argparse.ArgumentParser(description="Print the crypto-data-aggregator ops dashboard.")
    p.add_argument("--json", action="store_true", help="emit the raw status report as JSON instead of the table view")
    args = p.parse_args()
    if args.json:
        print(json.dumps(status_report(), indent=2, default=str))
    else:
        print_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
