"""Nightly part-file compaction.

Merges the hourly Parquet part files written by the live ingester into a single file
per ``(table, symbol, venue, date)`` partition, so a day that accumulated 24 small
parts ends up as one well-sized file (better scan throughput, fewer file handles).
Idempotent: a partition that's already a single file is left alone.

Schedule it once a day (cron / systemd timer / Task Scheduler) at a quiet hour, e.g.
~03:00 UTC, after the day has rolled over. Exposed as the ``cda-compact`` console script.
"""
from __future__ import annotations

import logging
import sys

from cryptodata.storage.compact import compact_all


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    total = compact_all()
    logging.info("compaction.done merged_files=%d", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
