"""Quick smoke test: ingest a few minutes of BTC-USDT trades from Binance,
build 1s bars, query via get_bars at 1m, and print summary stats.

Usage:
    python -m scripts.smoke_test --seconds 60
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
import time

from cryptodata.core.aggregate import bars_from_trades
from cryptodata.sources.binance import BinanceSpot
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe


async def _ingest_once(seconds: int) -> int:
    init_db()
    source = BinanceSpot()
    rows: list[dict] = []

    async def collect():
        async for trade in source.stream_trades(["BTC-USDT"]):
            rows.append(trade.to_row())

    task = asyncio.create_task(collect())
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.shield(asyncio.sleep(seconds)), timeout=seconds + 1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task

    if not rows:
        return 0

    # Write trades grouped by date
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(date_str(r["ts_ns"]), []).append(r)
    for date, batch in by_date.items():
        write_dataframe("trades", batch, symbol="BTC-USDT", venue="binance", date=date)

    # Build per-venue bars
    bars = bars_from_trades(rows, symbol="BTC-USDT", venue="binance")
    if bars:
        bar_by_date: dict[str, list[dict]] = {}
        for b in bars:
            bar_by_date.setdefault(date_str(b["ts_ns"]), []).append(b)
        for date, batch in bar_by_date.items():
            write_dataframe("bars_1s", batch, symbol="BTC-USDT", venue="binance", date=date)
    return len(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=60)
    args = p.parse_args()
    t0 = time.time()
    n = asyncio.run(_ingest_once(args.seconds))
    elapsed = time.time() - t0
    print(f"smoke_test: ingested {n} trades in {elapsed:.1f}s ({n / max(elapsed, 1e-9):.1f}/s)")

    if n == 0:
        return 1

    # Query the freshly-written data
    from cryptodata import get_bars, get_trades
    end = time.time_ns()
    start = end - args.seconds * 2 * 1_000_000_000

    bars = get_bars("BTC-USDT", start, end, "1m", sources=["binance"])
    trades = get_trades("BTC-USDT", start, end, venues=["binance"])
    print("\n1m bars (binance):")
    print(bars.tail(5))
    print(f"\ntrades returned: {len(trades)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
