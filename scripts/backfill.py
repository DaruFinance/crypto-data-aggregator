"""REST-only historical backfill for trades / funding / open_interest / klines.

Examples:
    python -m scripts.backfill --symbol BTC-USD     --venue coinbase        --start 2026-05-10 --end 2026-05-11 --table trades
    python -m scripts.backfill --symbol BTC-USDT-PERP --venue binance_futures --start 2026-04-01 --end 2026-05-01 --table funding
    python -m scripts.backfill --symbol ETH-USDT    --venue binance         --start 2026-04-12 --end 2026-05-12 --table klines --interval 1m

`klines` land in the `bars_ref` table (one parquet partition per symbol/venue/date),
*not* in `bars_1s` — the consolidated tape is only built from raw trades, never from
venues' published candles.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import defaultdict

import pandas as pd

from cryptodata.core.corrections import record_correction
from cryptodata.sources.registry import make_source
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe

log = logging.getLogger("backfill")


def _to_ns(t: str) -> int:
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.value)


async def run_backfill(symbol: str, venue: str, start_ns: int, end_ns: int, table: str, interval: str = "1m") -> int:
    """Backfill one (symbol, venue, table) slice. Returns rows written."""
    init_db()
    source = make_source(venue)
    if table == "trades":
        items = await source.fetch_trades(symbol, start_ns, end_ns)
        rows = [t.to_row() for t in items]
        dest = "trades"
    elif table == "funding":
        items = await source.fetch_funding(symbol, start_ns, end_ns)
        rows = [f.to_row() for f in items]
        dest = "funding"
    elif table == "open_interest":
        # OI history isn't a uniform REST shape across venues, so v1 backfills OI only
        # via the live ingest poller; nothing to do here.
        log.warning("open_interest backfill not implemented; populated by live ingest")
        return 0
    elif table == "klines":
        items = await source.fetch_klines(symbol, start_ns, end_ns, interval=interval)
        rows = [{
            "ts_ns": k["ts_ns"], "symbol": symbol, "venue": venue, "interval": interval,
            "open": k["open"], "high": k["high"], "low": k["low"], "close": k["close"],
            "volume": k["volume"], "trades": int(k.get("trades", 0)), "ingested_at_ns": None,
        } for k in items]
        dest = "bars_ref"
    else:
        log.error("unknown --table value: %s", table)
        return 0

    if not rows:
        log.info("backfill: no rows returned for %s %s %s", symbol, venue, table)
        return 0

    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[date_str(r["ts_ns"])].append(r)
    written = 0
    for date, batch in by_date.items():
        path = write_dataframe(dest, batch, symbol=symbol, venue=venue, date=date)
        written += len(batch)
        log.info("wrote %d rows to %s", len(batch), path)

    record_correction(
        table_name=dest, kind="backfill", severity="info", symbol=symbol, venue=venue,
        effective_from_ns=int(start_ns), effective_to_ns=int(end_ns), rows_affected=written,
        note=f"REST backfill {table}" + (f" interval={interval}" if table == "klines" else ""),
    )
    return written


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True, help="canonical symbol, e.g. BTC-USD")
    p.add_argument("--venue", required=True,
                   help="binance, coinbase, kraken, okx, bitstamp, gemini, bitfinex, binance_futures, bybit")
    p.add_argument("--start", required=True, help="UTC start (ISO, e.g. 2026-05-10 or 2026-05-10T12:00:00)")
    p.add_argument("--end", required=True, help="UTC end")
    p.add_argument("--table", default="trades", choices=["trades", "funding", "open_interest", "klines"])
    p.add_argument("--interval", default="1m", help="kline interval (only for --table klines)")
    args = p.parse_args()
    n = asyncio.run(run_backfill(args.symbol, args.venue, _to_ns(args.start), _to_ns(args.end), args.table, args.interval))
    print(f"backfilled {n} rows: {args.symbol} {args.venue} {args.table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
