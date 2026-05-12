"""Build 1-second bars from raw trades, including the cross-venue 'agg' consolidated tape.

For each (symbol, date) requested:
  1. Read all trades from `data/raw/trades/symbol=<S>/venue=*/date=<D>/*.parquet`
  2. Group by venue, run `bars_from_trades` to produce per-venue 1s bars
  3. k-way merge the *spot* venue trade streams into one consolidated stream, then
     roll it into the cross-venue 'agg' 1s bars (robust per-trade outlier filter +
     provenance bitmask) via `cryptodata.core.consolidated.build_agg_bars`
  4. Write per-venue bars and 'agg' bars to `data/derived/bars_1s/...`, and record a
     `backfill` correction noting the build.

Usage:
    python -m scripts.build_bars_1s --symbol BTC-USD --date 2026-05-12
    python -m scripts.build_bars_1s --all-present        # every (symbol, date) with raw trades
"""
from __future__ import annotations

import argparse
import logging
import sys

import pyarrow.parquet as pq

from cryptodata.core.aggregate import bars_from_trades
from cryptodata.core.consolidated import build_agg_bars, merge_trades
from cryptodata.core.corrections import record_correction
from cryptodata.core.symbols import venue_bitmask_index
from cryptodata.paths import RAW_ROOT, load_ingest
from cryptodata.sources.registry import SPOT_VENUES, all_sources
from cryptodata.storage.parquet import write_dataframe

log = logging.getLogger("build_bars_1s")


def _collect_trades(symbol: str, date: str) -> dict[str, list[dict]]:
    """Returns {venue: trades sorted by ts_ns}."""
    base = RAW_ROOT / "trades" / f"symbol={symbol}"
    if not base.exists():
        return {}
    per_venue: dict[str, list[dict]] = {}
    for venue_dir in base.iterdir():
        if not venue_dir.is_dir() or not venue_dir.name.startswith("venue="):
            continue
        venue = venue_dir.name.split("=", 1)[1]
        day_dir = venue_dir / f"date={date}"
        if not day_dir.exists():
            continue
        rows: list[dict] = []
        for f in sorted(day_dir.glob("*.parquet")):
            # ParquetFile.read() skips hive-partition inference, which would otherwise
            # collide with the symbol/venue columns we store inside the file.
            tab = pq.ParquetFile(f).read()
            rows.extend(tab.to_pylist())
        rows.sort(key=lambda r: r["ts_ns"])
        if rows:
            per_venue[venue] = rows
    return per_venue


def build_for(symbol: str, date: str) -> tuple[int, int, dict]:
    """Returns (per_venue_bar_count, agg_bar_count, diagnostics)."""
    agg_cfg = load_ingest().get("aggregate", {})
    mad_k = float(agg_cfg.get("mad_k", 5.0))
    min_venues = int(agg_cfg.get("min_venues_for_filter", 3))
    stale_ms = float(agg_cfg.get("stale_recv_lag_ms", 5000))

    per_venue_trades = _collect_trades(symbol, date)
    if not per_venue_trades:
        log.warning("no trades for symbol=%s date=%s", symbol, date)
        return (0, 0, {})

    venues_index = venue_bitmask_index(all_sources())   # global, stable index

    # 1. per-venue bars
    per_venue_bar_total = 0
    for venue, trades in per_venue_trades.items():
        bars = bars_from_trades(trades, symbol=symbol, venue=venue)
        if bars:
            write_dataframe("bars_1s", bars, symbol=symbol, venue=venue, date=date)
            per_venue_bar_total += len(bars)
            log.info("built per-venue bars venue=%s rows=%d", venue, len(bars))

    # 2. consolidated tape from the spot venues only
    spot_trades = {v: rows for v, rows in per_venue_trades.items() if v in SPOT_VENUES}
    agg_count = 0
    diagnostics: dict = {"contributing_venues": sorted(spot_trades.keys())}
    if len(spot_trades) >= 1:
        consolidated = merge_trades(spot_trades)
        agg_bars, diags = build_agg_bars(
            consolidated, symbol=symbol, venues_index=venues_index,
            mad_k=mad_k, min_venues_for_filter=min_venues, stale_recv_lag_ms=stale_ms,
            return_diagnostics=True,
        )
        if agg_bars:
            write_dataframe("bars_1s", agg_bars, symbol=symbol, venue="agg", date=date)
            agg_count = len(agg_bars)
            log.info("built agg bars rows=%d from venues=%s", agg_count, diagnostics["contributing_venues"])
        dropped = sum(d["n_dropped"] for d in diags)
        diagnostics.update({"agg_bars": agg_count, "trades_dropped_by_filter": int(dropped),
                            "consolidated_trades": len(consolidated)})

    record_correction(
        table_name="bars_1s", kind="backfill", severity="info", symbol=symbol, venue="agg",
        effective_from_ns=0, effective_to_ns=0,
        rows_affected=per_venue_bar_total + agg_count,
        note=f"rebuilt {date}: per_venue={per_venue_bar_total} agg={agg_count} "
             f"venues={diagnostics.get('contributing_venues')} dropped={diagnostics.get('trades_dropped_by_filter', 0)}",
    )
    return (per_venue_bar_total, agg_count, diagnostics)


def _all_present_combos() -> list[tuple[str, str]]:
    base = RAW_ROOT / "trades"
    combos: set[tuple[str, str]] = set()
    if not base.exists():
        return []
    for sym_dir in base.glob("symbol=*"):
        symbol = sym_dir.name.split("=", 1)[1]
        for date_dir in sym_dir.glob("venue=*/date=*"):
            combos.add((symbol, date_dir.name.split("=", 1)[1]))
    return sorted(combos)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--symbol")
    p.add_argument("--date", help="YYYY-MM-DD (UTC)")
    p.add_argument("--all-present", action="store_true", help="build for every (symbol, date) with raw trades")
    args = p.parse_args()
    if args.all_present:
        combos = _all_present_combos()
    elif args.symbol and args.date:
        combos = [(args.symbol, args.date)]
    else:
        p.error("provide --symbol and --date, or --all-present")
        return 2
    total_pv = total_agg = 0
    for symbol, date in combos:
        pv, agg, _ = build_for(symbol, date)
        total_pv += pv
        total_agg += agg
        print(f"{symbol} {date}: per_venue={pv} agg={agg}")
    print(f"TOTAL per_venue={total_pv} agg={total_agg} over {len(combos)} (symbol, date) slices")
    return 0


if __name__ == "__main__":
    sys.exit(main())
