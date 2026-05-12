"""Build the v1 sample dataset end-to-end (the "deliverable" entry point).

Pipeline:
  1. init DuckDB, seed `symbol_map` from config (point-in-time reference data)
  2. backfill tick-level *trades* for the flagship pairs from every spot venue that
     lists them (window: `[dataset].trades_window_minutes`)
  3. backfill per-venue 1-minute *reference klines* (`bars_ref`) — flagship pairs from
     all venues, breadth universe from Binance — for `[dataset].reference_lookback_days`
  4. backfill perp *funding* history for `[dataset].perp_symbols`
  5. build per-venue 1s bars + the cross-venue consolidated `agg` tape for every
     (symbol, date) that now has raw trades
  6. recompute the coverage matrix and the data-quality scorecards
  7. write `data/meta/dataset_manifest.json`

Every network step is wrapped so a single venue/symbol failure logs and is skipped —
the build always finishes with whatever it could fetch (which is realistic: not every
venue lists every pair).

Usage:
    python -m scripts.build_dataset
    python -m scripts.build_dataset --trades-minutes 30 --reference-days 3 --no-bench
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time

import pandas as pd

from cryptodata.core.corrections import seed_symbol_map
from cryptodata.core.symbols import all_canonical, symbols_for_venue
from cryptodata.paths import META_ROOT, ensure_dirs, load_ingest
from cryptodata.sources.registry import PERP_VENUES, SPOT_VENUES
from cryptodata.storage.duckdb_views import init_db
from scripts.backfill import run_backfill

log = logging.getLogger("build_dataset")


def _venues_listing(symbol: str, venue_pool) -> list[str]:
    out = []
    for v in venue_pool:
        try:
            if any(c == symbol for c, _ in symbols_for_venue(v)):
                out.append(v)
        except Exception:
            pass
    return out


async def _bf(symbol: str, venue: str, start_ns: int, end_ns: int, table: str, interval: str = "1m") -> int:
    try:
        n = await run_backfill(symbol, venue, start_ns, end_ns, table, interval)
        log.info("backfill ok: %s %s %s -> %d rows", symbol, venue, table, n)
        return n
    except Exception as e:  # noqa: BLE001 - resilience by design
        log.warning("backfill FAILED (skipping): %s %s %s — %s", symbol, venue, table, e)
        return 0


async def build(args) -> dict:
    cfg = load_ingest().get("dataset", {})
    flagship = cfg.get("flagship_symbols", ["BTC-USD", "BTC-USDT", "ETH-USD", "ETH-USDT"])
    trades_minutes = int(args.trades_minutes if args.trades_minutes is not None else cfg.get("trades_window_minutes", 45))
    ref_days = int(args.reference_days if args.reference_days is not None else cfg.get("reference_lookback_days", 7))
    ref_interval = cfg.get("reference_interval", "1m")
    perp_symbols = cfg.get("perp_symbols", ["BTC-USDT-PERP", "ETH-USDT-PERP"])
    perp_days = int(cfg.get("perp_lookback_days", 14))

    init_db()
    n_map = seed_symbol_map()
    log.info("symbol_map seeded: %d (canonical, venue) mappings", n_map)

    now_ns = time.time_ns()
    # End the trades window a couple minutes in the past so partial trailing seconds settle.
    trades_end_ns = now_ns - 2 * 60 * 1_000_000_000
    trades_start_ns = trades_end_ns - trades_minutes * 60 * 1_000_000_000
    ref_end_ns = now_ns
    ref_start_ns = now_ns - ref_days * 24 * 3600 * 1_000_000_000
    perp_end_ns = now_ns
    perp_start_ns = now_ns - perp_days * 24 * 3600 * 1_000_000_000

    counts = {"trades": 0, "bars_ref": 0, "funding": 0}

    # 2. flagship trades from every spot venue that lists them
    for sym in flagship:
        for v in _venues_listing(sym, SPOT_VENUES):
            counts["trades"] += await _bf(sym, v, trades_start_ns, trades_end_ns, "trades")

    # 3a. flagship reference klines from all venues that list them
    for sym in flagship:
        for v in _venues_listing(sym, SPOT_VENUES):
            counts["bars_ref"] += await _bf(sym, v, ref_start_ns, ref_end_ns, "klines", ref_interval)

    # 3b. breadth universe (the rest of the spot symbols) — Binance reference klines
    breadth = [c for c in all_canonical() if not c.endswith("-PERP") and c not in flagship]
    for sym in breadth:
        if sym in {c for c, _ in symbols_for_venue("binance")}:
            counts["bars_ref"] += await _bf(sym, "binance", ref_start_ns, ref_end_ns, "klines", ref_interval)

    # 4. perp funding history
    for sym in perp_symbols:
        for v in _venues_listing(sym, PERP_VENUES):
            counts["funding"] += await _bf(sym, v, perp_start_ns, perp_end_ns, "funding")

    # 5. build 1s bars + consolidated tape
    from scripts.build_bars_1s import _all_present_combos, build_for
    combos = _all_present_combos()
    bar_total = agg_total = 0
    for symbol, date in combos:
        pv, agg, _ = build_for(symbol, date)
        bar_total += pv
        agg_total += agg
    log.info("bars built: per_venue=%d agg=%d over %d slices", bar_total, agg_total, len(combos))

    # 6. coverage + quality
    from scripts.coverage_report import _summary as _cov_summary
    from scripts.coverage_report import build_coverage  # noqa: PLC0415
    cov_df = build_coverage()
    cov_summary = _cov_summary(cov_df)
    from cryptodata.quality import quality_summary, write_daily_quality
    write_daily_quality()
    q_summary = quality_summary()
    # mirror the markdown coverage file
    from scripts.coverage_report import _markdown_matrix
    if not cov_df.empty:
        (META_ROOT / "coverage.md").write_text(_markdown_matrix(cov_df))
        (META_ROOT / "coverage.json").write_text(json.dumps({"summary": cov_summary, "partitions": cov_df.to_dict("records")},
                                                            indent=2, sort_keys=True, default=str))

    manifest = {
        "built_at": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime()),
        "params": {"flagship_symbols": flagship, "trades_window_minutes": trades_minutes,
                   "reference_lookback_days": ref_days, "reference_interval": ref_interval,
                   "perp_symbols": perp_symbols, "perp_lookback_days": perp_days},
        "windows_utc": {
            "trades": [str(pd.Timestamp(trades_start_ns, unit="ns", tz="UTC")), str(pd.Timestamp(trades_end_ns, unit="ns", tz="UTC"))],
            "reference": [str(pd.Timestamp(ref_start_ns, unit="ns", tz="UTC")), str(pd.Timestamp(ref_end_ns, unit="ns", tz="UTC"))],
            "perp_funding": [str(pd.Timestamp(perp_start_ns, unit="ns", tz="UTC")), str(pd.Timestamp(perp_end_ns, unit="ns", tz="UTC"))],
        },
        "rows_backfilled": counts,
        "bars_built": {"per_venue_1s": bar_total, "agg_1s": agg_total, "slices": len(combos)},
        "symbol_map_mappings": n_map,
        "coverage": cov_summary,
        "quality": q_summary,
    }
    ensure_dirs()
    (META_ROOT / "dataset_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str))
    return manifest


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--trades-minutes", type=int, default=None, help="override [dataset].trades_window_minutes")
    p.add_argument("--reference-days", type=int, default=None, help="override [dataset].reference_lookback_days")
    p.add_argument("--no-bench", action="store_true", help="skip running cda-bench at the end")
    args = p.parse_args()
    manifest = asyncio.run(build(args))
    print("\n" + "=" * 72)
    print("DATASET BUILD COMPLETE")
    print("=" * 72)
    print(json.dumps(manifest, indent=2, default=str))
    if not args.no_bench:
        try:
            from scripts.bench import main as bench_main
            bench_main()
        except Exception as e:  # noqa: BLE001
            log.warning("bench failed (non-fatal): %s", e)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
