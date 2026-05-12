"""Micro-benchmarks for the hot paths + query latency over the real dataset.

Reports, as JSON (`data/meta/benchmarks.json`) and a markdown table
(`docs/BENCHMARKS.md`):

  parse/build  — rows/s for `bars_from_trades` and the consolidated k-way merge
                 (driven by a synthetic trade generator — this is a CPU micro-bench,
                 not a data sample)
  storage      — Parquet write & read throughput (synthetic rows)
  query        — wall-clock latency for `get_bars` at several timeframes, and a raw
                 DuckDB scan, run against whatever real data is in `data/`

Run after `cda-build-dataset` so the query numbers reflect the shipped corpus:
    python -m scripts.bench
"""
from __future__ import annotations

import json
import logging
import platform
import sys
import time

from cryptodata.paths import META_ROOT, PROJECT_ROOT, ensure_dirs

log = logging.getLogger("bench")


def _now() -> float:
    return time.perf_counter()


def _synthetic_trades(n: int, *, venue: str, base_px: float = 50_000.0) -> list[dict]:
    """n trades over n/50 seconds (≈50 trades/s), small random walk. Deterministic."""
    import random
    rng = random.Random(1234 + hash(venue) % 9973)
    rows = []
    px = base_px
    t0 = 1_700_000_000_000_000_000
    for i in range(n):
        px *= 1.0 + rng.uniform(-2e-5, 2e-5)
        ts = t0 + (i // 50) * 1_000_000_000 + (i % 50) * 20_000_000
        rows.append({"ts_ns": ts, "recv_ns": ts + rng.randint(0, 5_000_000), "symbol": "BTC-USDT",
                     "venue": venue, "price": round(px, 2), "size": round(rng.uniform(0.001, 2.0), 4),
                     "side": 1 if rng.random() > 0.5 else -1, "trade_id": f"{venue}-{i}", "ingested_at_ns": ts})
    return rows


def bench_parse_build(n: int = 200_000) -> dict:
    from cryptodata.core.aggregate import bars_from_trades
    from cryptodata.core.consolidated import build_agg_bars, merge_trades

    per_venue = {v: _synthetic_trades(n, venue=v, base_px=p) for v, p in
                 [("binance", 50_000.0), ("coinbase", 50_010.0), ("kraken", 49_990.0)]}

    t = _now()
    nbars = sum(len(bars_from_trades(rows, symbol="BTC-USDT", venue=v)) for v, rows in per_venue.items())
    dt_build = _now() - t

    t = _now()
    merged = merge_trades(per_venue)
    dt_merge = _now() - t

    t = _now()
    agg = build_agg_bars(merged, symbol="BTC-USDT", mad_k=5.0, min_venues_for_filter=3)
    dt_agg = _now() - t

    total_trades = sum(len(v) for v in per_venue.values())
    return {
        "trades_in": total_trades,
        "bars_from_trades_rows_per_s": round(total_trades / dt_build) if dt_build else None,
        "merge_trades_rows_per_s": round(total_trades / dt_merge) if dt_merge else None,
        "build_agg_bars_rows_per_s": round(len(merged) / dt_agg) if dt_agg else None,
        "per_venue_bars": int(nbars),
        "agg_bars": int(len(agg)),
        "timings_s": {"bars_from_trades": round(dt_build, 4), "merge_trades": round(dt_merge, 4),
                      "build_agg_bars": round(dt_agg, 4)},
    }


def bench_storage(n: int = 500_000) -> dict:
    import tempfile
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    from cryptodata.storage.schemas import schema_for

    rows = _synthetic_trades(n, venue="binance")
    schema = schema_for("trades")
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    tab = pa.Table.from_arrays([pa.array(cols[f.name], type=f.type) for f in schema], schema=schema)

    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "bench.parquet"
        t = _now()
        pq.write_table(tab, path, compression="zstd", compression_level=6, use_dictionary=["symbol", "venue"])
        dt_w = _now() - t
        size = path.stat().st_size
        t = _now()
        _ = pq.ParquetFile(path).read()
        dt_r = _now() - t
    return {
        "rows": n,
        "write_rows_per_s": round(n / dt_w) if dt_w else None,
        "read_rows_per_s": round(n / dt_r) if dt_r else None,
        "bytes_on_disk": size,
        "bytes_per_row": round(size / n, 2),
        "timings_s": {"write": round(dt_w, 4), "read": round(dt_r, 4)},
    }


def bench_query() -> dict:
    """Query the real dataset if present. Picks the symbol/venue with the most bars."""
    from cryptodata.storage.duckdb_views import connect

    with connect() as con:
        try:
            row = con.execute(
                "SELECT symbol, venue, MIN(ts_ns) AS lo, MAX(ts_ns) AS hi, COUNT(*) AS n "
                "FROM bars_1s GROUP BY symbol, venue ORDER BY n DESC LIMIT 1"
            ).fetchone()
        except Exception:
            row = None
        scan_rows = None
        if row:
            t = _now()
            scan_rows = con.execute("SELECT COUNT(*) FROM bars_1s").fetchone()[0]
            dt_scan = _now() - t
        else:
            dt_scan = None

    if not row:
        return {"note": "no bars_1s data — run cda-build-dataset first"}

    from cryptodata import get_bars
    symbol, venue, lo, hi, n = row
    import pandas as pd
    start = pd.Timestamp(lo, unit="ns", tz="UTC")
    end = pd.Timestamp(hi, unit="ns", tz="UTC") + pd.Timedelta(seconds=1)
    out = {"symbol": symbol, "venue": venue, "bars_1s_rows": int(n),
           "duckdb_scan_rows": int(scan_rows) if scan_rows is not None else None,
           "duckdb_scan_s": round(dt_scan, 4) if dt_scan is not None else None, "get_bars_s": {}}
    for tf in ("1s", "1m", "5m", "1h"):
        t = _now()
        df = get_bars(symbol, start, end, tf, sources=[venue])
        dt = _now() - t
        out["get_bars_s"][tf] = {"seconds": round(dt, 4), "rows": int(len(df))}
    return out


def _markdown(report: dict) -> str:
    pb = report["parse_build"]
    st = report["storage"]
    q = report["query"]
    lines = [
        "# Benchmarks", "",
        f"_Machine: {report['machine']['python']} on {report['machine']['platform']}; "
        f"generated {report['generated_at']}._", "",
        "Synthetic micro-benchmarks (single core, deterministic generator) plus query",
        "latency over the shipped dataset. Numbers are indicative, not a guarantee.", "",
        "> Query-latency figures include the per-call DuckDB connect + view (re)registration"
        " and Parquet footer reads, so they are dominated by filesystem round-trips: a few"
        " milliseconds on local NVMe, considerably more on a slow or network-backed filesystem."
        " Reusing a warm connection across queries is on the roadmap.", "",
        "## Parse / build (CPU)", "",
        "| stage | throughput |",
        "|---|---|",
        f"| `bars_from_trades` (trades → 1s bars) | {pb['bars_from_trades_rows_per_s']:,} trades/s |",
        f"| `merge_trades` (k-way consolidated merge, 3 venues) | {pb['merge_trades_rows_per_s']:,} trades/s |",
        f"| `build_agg_bars` (consolidated → agg 1s tape) | {pb['build_agg_bars_rows_per_s']:,} trades/s |",
        "",
        "## Storage (Parquet, ZSTD-6, dict-encoded symbol/venue)", "",
        "| metric | value |",
        "|---|---|",
        f"| write throughput | {st['write_rows_per_s']:,} rows/s |",
        f"| read throughput | {st['read_rows_per_s']:,} rows/s |",
        f"| bytes / trade row on disk | {st['bytes_per_row']} |",
        "",
        "## Query latency (real dataset)", "",
    ]
    if "note" in q:
        lines.append(f"_{q['note']}_")
    else:
        lines += [
            f"Largest series: `{q['symbol']}` / `{q['venue']}` — {q['bars_1s_rows']:,} 1s bars; "
            f"full `bars_1s` scan ({q['duckdb_scan_rows']:,} rows) in {q['duckdb_scan_s']}s.", "",
            "| timeframe | `get_bars` latency | rows returned |",
            "|---|---|---|",
        ]
        for tf, r in q["get_bars_s"].items():
            lines.append(f"| {tf} | {r['seconds']}s | {r['rows']:,} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ensure_dirs()
    report = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime()),
        # Coarse, environment-agnostic machine label (no kernel build string / hostname).
        "machine": {"python": platform.python_version(),
                    "platform": f"{platform.system()} {platform.machine()}",
                    "processor": platform.processor() or "unknown"},
        "parse_build": bench_parse_build(),
        "storage": bench_storage(),
        "query": bench_query(),
    }
    (META_ROOT / "benchmarks.json").write_text(json.dumps(report, indent=2, sort_keys=True))
    docs_dir = PROJECT_ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)
    (docs_dir / "BENCHMARKS.md").write_text(_markdown(report))
    print(json.dumps({k: v for k, v in report.items() if k != "machine"}, indent=2)[:2000])
    print(f"\nwrote {META_ROOT/'benchmarks.json'} and {docs_dir/'BENCHMARKS.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
