# Benchmarks

_Machine: 3.12.13 on Linux x86_64; generated 2026-05-12 22:32:21Z._

Synthetic micro-benchmarks (single core, deterministic generator) plus query
latency over the shipped dataset. Numbers are indicative, not a guarantee.

> Query-latency figures include the per-call DuckDB connect + view (re)registration and Parquet footer reads, so they are dominated by filesystem round-trips: a few milliseconds on local NVMe, considerably more on a slow or network-backed filesystem. Reusing a warm connection across queries is on the roadmap.

## Parse / build (CPU)

| stage | throughput |
|---|---|
| `bars_from_trades` (trades → 1s bars) | 3,351,343 trades/s |
| `merge_trades` (k-way consolidated merge, 3 venues) | 4,220,376 trades/s |
| `build_agg_bars` (consolidated → agg 1s tape) | 1,661,674 trades/s |

## Storage (Parquet, ZSTD-6, dict-encoded symbol/venue)

| metric | value |
|---|---|
| write throughput | 1,973,404 rows/s |
| read throughput | 15,691,268 rows/s |
| bytes / trade row on disk | 16.32 |

## Query latency (real dataset)

Largest series: `BTC-USDT` / `agg` — 1,733 1s bars; full `bars_1s` scan (12,554 rows) in 0.4529s.

| timeframe | `get_bars` latency | rows returned |
|---|---|---|
| 1s | 4.5379s | 1,733 |
| 1m | 4.4817s | 31 |
| 5m | 4.4844s | 7 |
| 1h | 4.4414s | 2 |
