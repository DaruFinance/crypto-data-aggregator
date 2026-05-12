# Architecture

```
                         live WS feeds                         REST (backfill / reconcile)
   binance ─┐  coinbase ─┐  kraken ─┐  okx ─┐  bitstamp ─┐  gemini ─┐  bitfinex ─┐  binance_futures ─┐  bybit ─┐
            │            │          │       │            │          │            │                  │         │
            ▼ adapters (cryptodata/sources/*) — one per venue, implement the `Source` protocol; venue-native
              symbol translation, UTC-ns timestamps, side normalization (+1/−1/0); WS reconnect-with-backoff
              lives inside the adapter
            │
   ┌────────┴───────────────────────────────────────────────────────────────────────────────────────┐
   │  ingest orchestrator (core/ingest.py)                                                            │
   │    • one supervised IngestWorker per (venue, stream) — restarts on error with jittered backoff,  │
   │      only exits on cancellation or an unsupported stream                                          │
   │    • AsyncWriter → PartitionedParquetWriter: batched, hourly-rotated, ZSTD-6, dict-encoded        │
   │    • HealthTracker (in-memory rate windows) → dumped to `venue_status` by health_writer           │
   │    • REST reconciler: hourly trade-gap fill after disconnects                                     │
   │    • Prometheus exporter (optional `obs` extra)                                                   │
   └────────┬────────────────────────────────────────────────────────────────────────────────────────┘
            │  appends normalized rows
            ▼
   data/raw/<table>/symbol=<S>/venue=<V>/date=<D>/part-*.parquet     (trades, quotes, book_l2_snapshot,
                                                                      funding, open_interest)
            │
            │  build_bars_1s / build_dataset
            ▼
   ┌─────────────────────────────────────────────────────────────────────────────┐
   │  derivation                                                                  │
   │    • bars_from_trades  — per-venue 1s OHLCV bars                              │
   │    • consolidated tape — k-way merge spot trades → robust per-second filter → │
   │                          agg 1s bars with `sources_mask` provenance          │
   │    • bars_ref          — REST-backfilled per-venue klines (independent ref)   │
   └─────────────────────────────────┬───────────────────────────────────────────┘
                                     ▼
   data/derived/bars_1s/...   data/derived/bars_ref/...
                                     │
            ┌────────────────────────┼─────────────────────────────────────────┐
            ▼                        ▼                                          ▼
   DuckDB views over the tree   metadata tables in the DuckDB file:      data/meta/*.json
   (auto-reflect new parquet)    symbol_map (point-in-time ref data)       coverage.{json,md}
            │                    venue_status                              quality/<date>.json
            ▼                    corrections (bitemporal log)              quality_summary.json
   query API (cryptodata.query) daily_quality (scorecard)                 benchmarks.json
   get_bars / get_trades / ...  coverage (matrix)                         dataset_manifest.json
   → pandas.DataFrame           schema_version (forward-only migrations)
   (point-in-time via `asof=`)
            │
            ▼
   data quality (cryptodata.quality)    observability (cryptodata.obs)
   run_checks → score_day → daily_quality   Prometheus exporter, `cda-status` dashboard
   `cda-validate` (CI gate)
```

## Module map

| Package / module | Responsibility |
|---|---|
| `cryptodata/sources/` | One adapter per venue (`binance`, `binance_futures`, `bybit`, `coinbase`, `kraken`, `okx`, `bitstamp`, `gemini`, `bitfinex`) implementing the `Source` protocol in `base.py`; `registry.py` constructs them and declares `SPOT_VENUES` / `PERP_VENUES`. |
| `cryptodata/core/symbols.py` | Canonical↔native symbol mapping from `config/symbols.toml`; `venue_bitmask_index` for the provenance mask. |
| `cryptodata/core/ingest.py` · `worker.py` · `writer.py` · `health.py` · `health_writer.py` · `reconciler.py` · `ratelimit.py` | Live ingest: orchestration, supervised workers, the batched parquet writer wrapper, the health tracker + its dumper, the REST gap reconciler, the token-bucket rate limiter. |
| `cryptodata/core/aggregate.py` | `bars_from_trades` (per-venue 1s bars) + the legacy per-second cross-venue combiner `aggregate_second`. |
| `cryptodata/core/consolidated.py` | The current consolidated-tape path: `merge_trades` (k-way merge) + `build_agg_bars` (robust filter + roll-up + provenance). |
| `cryptodata/core/corrections.py` | The bitemporal `corrections` log (`record_correction` / `list_corrections`) and point-in-time `symbol_map` maintenance (`seed_symbol_map`, `retire_mapping`, `symbol_map_asof`). |
| `cryptodata/core/resample.py` | Left-closed/left-labeled resampling of 1s bars to arbitrary timeframes, with volume-weighted vwap. |
| `cryptodata/storage/` | `schemas.py` (canonical pyarrow schemas for every table), `parquet.py` (the partitioned writer + `write_dataframe`), `duckdb_views.py` (views over the tree + the metadata tables + forward-only `ADD COLUMN` migrations), `compact.py` (nightly part-file merge). |
| `cryptodata/query/` | `bars.py` (`get_bars`, with `asof=`), `trades.py`, `quotes.py`, `funding.py`, `ref.py` (`get_ref_bars`), `books.py` (`get_book_snapshots`, `book_top`, `book_quality`), `meta.py` (`list_symbols`, `venue_status`, `get_open_interest`). |
| `cryptodata/quality/` | `checks.py` (the invariant/consistency checks → `Issue` records), `report.py` (`score_day`, `write_daily_quality`, `quality_summary`). |
| `cryptodata/obs/` | `metrics.py` (Prometheus exporter, no-op without the `obs` extra), `status.py` (the `cda-status` dashboard). |
| `scripts/` | CLI entry points: `run_ingest`, `backfill`, `build_bars_1s`, `build_dataset`, `compact`, `coverage_report`, `validate`, `bench`, `status`, `smoke_test`. |

## Design choices worth knowing

- **One writer instance buffers by `(table, symbol, venue, hour)`** and flushes on a
  row-count or wall-clock threshold; files rotate hourly so a crash loses at most the
  last partial batch; `cda-compact` merges hourly parts into one file per day.
- **DuckDB views auto-reflect the parquet tree** (`read_parquet(glob, hive_partitioning,
  union_by_name)`), so adding a column to a schema (e.g. `bars_1s.ingested_at_ns`) is
  backward-compatible: old part files just NULL-fill the new column.
- **`connect()` reopens the DuckDB file and re-registers views each time** so a
  read process always sees freshly-written partitions. The trade-off is per-query
  connection overhead (dominated by filesystem round-trips — see `docs/BENCHMARKS.md`);
  reusing a warm connection across queries is on the roadmap.
- **Reference data is point-in-time.** `symbol_map` carries `effective_from_ns` /
  `effective_to_ns`; ticker renames/delistings close out an interval rather than
  mutating in place; `symbol_map_asof(t)` is the reference data as of `t`.
