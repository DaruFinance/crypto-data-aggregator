# Data dictionary

Every table, every column. Timestamps are **int64 UTC nanoseconds** on disk; the query
layer converts to `datetime64[ns, UTC]` at the boundary. Partition keys (`symbol`,
`venue`, `date`) are Hive-encoded in the path and surfaced as columns by DuckDB.

## Raw tables — `data/raw/<table>/symbol=/venue=/date=/part-*.parquet`

### `trades` — tick-by-tick executions
| column | type | notes |
|---|---|---|
| `ts_ns` | int64 | exchange timestamp (UTC ns) |
| `recv_ns` | int64 | our local receive time (UTC ns); on REST backfill, == `ts_ns` |
| `symbol` | string | canonical, e.g. `BTC-USD` |
| `venue` | string | `binance`, `coinbase`, `kraken`, `okx`, `bitstamp`, `gemini`, `bitfinex` (spot) or `bybit` (perp) |
| `price` | float64 | |
| `size` | float64 | base-asset quantity |
| `side` | int8 | `+1` buy-aggressor, `−1` sell-aggressor, `0` unknown |
| `trade_id` | string \| null | venue trade id (or aggTrade id); null when the venue doesn't provide one |
| `ingested_at_ns` | int64 | when this row was written (point-in-time) |

### `quotes` — top-of-book (BBO)
`ts_ns, recv_ns, symbol, venue` as above, plus `bid_px, ask_px, bid_sz, ask_sz` (float64) and `ingested_at_ns`.
Note: some venues lack a per-quote exchange timestamp — see `docs/METHODOLOGY.md` §4.

### `book_l2_snapshot` — L2 order-book snapshots
`ts_ns, recv_ns, symbol, venue` plus `depth` (int16, levels per side), `bids` / `asks` (list of `{px: float64, sz: float64}`, best-first), `ingested_at_ns`. v1 ingests this for `BTC-USDT`, `ETH-USDT`, `BTC-USD`, `ETH-USD` only, top-20 levels every 5 s, from Binance and Coinbase.

### `funding` — perpetual funding rates (never aggregated across venues)
`ts_ns` (settlement time), `recv_ns, symbol` (`*-PERP`), `venue` (`binance_futures` or `bybit`), `funding_rate` (float64), `mark_price` (float64 \| null), `next_ts_ns` (int64 \| null, next settlement), `ingested_at_ns`.

### `open_interest`
`ts_ns, recv_ns, symbol, venue`, `oi_base` (float64 \| null, OI in base asset), `oi_quote` (float64 \| null, OI in quote/USD), `ingested_at_ns`. REST-polled on a ~60 s cadence by the live ingester.

## Derived tables — `data/derived/<table>/symbol=/venue=/date=/part-*.parquet`

### `bars_1s` — per-venue + consolidated 1-second OHLCV bars
| column | type | notes |
|---|---|---|
| `ts_ns` | int64 | **left edge** of the second; the bar covers `[ts, ts+1s)` and is causally available at `ts+1s` |
| `symbol` | string | canonical |
| `venue` | string | a venue name for a single-venue series, or `agg` for the cross-venue consolidated tape |
| `open, high, low, close` | float64 | for `agg`: real time-ordered open/close over surviving trades |
| `volume` | float64 | base-asset volume in the second |
| `vwap` | float64 | size-weighted mean trade price in the second |
| `trades` | int32 | trade count in the second (surviving trades, for `agg`) |
| `sources_mask` | int32 | for `agg`: bitmask of contributing venues per the stable global venue→bit index; `0` for single-venue rows |
| `ingested_at_ns` | int64 \| null | when this bar row was written (point-in-time; null for v0-era parts) |

`get_bars(symbol, start, end, timeframe, sources=['agg'], asof=None)` resamples these to any timeframe, left-closed/left-labeled, with vwap kept volume-weighted under aggregation; `asof=` filters to rows with `ingested_at_ns ≤ asof`.

### `bars_ref` — per-venue REST reference klines
`ts_ns` (bar open), `symbol`, `venue`, `interval` (string: `1m`, `5m`, `1h`, …), `open, high, low, close, volume` (float64), `trades` (int32), `ingested_at_ns` (int64 \| null). These are what the venue published, at the venue's native granularity — kept separate from `bars_1s` so the consolidated tape never mixes our own roll-ups with a venue's candles. Queried via `get_ref_bars`.

## Metadata tables (native DuckDB tables in `data/duckdb/aggregator.duckdb`)

### `symbol_map` — point-in-time reference data
`canonical, venue, native, asset_class` (`spot`|`perp`), `base, quote, effective_from_ns, effective_to_ns` (null = still listed), `note`. Seeded from `config/symbols.toml`; ticker renames/delistings close out an interval (`retire_mapping`) rather than mutate in place; `symbol_map_asof(t)` returns the mapping as of instant `t`.

### `venue_status` — feed-health snapshots
`ts_ns, venue, symbol, stream, up` (bool), `coverage_pct` (double \| null), `notes`. Written periodically during live ingest; queried via `venue_status()`.

### `corrections` — bitemporal corrections log
`correction_id` (= recording epoch ns), `recorded_at_ns` (valid time), `effective_from_ns`/`effective_to_ns` (event-time range covered), `table_name, symbol, venue, kind` (`backfill`|`restatement`|`bad_range`|`gap`|`note`), `severity` (`info`|`minor`|`major`|`critical`), `rows_affected`, `note`. Every backfill/rebuild and any known-bad-range or restatement is logged here.

### `daily_quality` — per-(symbol, venue, date) scorecard
`date, symbol, venue, score` (0–100), `grade` (A–F), `bars, expected_bars, completeness_pct, max_gap_seconds, n_issues, n_critical, issues_json` (serialized list of `{check, severity, count, detail, sample}`), `computed_at_ns`. Produced by `cda-validate`; mirrored to `data/meta/quality/<date>.json`.

### `coverage` — the coverage matrix
`table_name, symbol, venue, date, rows` (record count), `first_ts_ns, last_ts_ns, span_seconds, bytes_on_disk, computed_at_ns`. Rebuilt by `cda-coverage`; mirrored to `data/meta/coverage.{json,md}`.

### `schema_version`
`version` (int), `applied_at` (timestamp). Current version: 2. Forward-only `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migrations run on every `init_db`.

## `data/meta/` JSON artifacts

| file | contents |
|---|---|
| `coverage.json` / `coverage.md` | the coverage matrix (machine-readable / a pasteable markdown table per table) |
| `quality/<date>.json` | full scorecards for that date (score, grade, all issues) |
| `quality_summary.json` | rolled-up quality summary (mean/min score, grade counts, total criticals) |
| `benchmarks.json` | latest `cda-bench` output (CPU micro-benchmarks + query latency) |
| `dataset_manifest.json` | what the last `cda-build-dataset` produced (windows, row counts per table, agg-tape venues per symbol, symbol-map size, corrections logged, quality summary) |
