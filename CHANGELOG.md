# Changelog

## 0.2.0

A substantial rebuild around three things: a *correct* consolidated tape, a real
data-quality layer, and point-in-time discipline.

### Added
- **Consolidated tape** (`cryptodata.core.consolidated`): k-way merge of per-venue
  *trade* streams → robust per-second outlier filter (median + MAD-derived σ, with a
  minimum-venue guard) → 1s bars with real time-ordered open/close, size-weighted vwap,
  and a `sources_mask` provenance bitmask. Optional exclusion of health-flagged-DOWN
  venues and of trades whose receive lag exceeds a budget. Replaces the v0 bar-level
  volume-weighted-average aggregation in the dataset build.
- **Data-quality layer** (`cryptodata.quality`): OHLC invariants, monotonic/duplicate/
  off-grid timestamps, liquidity-aware gap detection, flatlines, trade-id duplication,
  extreme prints, receive-lag, crossed/locked quotes, implausible spreads,
  consolidated-vs-venue range breaches, cross-venue dispersion. Per-`(symbol, venue,
  date)` 0–100 score + A–F grade → `daily_quality` table + `data/meta/quality/<date>.json`.
  `cda-validate` exits non-zero on any CRITICAL issue.
- **Point-in-time correctness**: `ingested_at_ns` on `bars_1s`/`bars_ref` (raw tables
  already had it); `get_bars(..., asof=t)`; a bitemporal `corrections` log
  (`cryptodata.core.corrections`); point-in-time `symbol_map` (`seed_symbol_map`,
  `retire_mapping`, `symbol_map_asof`).
- **Four new spot venues**: OKX, Bitstamp, Gemini, Bitfinex — bringing spot consolidated
  coverage to seven venues; perps remain Binance Futures + Bybit.
- **`bars_ref` table**: per-venue REST reference klines, kept separate from `bars_1s`;
  queried via `get_ref_bars`.
- **L2 book query API**: `get_book_snapshots`, `book_top` (flatten to BBO + mid + spread
  + imbalance), `book_quality` (ascending/descending levels, crossed top, dup levels).
- **Observability**: a Prometheus exporter (optional `obs` extra, no-op without it) and
  `cda-status` (coverage / feed health / quality grades / corrections dashboard); a
  sample Grafana dashboard in `docs/grafana/`.
- **Benchmarks**: `cda-bench` — CPU micro-benchmarks (1s-bar build, k-way merge, agg
  roll-up, Parquet read/write) + query latency over the shipped dataset →
  `data/meta/benchmarks.json` + `docs/BENCHMARKS.md`.
- **Dataset bootstrap**: `cda-build-dataset` — backfills real data, builds the bars +
  tape, runs coverage + validation, writes `data/meta/dataset_manifest.json`. Resilient
  to per-venue/per-symbol failures.
- **Coverage matrix**: `cda-coverage` → `data/meta/coverage.{json,md}` + `coverage` table.
- **Adapter parse tests**: recorded-shape WS trade fixtures (`tests/fixtures/`) replayed
  through every venue adapter (`tests/test_sources_parse.py`); new test suites for the
  consolidated tape, the quality checks, the corrections/point-in-time layer, the book
  queries, and as-of reads.
- **Docs**: `docs/METHODOLOGY.md` (tape spec + known limitations), `docs/ARCHITECTURE.md`,
  `docs/DATA_DICTIONARY.md`, `docs/OPERATIONS.md`, `docs/SLA.md`, `docs/BENCHMARKS.md`,
  and analysis notebooks in `notebooks/`.
- **Ops**: GitHub Actions CI (ruff + pytest + a build-dataset smoke), a `Dockerfile`,
  a `Makefile`, `py.typed` + a mypy config, an MIT `LICENSE`.

### Fixed
- **Crashed ingest workers no longer die for the life of the process.** The v0 worker
  caught an exception, slept 5 s, and returned — the venue feed was then permanently
  dead. Workers now restart with jittered backoff and only exit on cancellation or an
  unsupported stream.
- **Kraken WS symbol mapping.** v0 looked up the canonical symbol by the REST altname
  (`XBTUSD`), so the WS v2 `BTC/USD` form never matched and the feed dropped everything
  for XBT pairs. The adapter now keeps an explicit WS-pair → canonical map.
- **Aggregation methodology.** v0's docstrings claimed staleness and venue-health
  filtering that the code never did, and its outlier filter was a no-op for fewer than
  three venues (`pstdev` of 3–5 closes at 5σ never fires). The legacy `aggregate_second`
  now uses a robust median+MAD filter with an explicit `excluded_venues` parameter and
  honest docs; the dataset build uses the trade-level consolidated path.
- **Dead config wired up**: `stale_recv_lag_ms` is now used by the tape builder;
  `mad_k` / `min_venues_for_filter` are new and used; removed the unreferenced klines→
  `bars_1s` routing (klines go to `bars_ref`).
- **SQL query construction** parametrized (`get_bars` venue list) instead of f-string
  interpolation.
- **Schema evolution**: forward-only `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migrations
  so a v0-era DuckDB file opens cleanly; `bars_1s` gained a nullable `ingested_at_ns`.

### Changed
- Python requirement relaxed from 3.13 to **≥ 3.11** (matches the actual minimum —
  `tomllib`).
- Ruff lint config (E/F/I/UP/B/C4/SIM) and a mypy config added; the codebase passes both.
- `MIT` license declared.

## 0.1.0

Initial prototype: per-venue adapters (Binance, Coinbase, Kraken, Bybit, Binance
Futures), partitioned Parquet + DuckDB views, a bar-level cross-venue VWAP aggregation,
a pandas-first query API, and a small test suite.
