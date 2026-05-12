# crypto-data-aggregator

A multi-venue crypto market-data aggregator with a **consolidated tape** at its core.
It ingests trades, top-of-book quotes, L2 order-book snapshots, perpetual funding
rates and open interest from nine venues, normalizes everything to one canonical
nanosecond-timestamped schema, builds a cross-venue consolidated price series with
full per-bar provenance, scores every slice for data quality, and serves it through
a point-in-time-correct, pandas-first query API over partitioned Parquet.

It applies the same disciplines a serious equities/options/futures data vendor would
expect — nanosecond timestamps, point-in-time reference data, a bitemporal corrections
log, an explicit data-quality scorecard, and a documented methodology — to crypto
spot + perps.

---

## What's in the box

| Layer | What it does |
|---|---|
| **Ingest** | One supervised async worker per `(venue, stream)`. WS feeds reconnect with backoff *inside* the adapter; a worker that hits an unexpected error logs, backs off (jittered), and reconnects — it never dies for the life of the process. A REST reconciler gap-fills trades after disconnects. |
| **Consolidated tape** | `cryptodata.core.consolidated`: k-way merges per-venue *trade* streams on `(exchange_ts, recv_ts)`, applies a robust per-second outlier filter (median + MAD-derived σ, with a minimum-venue guard so a thin book is never silently halved), optionally drops venues flagged DOWN by the health tracker or whose receive lag exceeds a budget, and rolls the survivors into one 1-second bar with a real `open`/`close`, size-weighted `vwap`, and a `sources_mask` bitmask of the venues that survived the filter. |
| **Storage** | Hive-partitioned Parquet (`symbol=/venue=/date=`), ZSTD-6, dictionary-encoded `symbol`/`venue`, hourly part rotation, nightly compaction. DuckDB views sit over the tree; metadata tables (`symbol_map`, `venue_status`, `corrections`, `daily_quality`, `coverage`, `schema_version`) live in the DuckDB file with forward-only `ADD COLUMN` migrations. |
| **Point-in-time** | Every row carries `ingested_at_ns` (raw tables natively; `bars_1s`/`bars_ref` as a nullable column). `get_bars(..., asof=...)` reproduces exactly what a query at that instant would have returned. `symbol_map` keeps listing/delisting effective dates; `symbol_map_asof(t)` is the reference data as of `t`. Anything that isn't a row append — a restatement, a known-bad range, a discovered gap, a backfill batch — is written to a **bitemporal `corrections` log** (valid time + event time). |
| **Data quality** | `cryptodata.quality`: OHLC invariants, monotonic/duplicate timestamps, off-grid bars, gaps, flatlines, trade-id duplication, extreme prints, receive-lag, crossed/locked quotes, implausible spreads, consolidated-vs-venue range breaches, cross-venue dispersion. Each `(symbol, venue, date)` gets a 0–100 score and an A–F grade, persisted to `daily_quality` and mirrored to `data/meta/quality/<date>.json`. `cda-validate` exits non-zero on any CRITICAL issue so CI can gate on it. |
| **Query API** | `get_bars`, `get_trades`, `get_quotes`, `get_funding`, `get_open_interest`, `get_ref_bars`, `get_book_snapshots` / `book_top` / `book_quality`, `list_symbols`, `venue_status`. All return `pandas.DataFrame` indexed by UTC timestamp; bars are left-closed / left-labeled (the `09:30` bar covers `[09:30, 09:31)` and is causally available at `09:31`). |
| **Observability** | A Prometheus exporter (optional `obs` extra — no-op if not installed): rows ingested, worker restarts, parquet files written, per-feed up/down, receive lag, latest quality score. `cda-status` prints a one-screen dashboard (coverage totals, feed health, latest grades, corrections). A sample Grafana dashboard JSON ships in `docs/grafana/`. |
| **Benchmarks** | `cda-bench` runs CPU micro-benchmarks (1s-bar build, k-way merge, agg roll-up, Parquet read/write) and query latency over the shipped dataset; writes `data/meta/benchmarks.json` and `docs/BENCHMARKS.md`. |

## Venues & coverage

- **Spot** (eligible to contribute to the consolidated price): Binance, Coinbase, Kraken, OKX, Bitstamp, Gemini, Bitfinex.
- **Perpetuals** (funding + open interest, kept out of the spot consolidated price by construction): Binance Futures, Bybit.
- **Symbol universe** — top spot pairs by volume: BTC, ETH, SOL, BNB, XRP, ADA, DOGE, AVAX, LINK, MATIC, DOT, LTC, TRX, NEAR, ATOM, ARB, OP, APT, FIL, ETC (USDT side; BTC/ETH/SOL also USD side). Flagship pairs (`BTC-USD`, `BTC-USDT`, `ETH-USD`, `ETH-USDT`) carry tick-level trades from every spot venue that lists them; the rest carry per-venue 1-minute reference klines (`bars_ref`). Perpetuals: `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP`, `BNB-USDT-PERP`, `XRP-USDT-PERP`.

Reference data — canonical→native ticker per venue, asset class, listing dates — lives in `config/symbols.toml` and is materialized into the point-in-time `symbol_map` table. The current coverage matrix (rows per `table × symbol × venue × date`, bytes on disk, date span) is regenerated by `cda-coverage` into `data/meta/coverage.{json,md}`; a build manifest is written to `data/meta/dataset_manifest.json`.

See [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md) for the consolidated-tape spec and known limitations, [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the component map, [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) for every table/column, [`docs/OPERATIONS.md`](docs/OPERATIONS.md) for the runbook, [`docs/SLA.md`](docs/SLA.md) for the coverage/quality targets, and [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for performance.

---

## Install

```bash
uv venv && uv pip install -e ".[dev]"          # or: python -m venv .venv && pip install -e ".[dev]"
pip install -e ".[obs]"                        # optional: Prometheus exporter
```

Python ≥ 3.11.

## Build the sample dataset

```bash
cda-build-dataset                              # backfills real data, builds bars, runs coverage + validation
# or with overrides:
python -m scripts.build_dataset --trades-minutes 30 --reference-days 5
```

This: seeds `symbol_map`; REST-backfills tick-level trades for the flagship pairs from every spot venue that lists them; backfills per-venue 1-minute reference klines (flagship pairs from all venues, breadth universe from Binance) and perp funding history; builds per-venue 1s bars + the consolidated `agg` tape; recomputes the coverage matrix and the data-quality scorecards; writes `data/meta/dataset_manifest.json`. Every network step is isolated — a venue/symbol failure logs and is skipped, so not-every-venue-lists-every-pair is handled gracefully.

## Query

```python
from cryptodata import get_bars, get_trades, get_funding, get_book_snapshots, book_top

# Cross-venue consolidated 1-minute bars (the default `sources=['agg']`):
bars = get_bars("BTC-USD", "2026-05-12 00:00", "2026-05-12 01:00", "1m")

# Same window, point-in-time: exactly what a query at 00:30 would have returned
bars_then = get_bars("BTC-USD", "2026-05-12 00:00", "2026-05-12 01:00", "1m", asof="2026-05-12 00:30")

# A single venue, or several stacked:
bn = get_bars("BTC-USDT", "2026-05-12 00:00", "2026-05-12 01:00", "5m", sources=["binance"])
multi = get_bars("BTC-USD", "2026-05-12 00:00", "2026-05-12 01:00", "1m", sources=["coinbase", "kraken", "bitstamp"])

# Raw ticks, funding, books:
ticks = get_trades("BTC-USD", "2026-05-12 00:00:00", "2026-05-12 00:05:00", venues=["coinbase", "kraken"])
fund  = get_funding("BTC-USDT-PERP", "2026-05-01", "2026-05-12", venues="all")
tob   = book_top(get_book_snapshots("BTC-USDT", "2026-05-12 00:00", "2026-05-12 01:00", venue="binance"))
```

Returned bar columns: `open, high, low, close, volume, vwap, trades, sources_mask, ingested_at_ns` (+ `venue`, `symbol`).

## Live ingest

```bash
cda-ingest                                     # runs forever; Ctrl-C flushes pending Parquet and exits
```

Multiplexes WS streams per venue, batched Parquet writes, supervised worker restarts, hourly REST gap-reconcile, an in-memory health tracker dumped to `venue_status`, and the Prometheus exporter (`http://localhost:9464/metrics` if the `obs` extra is installed).

## Operations CLIs

```bash
cda-backfill --symbol BTC-USD --venue coinbase --start 2026-05-01 --end 2026-05-02 --table trades
cda-backfill --symbol ETH-USDT --venue binance --start 2026-04-12 --end 2026-05-12 --table klines --interval 1m
cda-build-bars --symbol BTC-USD --date 2026-05-12          # or --all-present
cda-compact                                                # nightly part-file compaction
cda-coverage                                               # rebuild data/meta/coverage.{json,md} + DuckDB `coverage`
cda-validate [--symbol …] [--fail-on minor|major|critical] # data-quality scorecards; non-zero on a failing slice
cda-status [--json]                                        # ops dashboard
cda-bench                                                  # benchmarks → data/meta/benchmarks.json + docs/BENCHMARKS.md
```

## Storage layout

```
data/raw/<table>/symbol=<S>/venue=<V>/date=<YYYY-MM-DD>/part-h<HH>-<ns>.parquet     # trades, quotes, book_l2_snapshot, funding, open_interest
data/derived/bars_1s/symbol=<S>/venue=<V|agg>/date=<YYYY-MM-DD>/part-<ns>.parquet   # per-venue + consolidated 1s bars
data/derived/bars_ref/symbol=<S>/venue=<V>/date=<YYYY-MM-DD>/part-<ns>.parquet      # per-venue REST reference klines
data/duckdb/aggregator.duckdb                                                        # views over the tree + metadata tables
data/meta/{coverage.json,coverage.md,quality/<date>.json,quality_summary.json,benchmarks.json,dataset_manifest.json}
```

## Tests

```bash
pytest -q          # invariants, resampling, storage roundtrip, consolidated tape, quality checks,
                   # corrections / point-in-time, books, and a per-venue WS-parse fixture suite
ruff check .
mypy cryptodata
```

## Roadmap

`bars_ref` cross-checks against the consolidated tape as a CI gate; clock-skew estimation per venue; L2 ingest on more than Binance + Coinbase; dated futures / options to mirror a wider vendor footprint; a FastAPI read layer (the `api` extra is reserved for it); Rust hot path for the k-way merge if ingest volume warrants it.
