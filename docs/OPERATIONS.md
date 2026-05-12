# Operations runbook

## Daily / continuous

| What | Command | Cadence |
|---|---|---|
| Live ingest | `cda-ingest` (run under a supervisor — systemd, `tini` in Docker, …) | always on |
| Part-file compaction | `cda-compact` | nightly, ~03:00 UTC |
| Coverage matrix refresh | `cda-coverage` | nightly, after compaction |
| Data-quality scorecards | `cda-validate` (non-zero exit ⇒ a slice failed) | nightly, after coverage |
| Status check | `cda-status` | on demand / before/after deploys |
| Metrics | scrape `http://<host>:9464/metrics` (Prometheus); import `docs/grafana/cryptodata-overview.json` | continuous |

`cda-ingest` itself self-heals: WS reconnects with backoff inside each adapter; an
`IngestWorker` that hits an unexpected error logs, backs off (jittered, capped at 60 s),
and reconnects — it never dies for the life of the process; an hourly REST reconciler
fills trade gaps left by disconnects.

## Backfilling

```bash
# tick-level trades for a (symbol, venue) window
cda-backfill --symbol BTC-USD --venue coinbase --start 2026-05-01 --end 2026-05-02 --table trades

# perp funding history
cda-backfill --symbol BTC-USDT-PERP --venue binance_futures --start 2026-04-01 --end 2026-05-01 --table funding

# per-venue reference klines (lands in bars_ref)
cda-backfill --symbol ETH-USDT --venue binance --start 2026-04-12 --end 2026-05-12 --table klines --interval 1m

# then (re)build derived bars + the consolidated tape for the affected days
cda-build-bars --symbol BTC-USD --date 2026-05-01      # or: --all-present
cda-coverage && cda-validate
```

Every backfill writes a `backfill` row to the `corrections` log. Re-running a backfill
for the same window appends a new part file (it does not de-duplicate at write time);
nightly compaction merges them — if you re-backfill an overlapping window deliberately,
consider clearing the affected `date=` partition first.

REST backfill depth differs by venue (Bitstamp ≈ last 24 h of trades; Kraken OHLC ≈
last ~720 candles regardless of `since`; Coinbase deep history needs cursor paging) —
`data/meta/coverage.json` shows exactly what landed.

## Bootstrapping a fresh dataset

```bash
cda-build-dataset                               # uses [dataset] params from config/ingest.toml
# or: python -m scripts.build_dataset --trades-minutes 30 --reference-days 5
```

Runs the whole pipeline (seed `symbol_map` → backfill flagship trades from every
listing spot venue → backfill reference klines + perp funding → build per-venue 1s bars
+ the consolidated tape → coverage → quality → `data/meta/dataset_manifest.json`). Every
network step is isolated; a venue/symbol failure logs and is skipped.

## Schema changes

`init_db()` runs forward-only `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migrations on
every connection, bumps `schema_version`, and re-registers the parquet views (which
NULL-fill new columns on old part files via `union_by_name`). To add a column to a
Parquet table: add it to the relevant schema in `cryptodata/storage/schemas.py`
(nullable for compatibility), have the producer populate it, add it to the relevant
query SELECT and the resampler's aggregation map if it's on `bars_1s`.

## Incident playbook

| Symptom | Likely cause | Action |
|---|---|---|
| `cda-status` shows a venue feed DOWN | venue WS outage / rate-limit / our network | the worker is already retrying with backoff; check the venue's status page; the reconciler will gap-fill trades hourly; the consolidated tape excludes DOWN venues automatically |
| `cda-validate` exits non-zero | a slice has a CRITICAL issue (impossible OHLC, crossed quote, non-positive price) | inspect `data/meta/quality/<date>.json`; isolate the bad `(symbol, venue, date)`; if it's bad source data, record a `bad_range` correction and consider re-backfilling; if it's a derivation bug, fix and `cda-build-bars` the affected day |
| `agg.outside_venue_range` flagged | a bug in the consolidated roll-up, or a per-venue input that's itself corrupt | check the per-second diagnostics; verify the contributing per-venue bars; rebuild |
| Query latency spike | per-call DuckDB connect + view re-registration + Parquet footer reads over a slow filesystem | run on local NVMe, not a network mount; warm-connection reuse is on the roadmap |
| Disk filling | uncompacted hourly parts | run `cda-compact`; check it's scheduled |

## Configuration

All knobs live in `config/`:
- `venues.toml` — WS/REST endpoints and per-venue REST rate limits (set to ≈70% of published).
- `symbols.toml` — the canonical↔native ticker map per venue (the seed for `symbol_map`).
- `ingest.toml` — which streams to enable per venue; writer batching; health thresholds; metrics port; consolidated-tape parameters (`mad_k`, `min_venues_for_filter`, `stale_recv_lag_ms`); `[dataset]` params for `cda-build-dataset`.
