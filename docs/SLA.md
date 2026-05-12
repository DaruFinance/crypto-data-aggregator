# Coverage & quality targets

These are the targets the dataset is *built and validated against*. They are not a
contractual guarantee — they're the bar `cda-validate` and the coverage matrix are
designed to hold the pipeline to, and the thing reviewers should check the dataset
against.

## Freshness (live ingest)

| Stream | Target end-to-end lag (exchange ts → on disk) | How it's enforced |
|---|---|---|
| trades, quotes | < 2 s p99 | batched writer flushes on a 60 s wall-clock *or* 50 k-row threshold; `recv_ns` recorded so lag is measurable; Prometheus `cryptodata_recv_lag_ms` |
| L2 snapshots | < 6 s (5 s snapshot interval + write) | snapshot cadence + writer flush |
| funding | within the venue's settlement cadence | event-driven (WS markPrice / tickers) |
| open interest | < 90 s | REST poll every 60 s |

## Completeness

| Series | Target | Metric |
|---|---|---|
| consolidated `agg` 1s tape | ≥ 99.5 % `agg_coverage_of_union_pct` — i.e. capture (essentially) every second any contributing venue traded; "misses" only when all that second's trades were filtered outliers | `daily_quality.score_base_pct` (= `agg_coverage_of_union_pct`) |
| 1s bar density (`agg` and per-venue) | reported, not targeted — `completeness_pct` / `day_coverage_pct` are market-activity metrics, not SLAs (USD spot is thinner than USDT, low-cap pairs are sparse) | `daily_quality.completeness_pct` |
| per-venue 1s bars | no *anomalous* gap (> 60 s **and** > ~30× the venue's own median inter-bar gap) | the liquidity-aware gap check; `daily_quality.max_gap_seconds` |
| per-venue reference klines (`bars_ref`) | every expected bar present for the requested window, subject to the venue's REST history depth | the coverage matrix |
| perp funding | every settlement in the requested window | the coverage matrix |

## Correctness (zero tolerance)

The data-quality suite **fails the build** (`cda-validate` non-zero) on any CRITICAL
issue. CRITICAL = a value that is provably impossible from the data alone:

- `high < low` in a bar; price ≤ 0 in a bar or trade; volume < 0;
- `bid > ask` is MAJOR (a crossed top-of-book happens transiently on real feeds) but a
  non-positive bid/ask is CRITICAL.

MAJOR issues (OHLC body violations, crossed books, duplicate trade ids, extreme prints,
agg-outside-venue-range, large anomalous gaps) cost 10 points each and should be
investigated; MINOR issues (small gaps, off-grid timestamps, high receive lag,
implausible spreads) cost 2; INFO (locked books) is free.

Grades: A ≥ 95 · B ≥ 85 · C ≥ 70 · D ≥ 50 · F < 50. The score grades *correctness and
completeness-of-job* (invariants, cross-venue consistency, feed continuity, and for the
`agg` tape its coverage of the contributing venues) — not market activity. Target: every
slice ≥ B; the consolidated `agg` tape ≥ A.

## Reference data & lineage

- `symbol_map` is point-in-time: ticker renames/delistings close out an interval; a
  query as of any past instant returns the mapping that was in force then.
- Every backfill, rebuild, restatement and known-bad-range is in the `corrections` log
  with both valid time and event time, so any figure in the dataset is traceable to
  when and how it was produced.
- Every Parquet row carries `ingested_at_ns`; `get_bars(..., asof=t)` reproduces what a
  query at instant `t` would have returned (no silent restatement).

## What is explicitly out of scope in v1

Hardware-timestamped feeds; clock-skew estimation per venue; L2 ingest beyond Binance +
Coinbase; dated futures / options; deep historical backfill where the venue's REST API
doesn't expose it (Bitstamp trades, Kraken OHLC depth). See `docs/METHODOLOGY.md` §4.
