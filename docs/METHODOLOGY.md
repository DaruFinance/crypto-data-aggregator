# Methodology — the consolidated tape and the data-quality model

This document specifies how the cross-venue **consolidated tape** (`venue='agg'` in
`bars_1s`) is constructed, how the data-quality scorecard is computed, and what the
known limitations are. It is the reference a consumer should read before trusting an
`agg` price.

## 1. Inputs

The tape is built **only from raw trades** of the **spot** venues
(`binance, coinbase, kraken, okx, bitstamp, gemini, bitfinex`). Perpetual-futures
venues (`binance_futures, bybit`) contribute funding and open interest only and are
**never** mixed into the spot consolidated price, even when they list the "same"
underlying — a perp's mark price is a different instrument with its own basis.

A trade is the canonical normalized row: `(ts_ns, recv_ns, symbol, venue, price, size,
side, trade_id, ingested_at_ns)`, with `ts_ns` the venue's exchange timestamp in UTC
nanoseconds and `recv_ns` our local receive time.

We deliberately do **not** consolidate at the bar level (a volume-weighted average of
per-venue 1-second bars). That throws away the microstructure needed to filter bad
prints and produces a meaningless `open`/`close`. The v0 prototype did this; the
current implementation does not.

## 2. Construction (`cryptodata.core.consolidated`)

For a given `(symbol, day)`:

1. **k-way merge** the per-venue trade streams into one stream ordered by
   `(ts_ns, recv_ns, venue)` — i.e. exchange-timestamp order, ties broken by local
   receive time then venue name for determinism. This is the consolidated tape.

2. **Exclusions, applied per trade before bucketing:**
   - venues flagged DOWN by the live health tracker (a 5-minute message rate below 10%
     of the trailing-hour baseline) — `excluded_venues`;
   - trades whose receive lag `recv_ns − ts_ns` exceeds `stale_recv_lag_ms` (default
     5 000 ms) — a clock-skew / buffered-feed guard; *negative* lag (exchange clock
     ahead of ours) is allowed and not gated;
   - trades with non-positive size.

3. **Per-second robust outlier filter.** Within each 1-second bucket, compute the
   cross-venue **median** price and the **median absolute deviation** (MAD). Scale MAD
   by 1.4826 to get a consistent σ estimate; drop any trade whose price deviates from
   the median by more than `mad_k · σ` (default `mad_k = 5`). The filter **abstains**
   when fewer than `min_venues_for_filter` venues (default 3) contributed in that
   second, so a thin book is never silently halved. If the MAD is degenerate (all
   prices equal / one dominant price), fall back to a relative-deviation guard
   (`|p − median| ≤ max(median·1e-3, 1e-12)`) so a 10× fat-finger is still caught.

4. **Roll up the survivors** into one 1-second OHLCV bar:
   - `open` = first surviving trade's price, `close` = last surviving trade's price
     (real time-ordered open/close, not a weighted average);
   - `high`/`low` = max/min over surviving prices;
   - `vwap` = size-weighted mean of surviving prices;
   - `volume` = Σ surviving sizes; `trades` = count of surviving trades;
   - `sources_mask` = bitwise-OR of the venue bits (per a **stable global** venue→bit
     index — `venue_bitmask_index(all_sources())` — so masks are comparable across
     days regardless of which venues happened to trade) for venues that survived the
     filter;
   - `ingested_at_ns` = build time, so the row participates in point-in-time reads.
   Per-second diagnostics (`n_raw`, `n_kept`, `n_dropped`, `median_px`, contributing
   venues) are available from `build_agg_bars(..., return_diagnostics=True)` and are
   summarized into the `corrections` log when a build runs.

Higher timeframes (`get_bars(..., "1m"|"5m"|"1h"|…)`) are resampled **left-closed /
left-labeled** from the 1-second bars, with `vwap` carried as a volume-weighted
numerator so it stays correct under aggregation.

## 3. Data-quality scorecard (`cryptodata.quality`)

Every `(symbol, venue, date)` slice gets a 0–100 score and an A–F grade, persisted to
the `daily_quality` DuckDB table and mirrored to `data/meta/quality/<date>.json`.

- **Base.** The score grades *how well we did our job*, not *how active the market was*:
  - **Single-venue series:** `base = 100`. The series captures a bar for every second
    the venue actually traded — we can't do better than the venue's own activity, so a
    thin venue isn't a defect. Its raw bar density is reported as `completeness_pct` /
    `day_coverage_pct` (informational), and only an *anomalous* gap (see below) — a feed
    outage relative to the venue's own cadence — costs points.
  - **Consolidated `agg` series:** `base = agg_coverage_of_union_pct` — the fraction of
    the seconds where *any* contributing spot venue had a bar that the consolidated tape
    also has a bar for. A correctly-built tape captures ≈100% of that union (the only
    "misses" are seconds where every trade was a filtered outlier, which is correct to
    drop). This is the metric that actually grades the consolidation; it deliberately
    does **not** punish the tape for, say, the USD spot market being thinner than the
    USDT one over a short window — the agg's raw density is still reported as
    `completeness_pct`, but as a market-activity metric, not a quality demerit. (If
    there's no per-venue context to compare against, `base = 100` — the tape can't be
    held to a standard we can't measure.)
- **Penalties.** Each distinct issue subtracts a weight: `info 0`, `minor 2`,
  `major 10`, `critical 30`. Score is clamped to `[0, 100]`. Grades: A ≥ 95, B ≥ 85,
  C ≥ 70, D ≥ 50, F < 50.
- **Checks** (each emits zero or more issues with a severity):
  - **Bars** — OHLC invariants (`high ≥ max(o,c)`, `low ≤ min(o,c)`, `high ≥ low`, all
    prices > 0, `vwap ∈ [low, high]`), non-negative volume; monotonic / non-duplicate
    / on-second-grid timestamps; **liquidity-aware gaps** — a gap is flagged only if it
    exceeds both an absolute floor (60 s warn / 600 s major) **and** a relative one
    (≈30× / ≈120× the slice's median inter-bar gap), so a 4-minute gap on a venue whose
    median gap is 3.5 minutes is normal but the same gap on a 1-second-cadence venue is
    a feed outage; flatlines (a long run of identical consecutive closes).
  - **Trades** — non-positive price/size, bad `side`, out-of-order timestamps on disk,
    duplicate `trade_id` within the slice (double-ingest), extreme prints (>50% from the
    slice median), high receive lag (>5 s, with p99).
  - **Quotes** — crossed book (`ask < bid`), locked book (info), non-positive prices,
    implausible spread (>500 bps).
  - **Cross-venue** — the consolidated `close` at second *t* must lie within
    `[min, max]` of the contributing venues' closes at *t* (`agg.outside_venue_range`);
    persistent large cross-venue close dispersion (>50 bps, p99) is flagged
    (`xvenue.dispersion`).

`cda-validate` runs the suite over the whole dataset and exits non-zero if any slice
has a CRITICAL issue (configurable lower with `--fail-on`), so CI can gate on it.

## 4. Known limitations (read these)

- **Timestamp fidelity varies by venue.** Several feeds don't carry an exchange
  timestamp on every channel: Binance `bookTicker` quotes use `recv_ns − 1 ms` as a
  proxy; Binance partial-book and Coinbase/Kraken rebuilt L2 snapshots use `recv_ns`;
  Gemini v2 l2-derived quotes use `recv_ns`. Trade timestamps *are* exchange-supplied
  on every venue. None of this is hardware-timestamped. This is fine for second-level
  bars and acceptable for L2-snapshot research; it is documented per-adapter and
  surfaced via `recv_ns`/`ingested_at_ns` so a consumer can reason about it.
- **No clock-skew estimation yet.** We trust each venue's `ts_ns`. A per-venue
  skew/drift estimator (using `recv_ns` and cross-venue agreement) is on the roadmap;
  until then the `stale_recv_lag_ms` guard and the cross-venue dispersion check are the
  only skew defences.
- **Symbol-map ambiguity.** A few venues use the same native ticker for spot and a
  derivative (e.g. Bybit's `BTCUSDT` is the linear perp; v1 only ingests Bybit perps,
  so this is currently unambiguous, but a future spot-Bybit ingest would need a
  `(venue, market, native)` key, not `(venue, native)`).
- **L2 ingest is Binance + Coinbase only in v1**, top-20-levels every 5 s for the four
  USDT/USD majors; the schema and query API support more.
- **REST backfill depth differs by venue.** Bitstamp's public trade endpoint only
  exposes a rolling 24 h window; Kraken's OHLC endpoint returns ~720 most-recent
  candles regardless of `since`; Coinbase deep history needs cursor pagination. The
  backfillers return whatever the venue exposes inside the requested range, so coverage
  is uneven for older windows — see `data/meta/coverage.{json,md}`.
- **Reference klines (`bars_ref`) are not cross-checked against the tape yet.** A CI
  gate that flags large `agg`-vs-`bars_ref` divergence is on the roadmap.
