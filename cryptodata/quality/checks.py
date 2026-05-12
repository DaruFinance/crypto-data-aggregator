"""Invariant and consistency checks over the dataset.

Each check is a small pure function that takes pandas DataFrames (already loaded by
the report layer) and yields :class:`Issue` records. Severity scale:

    info     — worth noting, no points lost
    minor    — small blemish (a handful of out-of-order ticks, etc.)
    major    — material problem (large gaps, crossed quotes, OHLC violations)
    critical — the data for this slice should not be trusted as-is

The checks are deliberately conservative — they only flag what's provably wrong from
the data itself (no model of "expected" prices), so a green scorecard means
something.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import IntEnum

import numpy as np
import pandas as pd


class Severity(IntEnum):
    INFO = 0
    MINOR = 1
    MAJOR = 2
    CRITICAL = 3

    @property
    def label(self) -> str:
        return self.name.lower()


@dataclass(slots=True)
class Issue:
    check: str
    severity: Severity
    count: int
    detail: str
    sample: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "check": self.check,
            "severity": self.severity.label,
            "count": int(self.count),
            "detail": self.detail,
            "sample": self.sample[:5],
        }


# --------------------------------------------------------------------------- #
# bar checks
# --------------------------------------------------------------------------- #

def check_bar_ohlc_invariants(bars: pd.DataFrame) -> Iterable[Issue]:
    if bars.empty:
        return
    hi_ok = bars["high"] >= bars[["open", "close"]].max(axis=1) - 1e-9
    lo_ok = bars["low"] <= bars[["open", "close"]].min(axis=1) + 1e-9
    hl_ok = bars["high"] >= bars["low"] - 1e-9
    bad_hi = (~hi_ok).sum()
    bad_lo = (~lo_ok).sum()
    bad_hl = (~hl_ok).sum()
    if bad_hi:
        yield Issue("bar.high_below_body", Severity.MAJOR, int(bad_hi),
                    "high < max(open, close) — impossible OHLC")
    if bad_lo:
        yield Issue("bar.low_above_body", Severity.MAJOR, int(bad_lo),
                    "low > min(open, close) — impossible OHLC")
    if bad_hl:
        yield Issue("bar.high_below_low", Severity.CRITICAL, int(bad_hl),
                    "high < low — impossible OHLC")
    neg_v = (bars["volume"] < 0).sum()
    if neg_v:
        yield Issue("bar.negative_volume", Severity.CRITICAL, int(neg_v), "volume < 0")
    bad_vwap = ((bars["vwap"] < bars["low"] - 1e-6) | (bars["vwap"] > bars["high"] + 1e-6)).sum()
    if bad_vwap:
        yield Issue("bar.vwap_outside_hl", Severity.MAJOR, int(bad_vwap),
                    "vwap outside [low, high] — bad weighting or corrupt bar")
    nonpos_px = ((bars[["open", "high", "low", "close"]] <= 0).any(axis=1)).sum()
    if nonpos_px:
        yield Issue("bar.nonpositive_price", Severity.CRITICAL, int(nonpos_px), "price <= 0 in a bar")


def check_bar_timestamps(bars: pd.DataFrame) -> Iterable[Issue]:
    if bars.empty:
        return
    ts = bars["ts_ns"].to_numpy()
    if len(ts) < 2:
        return
    diffs = np.diff(ts)
    n_nonmono = int((diffs < 0).sum())
    n_dups = int((diffs == 0).sum())
    if n_nonmono:
        yield Issue("bar.timestamps_out_of_order", Severity.MAJOR, n_nonmono,
                    "bar ts_ns not monotonically increasing")
    if n_dups:
        yield Issue("bar.duplicate_timestamps", Severity.MAJOR, n_dups,
                    "two bars share the same ts_ns")
    # 1-second bars: nominal step is 1e9 ns. Anything bigger is a within-day gap.
    off_grid = int((ts % 1_000_000_000 != 0).sum())
    if off_grid:
        yield Issue("bar.timestamp_off_second_grid", Severity.MINOR, off_grid,
                    "bar ts_ns not aligned to a whole second")


def check_bar_gaps(bars: pd.DataFrame, *, warn_gap_s: int = 60, major_gap_s: int = 600,
                   rel_warn_k: float = 30.0, rel_major_k: float = 120.0) -> Iterable[Issue]:
    """Gaps between consecutive 1s bars, *liquidity-aware*.

    Crypto trades 24/7, so a thin venue legitimately has many seconds with no trade
    (hence no bar). A gap is only suspicious if it's large in *absolute* terms **and**
    large relative to the venue's typical inter-bar interval — a 5-minute gap on a
    venue whose median gap is 4 minutes is normal; the same gap on a venue whose median
    gap is 1 second is a feed outage. We flag a gap when it exceeds
    ``max(warn_gap_s, rel_warn_k × median_gap)`` (MINOR) or
    ``max(major_gap_s, rel_major_k × median_gap)`` (MAJOR). Pure sparsity is reported
    via ``completeness_pct`` on the scorecard, not here.
    """
    if len(bars) < 2:
        return
    ts = np.sort(bars["ts_ns"].to_numpy())
    gaps_s = np.diff(ts) / 1e9
    real = gaps_s[gaps_s > 1.5]   # > 1 nominal second
    if real.size == 0:
        return
    median_gap = float(np.median(gaps_s))
    warn_th = max(float(warn_gap_s), rel_warn_k * median_gap)
    major_th = max(float(major_gap_s), rel_major_k * median_gap)
    n_warn = int((real >= warn_th).sum())
    n_major = int((real >= major_th).sum())
    if not n_warn:
        return   # sparse but not anomalous
    longest = float(real.max())
    sev = Severity.MAJOR if n_major else Severity.MINOR
    yield Issue("bar.gaps", sev, n_warn,
                f"{n_warn} anomalous gap(s) between 1s bars (median gap {median_gap:.1f}s, "
                f"longest {longest:.0f}s; {n_major} >= {major_th:.0f}s)",
                sample=[round(float(x), 1) for x in np.sort(real)[::-1][:5]])


def check_flatline(bars: pd.DataFrame, *, min_run: int = 120) -> Iterable[Issue]:
    """A long run of bars with identical close *and* zero or constant volume usually
    means a stalled feed forward-filled into the bar builder."""
    if len(bars) < min_run:
        return
    close = bars.sort_values("ts_ns")["close"].to_numpy()
    # length of the longest run of equal consecutive closes
    if close.size == 0:
        return
    change = np.concatenate(([True], close[1:] != close[:-1]))
    run_ids = np.cumsum(change)
    counts = np.bincount(run_ids)
    longest = int(counts.max()) if counts.size else 0
    if longest >= min_run:
        yield Issue("bar.flatline", Severity.MINOR, longest,
                    f"longest run of identical consecutive closes is {longest} bars (>= {min_run})")


# --------------------------------------------------------------------------- #
# trade checks
# --------------------------------------------------------------------------- #

def check_trade_invariants(trades: pd.DataFrame) -> Iterable[Issue]:
    if trades.empty:
        return
    nonpos_px = int((trades["price"] <= 0).sum())
    nonpos_sz = int((trades["size"] <= 0).sum())
    if nonpos_px:
        yield Issue("trade.nonpositive_price", Severity.CRITICAL, nonpos_px, "trade price <= 0")
    if nonpos_sz:
        yield Issue("trade.nonpositive_size", Severity.MAJOR, nonpos_sz, "trade size <= 0")
    bad_side = int((~trades["side"].isin([-1, 0, 1])).sum())
    if bad_side:
        yield Issue("trade.bad_side", Severity.MINOR, bad_side, "side not in {-1, 0, +1}")
    ts = trades["ts_ns"].to_numpy()
    if len(ts) >= 2:
        n_nonmono = int((np.diff(ts) < 0).sum())
        if n_nonmono:
            yield Issue("trade.timestamps_out_of_order", Severity.MINOR, n_nonmono,
                        "trades within a (symbol, venue) slice not in ts order on disk")
    if "trade_id" in trades.columns:
        ids = trades["trade_id"].dropna()
        ids = ids[ids != ""]
        if len(ids):
            n_dup = int(len(ids) - ids.nunique())
            if n_dup:
                yield Issue("trade.duplicate_trade_id", Severity.MAJOR, n_dup,
                            "repeated venue trade_id within the slice (likely double-ingest)")
    # huge single-tick jumps (relative to a rolling median) — fat-finger / bad print
    px = trades.sort_values("ts_ns")["price"].to_numpy()
    if px.size >= 20:
        med = np.median(px)
        if med > 0:
            rel = np.abs(px - med) / med
            n_wild = int((rel > 0.5).sum())   # >50% off the day's median
            if n_wild:
                yield Issue("trade.extreme_price", Severity.MAJOR, n_wild,
                            ">50% deviation from the slice median price")
    # receive lag: recv_ns far behind ts_ns means a slow/buffered feed
    if "recv_ns" in trades.columns:
        lag_ms = (trades["recv_ns"].to_numpy() - trades["ts_ns"].to_numpy()) / 1e6
        n_stale = int((lag_ms > 5000).sum())
        if n_stale:
            p99 = float(np.percentile(lag_ms, 99))
            yield Issue("trade.high_receive_lag", Severity.MINOR, n_stale,
                        f"{n_stale} trades with recv lag > 5s (p99 lag {p99:.0f} ms)")


# --------------------------------------------------------------------------- #
# quote checks
# --------------------------------------------------------------------------- #

def check_quote_invariants(quotes: pd.DataFrame) -> Iterable[Issue]:
    if quotes.empty:
        return
    crossed = int((quotes["ask_px"] < quotes["bid_px"] - 1e-12).sum())
    locked = int((quotes["ask_px"] == quotes["bid_px"]).sum())
    nonpos = int(((quotes[["bid_px", "ask_px"]] <= 0).any(axis=1)).sum())
    if crossed:
        yield Issue("quote.crossed_book", Severity.MAJOR, crossed, "ask < bid")
    if locked:
        yield Issue("quote.locked_book", Severity.INFO, locked, "ask == bid (locked)")
    if nonpos:
        yield Issue("quote.nonpositive_price", Severity.CRITICAL, nonpos, "bid or ask <= 0")
    # absurd spreads
    mid = (quotes["bid_px"] + quotes["ask_px"]) / 2.0
    spread_bps = (quotes["ask_px"] - quotes["bid_px"]) / mid.replace(0, np.nan) * 1e4
    n_wide = int((spread_bps > 500).sum())   # > 5%
    if n_wide:
        yield Issue("quote.implausible_spread", Severity.MINOR, n_wide, "spread > 500 bps")


# --------------------------------------------------------------------------- #
# cross-venue consistency
# --------------------------------------------------------------------------- #

def check_agg_within_venue_range(agg_bars: pd.DataFrame, per_venue: dict[str, pd.DataFrame]) -> Iterable[Issue]:
    """The consolidated ('agg') close at second t should sit between the lowest and
    highest per-venue close at t (it's a blend of survivors). A breach means the
    aggregation logic or the inputs are wrong."""
    if agg_bars.empty or len(per_venue) < 2:
        return
    agg = agg_bars.set_index("ts_ns")["close"]
    lo = None
    hi = None
    for df in per_venue.values():
        if df.empty:
            continue
        s = df.set_index("ts_ns")["close"]
        lo = s if lo is None else pd.concat([lo, s], axis=1).min(axis=1)
        hi = s if hi is None else pd.concat([hi, s], axis=1).max(axis=1)
    if lo is None:
        return
    common = agg.index.intersection(lo.index)
    if len(common) == 0:
        return
    a = agg.loc[common]
    breach = ((a < lo.loc[common] - (lo.loc[common].abs() * 1e-6 + 1e-9)) |
              (a > hi.loc[common] + (hi.loc[common].abs() * 1e-6 + 1e-9)))
    n = int(breach.sum())
    if n:
        yield Issue("agg.outside_venue_range", Severity.MAJOR, n,
                    "consolidated close fell outside [min, max] of contributing venue closes")


def check_cross_venue_dispersion(per_venue: dict[str, pd.DataFrame], *, warn_bps: float = 50.0) -> Iterable[Issue]:
    """How far apart do venues quote the same asset at the same second? Persistent
    large dispersion is either an arbitrage truth (rare) or a stale/bad feed."""
    series = []
    for v, df in per_venue.items():
        if df.empty or v == "agg":
            continue
        series.append(df.set_index("ts_ns")["close"].rename(v))
    if len(series) < 2:
        return
    wide = pd.concat(series, axis=1).dropna(how="all")
    if wide.shape[0] == 0:
        return
    rowmin = wide.min(axis=1)
    rowmax = wide.max(axis=1)
    mid = (rowmin + rowmax) / 2.0
    disp_bps = (rowmax - rowmin) / mid.replace(0, np.nan) * 1e4
    p99 = float(np.nanpercentile(disp_bps, 99)) if disp_bps.notna().any() else 0.0
    n_wide = int((disp_bps > warn_bps).sum())
    if n_wide:
        sev = Severity.MINOR if p99 < 200 else Severity.MAJOR
        yield Issue("xvenue.dispersion", sev, n_wide,
                    f"{n_wide} seconds with cross-venue close dispersion > {warn_bps:.0f} bps (p99 {p99:.0f} bps)")


# --------------------------------------------------------------------------- #
# orchestration
# --------------------------------------------------------------------------- #

def run_checks(
    *,
    bars: pd.DataFrame | None = None,
    trades: pd.DataFrame | None = None,
    quotes: pd.DataFrame | None = None,
    agg_bars: pd.DataFrame | None = None,
    per_venue_bars: dict[str, pd.DataFrame] | None = None,
) -> list[Issue]:
    """Run every applicable check over whatever frames are supplied. Returns a flat
    list of Issues, sorted by severity descending."""
    issues: list[Issue] = []
    if bars is not None and not bars.empty:
        issues += list(check_bar_ohlc_invariants(bars))
        issues += list(check_bar_timestamps(bars))
        issues += list(check_bar_gaps(bars))
        issues += list(check_flatline(bars))
    if trades is not None and not trades.empty:
        issues += list(check_trade_invariants(trades))
    if quotes is not None and not quotes.empty:
        issues += list(check_quote_invariants(quotes))
    if agg_bars is not None and per_venue_bars:
        issues += list(check_agg_within_venue_range(agg_bars, per_venue_bars))
    if per_venue_bars:
        issues += list(check_cross_venue_dispersion(per_venue_bars))
    issues.sort(key=lambda i: (-int(i.severity), i.check))
    return issues
