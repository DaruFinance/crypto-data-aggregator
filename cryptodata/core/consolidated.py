"""Cross-venue consolidated tape.

This is the corrected v1 aggregation path. The original v0 built the ``agg`` series
by taking a *volume-weighted average of per-venue 1-second bars* — which is wrong:
a volume-weighted "open" is not an open, and bar-level blending throws away the
microstructure you need to filter bad prints.

Here we instead build a **consolidated trade stream** the way a real consolidated
tape works:

1. k-way merge the per-venue trade streams on ``(ts_ns, recv_ns)``  — i.e. order by
   exchange timestamp, breaking ties by local receive time;
2. per second, apply a *robust* outlier filter — drop trades whose price deviates
   from the cross-venue median by more than ``mad_k`` MADs (median absolute
   deviation, scaled to be a consistent estimator of σ). The filter only engages
   when at least ``min_venues_for_filter`` venues contributed in that second, so a
   thin book never gets a print silently dropped;
3. roll the surviving trades into a single 1-second OHLCV bar where ``open`` is the
   first surviving trade, ``close`` the last, ``vwap`` the size-weighted mean, and
   ``sources_mask`` a bitmask of the venues that survived the filter.

Optionally, venues flagged DOWN by the health tracker (or whose receive lag exceeds
``stale_recv_lag_ms``) are excluded entirely — the same set of guards the docstrings
have always advertised, now actually wired up.

The output bars match ``BARS_1S_SCHEMA`` with ``venue='agg'`` and a ``median_px``
audit value carried in the ``vwap``-adjacent provenance (we keep ``median_px`` out
of the public bar schema; it lives in the per-second diagnostics returned by
:func:`build_agg_bars` when ``return_diagnostics=True``).
"""
from __future__ import annotations

import heapq
import math
import statistics
import time
from collections.abc import Iterable, Iterator, Sequence

from cryptodata.core.symbols import venue_bitmask_index

# Scale factor turning MAD into a consistent estimator of the standard deviation
# for normally-distributed data: 1 / Φ⁻¹(3/4) ≈ 1.4826.
_MAD_TO_SIGMA = 1.4826


def merge_trades(per_venue: dict[str, Sequence[dict]]) -> list[dict]:
    """k-way merge per-venue trade lists into one stream ordered by ``(ts_ns, recv_ns, venue)``.

    Each input list must already be sorted by ``ts_ns``. Each trade dict must carry
    ``ts_ns``, ``recv_ns`` (falls back to ``ts_ns``), ``price``, ``size`` and ``venue``.
    Returns a new list; inputs are not mutated.
    """
    iterators: list[Iterator[dict]] = [iter(rows) for rows in per_venue.values()]
    # heapq needs a total order; key on (ts_ns, recv_ns, venue, monotonic counter).
    out: list[dict] = []

    def _key(t: dict) -> tuple:
        return (t["ts_ns"], t.get("recv_ns", t["ts_ns"]), t.get("venue", ""))

    heap: list[tuple] = []
    for i, it in enumerate(iterators):
        first = next(it, None)
        if first is not None:
            heapq.heappush(heap, (_key(first), i, first))
    while heap:
        _, i, t = heapq.heappop(heap)
        out.append(t)
        nxt = next(iterators[i], None)
        if nxt is not None:
            heapq.heappush(heap, (_key(nxt), i, nxt))
    return out


def _robust_filter(trades: list[dict], mad_k: float, min_venues: int) -> tuple[list[dict], float]:
    """Return (surviving trades, median price) for a single second's worth of trades."""
    if not trades:
        return [], math.nan
    prices = [t["price"] for t in trades]
    med = statistics.median(prices)
    venues_present = {t.get("venue") for t in trades}
    if len(venues_present) < min_venues:
        return trades, med
    abs_dev = [abs(p - med) for p in prices]
    mad = statistics.median(abs_dev)
    if mad <= 0:
        # Degenerate spread (all prices equal, or a single dominant price). Fall back
        # to a relative-deviation guard so a 10x fat-finger still gets caught.
        rel_thresh = max(med * 1e-3, 1e-12)
        return [t for t in trades if abs(t["price"] - med) <= rel_thresh], med
    sigma = mad * _MAD_TO_SIGMA
    thresh = mad_k * sigma
    return [t for t in trades if abs(t["price"] - med) <= thresh], med


def build_agg_bars(
    consolidated: Iterable[dict],
    *,
    symbol: str,
    venues_index: dict[str, int] | None = None,
    mad_k: float = 5.0,
    min_venues_for_filter: int = 3,
    excluded_venues: Iterable[str] = (),
    stale_recv_lag_ms: float | None = None,
    return_diagnostics: bool = False,
) -> list[dict] | tuple[list[dict], list[dict]]:
    """Build cross-venue ``agg`` 1-second bars from a consolidated trade stream.

    Args:
        consolidated: trades ordered by ts (e.g. the output of :func:`merge_trades`).
        symbol: canonical symbol stamped on the output bars.
        venues_index: stable venue→bit map for ``sources_mask`` (pass one built from
            the *global* venue list so masks are comparable across days).
        mad_k: outlier threshold in MAD-derived sigmas.
        min_venues_for_filter: don't filter a second that has fewer than this many venues.
        excluded_venues: venue names to drop entirely (e.g. health-flagged DOWN feeds).
        stale_recv_lag_ms: if set, drop trades whose ``recv_ns - ts_ns`` exceeds this.
        return_diagnostics: also return a list of per-second diagnostics dicts.

    Returns:
        ``list[bar]`` (or ``(list[bar], list[diag])`` when ``return_diagnostics``).
    """
    excluded = set(excluded_venues)
    stale_lag_ns = int(stale_recv_lag_ms * 1_000_000) if stale_recv_lag_ms else None
    now_ns = time.time_ns()

    # Bucket trades by second.
    by_sec: dict[int, list[dict]] = {}
    venues_seen: set[str] = set()
    for t in consolidated:
        v = t.get("venue", "")
        if v in excluded:
            continue
        if stale_lag_ns is not None:
            lag = t.get("recv_ns", t["ts_ns"]) - t["ts_ns"]
            # Negative lag (clock skew where exchange is ahead of us) is allowed; only
            # gate positive lag beyond the budget.
            if lag > stale_lag_ns:
                continue
        if t.get("size", 0.0) <= 0:
            continue
        sec = (t["ts_ns"] // 1_000_000_000) * 1_000_000_000
        by_sec.setdefault(sec, []).append(t)
        venues_seen.add(v)

    if venues_index is None:
        venues_index = venue_bitmask_index(venues_seen)

    bars: list[dict] = []
    diags: list[dict] = []
    for sec in sorted(by_sec):
        raw = by_sec[sec]
        kept, med = _robust_filter(raw, mad_k=mad_k, min_venues=min_venues_for_filter)
        if not kept:
            if return_diagnostics:
                diags.append({"ts_ns": sec, "symbol": symbol, "n_raw": len(raw), "n_kept": 0,
                              "n_dropped": len(raw), "median_px": med, "venues": sorted({t.get("venue") for t in raw})})
            continue
        total_v = sum(t["size"] for t in kept)
        if total_v <= 0:
            continue
        prices = [t["price"] for t in kept]
        vwap = sum(t["price"] * t["size"] for t in kept) / total_v
        mask = 0
        kept_venues = sorted({t.get("venue", "") for t in kept})
        for v in kept_venues:
            mask |= venues_index.get(v, 0)
        bars.append({
            "ts_ns": sec,
            "symbol": symbol,
            "venue": "agg",
            "open": float(kept[0]["price"]),
            "high": float(max(prices)),
            "low": float(min(prices)),
            "close": float(kept[-1]["price"]),
            "volume": float(total_v),
            "vwap": float(vwap),
            "trades": int(len(kept)),
            "sources_mask": int(mask),
            "ingested_at_ns": int(now_ns),
        })
        if return_diagnostics:
            diags.append({"ts_ns": sec, "symbol": symbol, "n_raw": len(raw), "n_kept": len(kept),
                          "n_dropped": len(raw) - len(kept), "median_px": float(med),
                          "venues": kept_venues})

    if return_diagnostics:
        return bars, diags
    return bars
