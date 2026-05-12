"""Per-venue 1-second bar construction, plus a legacy per-second cross-venue combiner.

``bars_from_trades`` is the primitive used everywhere: it rolls a venue's trade
stream into one OHLCV bar per second.

``aggregate_second`` is the *legacy* cross-venue combiner that blends already-built
per-venue 1s bars. It is kept for backwards compatibility and for cheap incremental
use, but the dataset build (and the documented "consolidated tape") now uses the
trade-level path in :mod:`cryptodata.core.consolidated`, which is strictly more
correct (real open/close, robust per-trade outlier filtering). New code should
prefer ``consolidated.build_agg_bars``.

Note: unlike the original v0 docstring, ``aggregate_second`` does *not* itself
consult the health tracker. Health/staleness exclusion is the caller's job — pass
the already-filtered set of per-venue bars, or use ``excluded_venues``.
"""
from __future__ import annotations

import statistics
import time
from collections.abc import Iterable

from cryptodata.core.symbols import venue_bitmask_index

_MAD_TO_SIGMA = 1.4826


def aggregate_second(
    per_venue_bars: Iterable[dict],
    *,
    outlier_sigma: float = 5.0,
    venues_index: dict[str, int] | None = None,
    excluded_venues: Iterable[str] = (),
) -> dict | None:
    """Combine per-venue 1s bars for one (symbol, ts_ns) into a single 'agg' bar (legacy path).

    The combiner:
      - drops venues in ``excluded_venues`` and venues with non-positive volume,
      - applies a robust (median + MAD) outlier filter on the per-venue close — only
        when ≥3 venues remain, so a 2-venue second is never silently halved,
      - computes ``vwap`` and the volume-weighted open/close across survivors,
      - records ``sources_mask`` (bitmask of surviving venues per ``venues_index``).

    Returns the aggregated bar dict, or ``None`` if no venue survives.
    """
    excluded = set(excluded_venues)
    bars = [b for b in per_venue_bars if b.get("volume", 0.0) > 0 and b.get("venue") not in excluded]
    if not bars:
        return None

    if venues_index is None:
        venues_index = venue_bitmask_index(b["venue"] for b in bars)

    closes = [b["close"] for b in bars]
    if len(closes) >= 3:
        med = statistics.median(closes)
        mad = statistics.median([abs(c - med) for c in closes])
        if mad > 0:
            thresh = outlier_sigma * mad * _MAD_TO_SIGMA
            kept = [b for b in bars if abs(b["close"] - med) <= thresh]
        else:
            rel = max(med * 1e-3, 1e-12)
            kept = [b for b in bars if abs(b["close"] - med) <= rel]
    else:
        kept = bars
    if not kept:
        return None

    total_volume = sum(b["volume"] for b in kept)
    if total_volume <= 0:
        return None
    vwap = sum(b["vwap"] * b["volume"] for b in kept) / total_volume
    weighted_open = sum(b["open"] * b["volume"] for b in kept) / total_volume
    weighted_close = sum(b["close"] * b["volume"] for b in kept) / total_volume
    high = max(b["high"] for b in kept)
    low = min(b["low"] for b in kept)
    total_trades = sum(b.get("trades", 0) for b in kept)

    mask = 0
    for b in kept:
        mask |= venues_index.get(b["venue"], 0)

    sample = kept[0]
    return {
        "ts_ns": sample["ts_ns"],
        "symbol": sample["symbol"],
        "venue": "agg",
        "open": float(weighted_open),
        "high": float(high),
        "low": float(low),
        "close": float(weighted_close),
        "volume": float(total_volume),
        "vwap": float(vwap),
        "trades": int(total_trades),
        "sources_mask": int(mask),
        "ingested_at_ns": int(time.time_ns()),
    }


def bars_from_trades(
    trades: Iterable[dict],
    *,
    symbol: str,
    venue: str,
) -> list[dict]:
    """Build per-second OHLCV bars from a stream of trades for one (symbol, venue).

    Trades must be sorted by ts_ns. Returns one bar per second that has trades;
    seconds with no trades are skipped (use a query-side resampler with ffill if
    you need contiguous output).
    """
    bars: list[dict] = []
    current: dict | None = None
    current_sec_ns = -1
    cum_pxv = 0.0
    cum_v = 0.0
    now_ns = time.time_ns()

    def _emit():
        nonlocal current, cum_pxv, cum_v
        if current is None:
            return
        current["vwap"] = cum_pxv / cum_v if cum_v > 0 else current["close"]
        current["volume"] = cum_v
        bars.append(current)

    for t in trades:
        ts_ns = t["ts_ns"]
        price = float(t["price"])
        size = float(t["size"])
        sec_ns = (ts_ns // 1_000_000_000) * 1_000_000_000
        if sec_ns != current_sec_ns:
            _emit()
            current = {
                "ts_ns": sec_ns,
                "symbol": symbol,
                "venue": venue,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
                "vwap": price,
                "trades": 0,
                "sources_mask": 0,
                "ingested_at_ns": now_ns,
            }
            current_sec_ns = sec_ns
            cum_pxv = 0.0
            cum_v = 0.0
        current["high"] = max(current["high"], price)
        current["low"] = min(current["low"], price)
        current["close"] = price
        current["trades"] += 1
        cum_pxv += price * size
        cum_v += size
    _emit()
    return bars
