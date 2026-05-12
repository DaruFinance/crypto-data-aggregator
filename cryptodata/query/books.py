"""L2 order-book snapshot queries + a lightweight book-quality summary.

Snapshots are stored as one row per (symbol, venue, snapshot time) with ``bids`` and
``asks`` as ordered lists of {px, sz}. :func:`get_book_snapshots` returns the raw
rows (lists kept as Python lists in an ``object`` column). :func:`book_top` flattens
to a tidy frame of best bid/ask + a few derived microstructure columns. :func:`book_quality`
runs invariant checks (ascending asks, descending bids, no crossed top, monotone
cumulative size) and returns an issue summary.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from cryptodata.storage.duckdb_views import connect


def _to_ns(t) -> int:
    if isinstance(t, int):
        return t
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value)


def get_book_snapshots(
    symbol: str,
    start,
    end,
    venue: str,
    levels: int | None = None,
    tz: str = "UTC",
) -> pd.DataFrame:
    """Return raw L2 snapshots for one (symbol, venue) over [start, end)."""
    start_ns, end_ns = _to_ns(start), _to_ns(end)
    with connect() as con:
        try:
            df = con.execute(
                "SELECT ts_ns, recv_ns, venue, depth, bids, asks FROM book_l2_snapshot "
                "WHERE symbol = ? AND venue = ? AND ts_ns >= ? AND ts_ns < ? ORDER BY ts_ns",
                [symbol, venue, start_ns, end_ns],
            ).fetchdf()
        except Exception:
            return pd.DataFrame(columns=["ts", "venue", "depth", "bids", "asks"])
    if df.empty:
        return df
    if levels is not None:
        df["bids"] = df["bids"].map(lambda lv: list(lv)[:levels])
        df["asks"] = df["asks"].map(lambda lv: list(lv)[:levels])
    idx = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    if tz != "UTC":
        idx = idx.tz_convert(tz)
    df = df.set_index(idx).drop(columns=["ts_ns"])
    df.index.name = "ts"
    return df


def _level_px_sz(level) -> tuple[float, float]:
    """Tolerate dict-shaped ({'px':..,'sz':..}) and tuple-shaped levels."""
    if isinstance(level, dict):
        return float(level.get("px")), float(level.get("sz"))
    return float(level[0]), float(level[1])


def book_top(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Flatten snapshots to best bid/ask + mid + spread (bps) + top-of-book imbalance."""
    if snapshots.empty:
        return pd.DataFrame(columns=["venue", "bid_px", "bid_sz", "ask_px", "ask_sz", "mid", "spread_bps", "imbalance"])
    rows = []
    for ts, r in snapshots.iterrows():
        bids, asks = list(r["bids"]), list(r["asks"])
        if not bids or not asks:
            continue
        bpx, bsz = _level_px_sz(bids[0])
        apx, asz = _level_px_sz(asks[0])
        mid = (bpx + apx) / 2.0
        spread_bps = (apx - bpx) / mid * 1e4 if mid > 0 else np.nan
        imb = (bsz - asz) / (bsz + asz) if (bsz + asz) > 0 else np.nan
        rows.append({"ts": ts, "venue": r["venue"], "bid_px": bpx, "bid_sz": bsz,
                     "ask_px": apx, "ask_sz": asz, "mid": mid, "spread_bps": spread_bps, "imbalance": imb})
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.set_index("ts")
    return out


def book_quality(snapshots: pd.DataFrame) -> dict:
    """Run L2 invariant checks. Returns {n_snapshots, issues:{check:count}, worst:...}."""
    if snapshots.empty:
        return {"n_snapshots": 0, "issues": {}}
    n = len(snapshots)
    counts = {
        "empty_side": 0, "crossed_top": 0, "bids_not_descending": 0,
        "asks_not_ascending": 0, "nonpositive_level": 0, "duplicate_price_level": 0,
    }
    for _, r in snapshots.iterrows():
        bids, asks = list(r["bids"]), list(r["asks"])
        if not bids or not asks:
            counts["empty_side"] += 1
            continue
        bpx = [_level_px_sz(x)[0] for x in bids]
        apx = [_level_px_sz(x)[0] for x in asks]
        bsz = [_level_px_sz(x)[1] for x in bids]
        asz = [_level_px_sz(x)[1] for x in asks]
        if bpx[0] >= apx[0]:
            counts["crossed_top"] += 1
        if any(bpx[i] < bpx[i + 1] for i in range(len(bpx) - 1)):
            counts["bids_not_descending"] += 1
        if any(apx[i] > apx[i + 1] for i in range(len(apx) - 1)):
            counts["asks_not_ascending"] += 1
        if any(p <= 0 for p in bpx + apx) or any(s < 0 for s in bsz + asz):
            counts["nonpositive_level"] += 1
        if len(set(bpx)) != len(bpx) or len(set(apx)) != len(apx):
            counts["duplicate_price_level"] += 1
    issues = {k: v for k, v in counts.items() if v}
    return {"n_snapshots": int(n), "issues": issues,
            "clean_pct": round(100.0 * (1 - sum(issues.values()) / max(n, 1)), 2)}


def list_book_symbols() -> list[tuple[str, str]]:
    """Return [(symbol, venue), ...] that have any L2 snapshot data."""
    with connect() as con:
        try:
            rows = con.execute("SELECT DISTINCT symbol, venue FROM book_l2_snapshot ORDER BY symbol, venue").fetchall()
            return [(r[0], r[1]) for r in rows]
        except Exception:
            return []
