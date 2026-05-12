"""get_ref_bars — per-venue reference klines (REST-backfilled OHLCV).

Distinct from :func:`cryptodata.get_bars`, which serves our own per-second roll-ups
and the consolidated ``agg`` series. ``bars_ref`` is what the venue published, at the
venue's native granularity — handy as an independent sanity reference and as the
breadth layer for symbols where we don't carry tick data.
"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from cryptodata.storage.duckdb_views import connect

_COLS = ["ts_ns", "symbol", "venue", "interval", "open", "high", "low", "close", "volume", "trades"]


def _to_ns(t) -> int:
    if isinstance(t, int):
        return t
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value)


def get_ref_bars(
    symbol: str,
    start,
    end,
    interval: str = "1m",
    venues: Iterable[str] | str | None = None,
    tz: str = "UTC",
) -> pd.DataFrame:
    """Return reference klines for `symbol` over [start, end) at `interval`.

    Args:
        venues: list of venue names, 'all', or None (defaults to 'all'). Multiple
                venues come back stacked (long format) with a ``venue`` column.
    """
    start_ns, end_ns = _to_ns(start), _to_ns(end)
    params: list = [symbol, interval, start_ns, end_ns]
    venue_clause = ""
    if venues not in (None, "all"):
        vlist = [venues] if isinstance(venues, str) else list(venues)
        venue_clause = f"AND venue IN ({','.join('?' for _ in vlist)})"
        params = [symbol, interval, *vlist, start_ns, end_ns]
    with connect() as con:
        try:
            df = con.execute(
                f"SELECT {', '.join(_COLS)} FROM bars_ref "
                f"WHERE symbol = ? AND interval = ? {venue_clause} AND ts_ns >= ? AND ts_ns < ? "
                f"ORDER BY venue, ts_ns",
                params,
            ).fetchdf()
        except Exception:
            return pd.DataFrame(columns=_COLS)
    if df.empty:
        return df
    idx = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    if tz != "UTC":
        idx = idx.tz_convert(tz)
    df = df.set_index(idx).drop(columns=["ts_ns", "symbol", "interval"])
    df.index.name = "ts"
    return df
