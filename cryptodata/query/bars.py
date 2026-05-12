"""User-facing get_bars — the surface every backtest hits."""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from cryptodata.core.resample import resample_bars
from cryptodata.storage.duckdb_views import connect

_BAR_COLS = ["ts_ns", "symbol", "venue", "open", "high", "low", "close", "volume", "vwap", "trades", "sources_mask", "ingested_at_ns"]


def _to_ns(t: str | pd.Timestamp | int) -> int:
    if isinstance(t, int):
        return t
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value)   # nanoseconds


def get_bars(
    symbol: str,
    start,
    end,
    timeframe: str = "1m",
    sources: Iterable[str] | None = None,
    fields: list[str] | None = None,
    fill: str = "none",
    tz: str = "UTC",
    asof=None,
) -> pd.DataFrame:
    """Return aggregated OHLCV bars for `symbol` over [start, end) at `timeframe`.

    Args:
        symbol: canonical (e.g. 'BTC-USDT').
        start, end: anything pd.Timestamp accepts; naive timestamps are treated as UTC.
        timeframe: '1s', '1m', '5m', '15m', '1h', '4h', '1d', etc.
        sources: list of venue names; default is ['agg'] (the cross-venue series).
                 Pass ['binance'] to get a single-venue series. Pass ['binance','coinbase']
                 to receive a long DataFrame with both venues stacked.
        fields: columns to return. Default is all bar columns.
        fill: 'none' (default), 'ffill', or 'zero' — applied AFTER resampling.
        tz: timezone for the returned index.
        asof: optional point-in-time cutoff (timestamp). Only bar rows whose
              ``ingested_at_ns`` is <= this instant are returned, so you can
              reproduce exactly what a query at that moment would have seen.
              (Rows written before the ``ingested_at_ns`` column existed are
              treated as always-visible.)
    """
    start_ns = _to_ns(start)
    end_ns = _to_ns(end)
    venues = list(sources) if sources else ["agg"]
    placeholders = ",".join("?" for _ in venues)
    params: list = [symbol, *venues, start_ns, end_ns]
    asof_clause = ""
    if asof is not None:
        asof_clause = "AND (ingested_at_ns IS NULL OR ingested_at_ns <= ?)"
        params.append(_to_ns(asof))

    with connect() as con:
        sql = f"""
            SELECT {", ".join(_BAR_COLS)}
            FROM bars_1s
            WHERE symbol = ?
              AND venue IN ({placeholders})
              AND ts_ns >= ?
              AND ts_ns < ?
              {asof_clause}
            ORDER BY venue, ts_ns
        """
        try:
            df = con.execute(sql, params).fetchdf()
        except Exception:
            df = pd.DataFrame(columns=_BAR_COLS)

    if df.empty:
        return df.set_index(pd.DatetimeIndex([], tz=tz)).rename_axis("ts")

    # Resample per-venue, then concat
    out_frames = []
    for venue, group in df.groupby("venue", sort=False):
        idx = pd.to_datetime(group["ts_ns"], unit="ns", utc=True)
        g = group.drop(columns=["ts_ns", "symbol", "venue"]).set_index(idx)
        rs = resample_bars(g, timeframe)
        rs["venue"] = venue
        rs["symbol"] = symbol
        out_frames.append(rs)
    out = pd.concat(out_frames).sort_index()

    if fill == "ffill":
        out[["open", "high", "low", "close", "vwap"]] = out[["open", "high", "low", "close", "vwap"]].ffill()
        out[["volume", "trades"]] = out[["volume", "trades"]].fillna(0)
    elif fill == "zero":
        out = out.fillna(0)

    if tz != "UTC":
        out.index = out.index.tz_convert(tz)
    out.index.name = "ts"

    if fields:
        keep = [c for c in fields if c in out.columns]
        if "venue" not in keep and len(venues) > 1:
            keep.append("venue")
        out = out[keep]

    return out
