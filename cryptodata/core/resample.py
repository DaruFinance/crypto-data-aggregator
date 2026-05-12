"""Resample per-second OHLCV bars to arbitrary higher timeframes.

Convention: left-closed / left-labeled. The 09:30:00 bar covers [09:30:00, 09:31:00)
and is causally available at 09:31:00.

Supported timeframes (string forms): 1s, 5s, 10s, 15s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w.
The implementation accepts any pandas frequency string; the list above is what we test.
"""
from __future__ import annotations

import pandas as pd

_AGG = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
    "vwap": "mean",        # placeholder; we replace with volume-weighted below
    "trades": "sum",
    "sources_mask": "max",      # union-like: max bitmask seen in the window
    "ingested_at_ns": "max",    # most recent write that contributed to this bar
}


def resample_bars(bars_1s: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample `bars_1s` to `timeframe`. Input must be indexed by tz-aware UTC timestamp
    (DatetimeIndex) and contain the canonical bar columns.

    Returns a DataFrame indexed by left-labeled bar timestamp.
    """
    if bars_1s.empty:
        return bars_1s.copy()
    if not isinstance(bars_1s.index, pd.DatetimeIndex):
        raise TypeError("resample_bars expects a DatetimeIndex")
    if timeframe in ("1s", "1S"):
        return bars_1s.copy()
    # Compute true volume-weighted vwap by carrying numerator separately.
    df = bars_1s.copy()
    df["_pxv"] = df["vwap"] * df["volume"]
    rule = _pandas_rule(timeframe)
    agg = {k: v for k, v in _AGG.items() if k in df.columns}
    rs = df.resample(rule, label="left", closed="left")
    out = rs.agg({**agg, "_pxv": "sum"})
    out["vwap"] = (out["_pxv"] / out["volume"]).where(out["volume"] > 0, out["close"])
    out = out.drop(columns=["_pxv"])
    out = out.dropna(subset=["close"])    # drop empty windows
    out["trades"] = out["trades"].astype("int64")
    if "sources_mask" in out.columns:
        out["sources_mask"] = out["sources_mask"].fillna(0).astype("int64")
    if "ingested_at_ns" in out.columns:
        out["ingested_at_ns"] = out["ingested_at_ns"].astype("Int64")
    return out


def _pandas_rule(timeframe: str) -> str:
    """Map our short forms to pandas frequency strings.

    pandas 2.x deprecates uppercase aliases for 'min'; we normalize to lowercase.
    """
    tf = timeframe.strip().lower()
    table = {
        "1s": "1s", "5s": "5s", "10s": "10s", "15s": "15s", "30s": "30s",
        "1m": "1min", "2m": "2min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
        "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h",
        "1d": "1D", "1w": "1W",
    }
    return table.get(tf, tf)
