import numpy as np
import pandas as pd

from cryptodata.core.resample import resample_bars


def _sample_1s(n=120):
    idx = pd.date_range("2026-05-12 00:00:00", periods=n, freq="1s", tz="UTC")
    # Synthetic bars where every second has volume 1 and price increasing by 1
    df = pd.DataFrame({
        "open": np.arange(n, dtype=float),
        "high": np.arange(n, dtype=float) + 0.5,
        "low": np.arange(n, dtype=float) - 0.5,
        "close": np.arange(n, dtype=float) + 0.25,
        "volume": np.ones(n, dtype=float),
        "vwap": np.arange(n, dtype=float),
        "trades": np.ones(n, dtype="int64"),
        "sources_mask": np.zeros(n, dtype="int64"),
    }, index=idx)
    return df


def test_left_closed_left_labeled_1m():
    df = _sample_1s(120)
    rs = resample_bars(df, "1m")
    # Two 1-minute windows
    assert len(rs) == 2
    # First bar should be labeled 00:00:00 (left-labeled)
    assert rs.index[0] == pd.Timestamp("2026-05-12 00:00:00", tz="UTC")
    # Open of first bar = open of first second
    assert rs.iloc[0]["open"] == 0.0
    # Close of first bar = close of LAST second in the window (second 59)
    assert rs.iloc[0]["close"] == 59.25
    # Volume sums
    assert rs.iloc[0]["volume"] == 60.0


def test_vwap_volume_weighted_correctly():
    idx = pd.date_range("2026-05-12 00:00:00", periods=2, freq="1s", tz="UTC")
    df = pd.DataFrame({
        "open": [100.0, 200.0],
        "high": [100.0, 200.0],
        "low": [100.0, 200.0],
        "close": [100.0, 200.0],
        "volume": [3.0, 1.0],
        "vwap": [100.0, 200.0],
        "trades": [1, 1],
        "sources_mask": [0, 0],
    }, index=idx)
    rs = resample_bars(df, "5s")
    # vwap = (100*3 + 200*1) / 4 = 500/4 = 125
    assert abs(rs.iloc[0]["vwap"] - 125.0) < 1e-9


def test_1s_passthrough():
    df = _sample_1s(10)
    rs = resample_bars(df, "1s")
    assert len(rs) == 10
    assert (rs["close"] == df["close"]).all()


def test_empty_input():
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "vwap", "trades", "sources_mask"])
    empty.index = pd.DatetimeIndex([], tz="UTC")
    rs = resample_bars(empty, "1m")
    assert rs.empty
