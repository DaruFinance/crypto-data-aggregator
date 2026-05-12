"""End-to-end: write trades + bars via the parquet writer, query them via get_bars / get_trades."""

import pandas as pd

from cryptodata import get_bars, get_trades
from cryptodata.core.aggregate import bars_from_trades
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe


def _make_trades(n=120, venue="binance", base_ns=None):
    base_ns = base_ns or int(pd.Timestamp("2026-05-12 12:00:00", tz="UTC").value)
    rows = []
    for i in range(n):
        ts_ns = base_ns + i * 500_000_000   # every 500ms
        rows.append({
            "ts_ns": ts_ns,
            "recv_ns": ts_ns,
            "symbol": "BTC-USDT",
            "venue": venue,
            "price": 50_000.0 + (i % 10),
            "size": 0.1,
            "side": 1 if i % 2 == 0 else -1,
            "trade_id": str(i),
            "ingested_at_ns": ts_ns,
        })
    return rows


def test_trades_roundtrip():
    init_db()
    rows = _make_trades(60)
    date = date_str(rows[0]["ts_ns"])
    path = write_dataframe("trades", rows, symbol="BTC-USDT", venue="binance", date=date)
    assert path.exists()

    end = rows[-1]["ts_ns"] + 1
    start = rows[0]["ts_ns"]
    df = get_trades("BTC-USDT", start, end)
    assert len(df) == 60
    assert (df["venue"] == "binance").all()
    assert df["price"].iloc[0] == 50_000.0


def test_bars_query_at_1m():
    init_db()
    rows = _make_trades(240, venue="binance")  # 240 * 0.5s = 120s of data
    date = date_str(rows[0]["ts_ns"])
    write_dataframe("trades", rows, symbol="BTC-USDT", venue="binance", date=date)
    bars = bars_from_trades(rows, symbol="BTC-USDT", venue="binance")
    write_dataframe("bars_1s", bars, symbol="BTC-USDT", venue="binance", date=date)

    start = rows[0]["ts_ns"]
    end = rows[-1]["ts_ns"] + 1
    df = get_bars("BTC-USDT", start, end, "1m", sources=["binance"])
    # 120 seconds covers 2 minute-buckets (at most); confirm we get some data and columns
    assert not df.empty
    assert {"open", "high", "low", "close", "volume", "vwap"}.issubset(df.columns)
    assert df["volume"].sum() > 0


def test_bars_query_handles_empty_range():
    init_db()
    far_future_start = int(pd.Timestamp("2099-01-01", tz="UTC").value)
    far_future_end = far_future_start + 60_000_000_000
    df = get_bars("BTC-USDT", far_future_start, far_future_end, "1m")
    assert df.empty
