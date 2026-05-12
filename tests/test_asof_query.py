"""Point-in-time ('as-of') reads of the derived bars layer via ingested_at_ns."""
import pandas as pd

from cryptodata import get_bars
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe


def _bar(ts_ns, ingested_at_ns, px=100.0):
    return {"ts_ns": ts_ns, "symbol": "BTC-USD", "venue": "agg", "open": px, "high": px, "low": px,
            "close": px, "volume": 1.0, "vwap": px, "trades": 1, "sources_mask": 0,
            "ingested_at_ns": ingested_at_ns}


def test_asof_filters_by_ingested_at():
    init_db()
    base = int(pd.Timestamp("2026-05-12 00:00:00", tz="UTC").value)
    t1 = int(pd.Timestamp("2026-05-12 06:00:00", tz="UTC").value)   # first write
    t2 = int(pd.Timestamp("2026-05-12 18:00:00", tz="UTC").value)   # later restatement / extension

    early = [_bar(base + i * 1_000_000_000, t1) for i in range(10)]
    late = [_bar(base + (100 + i) * 1_000_000_000, t2) for i in range(10)]
    date = date_str(base)
    write_dataframe("bars_1s", early, symbol="BTC-USD", venue="agg", date=date)
    write_dataframe("bars_1s", late, symbol="BTC-USD", venue="agg", date=date)

    start = base
    end = base + 200 * 1_000_000_000

    all_bars = get_bars("BTC-USD", start, end, "1s", sources=["agg"])
    assert len(all_bars) == 20

    # as of t1 + 1ns: only the early batch is visible
    asof_early = get_bars("BTC-USD", start, end, "1s", sources=["agg"], asof=t1 + 1)
    assert len(asof_early) == 10

    # as of just before t1: nothing visible yet
    asof_none = get_bars("BTC-USD", start, end, "1s", sources=["agg"], asof=t1 - 1)
    assert asof_none.empty

    # as of t2: everything
    asof_all = get_bars("BTC-USD", start, end, "1s", sources=["agg"], asof=t2)
    assert len(asof_all) == 20
