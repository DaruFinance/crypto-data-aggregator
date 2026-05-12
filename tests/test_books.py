"""L2 book queries: top-of-book flattening, invariant checks, parquet roundtrip."""
import pandas as pd

from cryptodata.query.books import book_quality, book_top, get_book_snapshots
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe


def _snap_rows(n=5, venue="binance", base_ns=None, bad_idx=None):
    base_ns = base_ns or int(pd.Timestamp("2026-05-12 12:00:00", tz="UTC").value)
    rows = []
    for i in range(n):
        ts = base_ns + i * 5_000_000_000
        bids = [{"px": 100.0 - j * 0.1, "sz": 1.0 + j} for j in range(3)]
        asks = [{"px": 100.1 + j * 0.1, "sz": 1.0 + j} for j in range(3)]
        if bad_idx is not None and i == bad_idx:
            asks = [{"px": 99.5, "sz": 1.0}, {"px": 100.2, "sz": 1.0}, {"px": 100.3, "sz": 1.0}]  # crossed top
        rows.append({"ts_ns": ts, "recv_ns": ts, "symbol": "BTC-USDT", "venue": venue,
                     "depth": 3, "bids": bids, "asks": asks, "ingested_at_ns": ts})
    return rows


def test_book_top_and_quality_on_frame():
    rows = _snap_rows(4, bad_idx=2)
    idx = pd.to_datetime([r["ts_ns"] for r in rows], unit="ns", utc=True)
    df = pd.DataFrame(rows).set_index(idx)
    df.index.name = "ts"
    top = book_top(df)
    assert len(top) == 4
    assert (top["ask_px"] > 0).all() and (top["bid_px"] > 0).all()
    # row 2 is crossed (bid 100.0 >= ask 99.5)
    q = book_quality(df)
    assert q["n_snapshots"] == 4
    assert q["issues"].get("crossed_top", 0) >= 1
    assert q["clean_pct"] < 100.0


def test_book_snapshot_parquet_roundtrip():
    init_db()
    rows = _snap_rows(5, venue="binance")
    date = date_str(rows[0]["ts_ns"])
    write_dataframe("book_l2_snapshot", rows, symbol="BTC-USDT", venue="binance", date=date)
    start = rows[0]["ts_ns"]
    end = rows[-1]["ts_ns"] + 1
    out = get_book_snapshots("BTC-USDT", start, end, venue="binance")
    assert len(out) == 5
    # levels are preserved (3 per side)
    first_bids = list(out.iloc[0]["bids"])
    assert len(first_bids) == 3
    top = book_top(out)
    assert len(top) == 5
    assert (top["spread_bps"] > 0).all()
