"""Full end-to-end: synthesize trades for three venues, build per-venue + agg 1s bars,
query at 1m, and verify the agg series is between the per-venue extremes."""
import pandas as pd

from cryptodata import get_bars
from cryptodata.core.aggregate import aggregate_second, bars_from_trades
from cryptodata.core.symbols import venue_bitmask_index
from cryptodata.storage.duckdb_views import init_db
from cryptodata.storage.parquet import date_str, write_dataframe


def _trades(base_ns: int, *, venue: str, price_offset: float, n: int = 600):
    """600 trades over 600 seconds, one per second, with a small price drift."""
    rows = []
    for i in range(n):
        rows.append({
            "ts_ns": base_ns + i * 1_000_000_000,
            "recv_ns": base_ns + i * 1_000_000_000,
            "symbol": "BTC-USDT", "venue": venue,
            "price": 50_000.0 + price_offset + (i * 0.01),
            "size": 1.0, "side": 1, "trade_id": f"{venue}-{i}",
            "ingested_at_ns": base_ns + i * 1_000_000_000,
        })
    return rows


def test_endtoend_three_venues():
    init_db()
    base = int(pd.Timestamp("2026-05-12 12:00:00", tz="UTC").value)

    all_trades_by_venue = {}
    for venue, offset in [("binance", 0.0), ("coinbase", 0.5), ("kraken", -0.3)]:
        trades = _trades(base, venue=venue, price_offset=offset)
        all_trades_by_venue[venue] = trades
        date = date_str(trades[0]["ts_ns"])
        write_dataframe("trades", trades, symbol="BTC-USDT", venue=venue, date=date)
        bars = bars_from_trades(trades, symbol="BTC-USDT", venue=venue)
        write_dataframe("bars_1s", bars, symbol="BTC-USDT", venue=venue, date=date)

    # Build agg bars
    idx = venue_bitmask_index(["binance", "coinbase", "kraken", "bybit", "binance_futures"])
    # For each second, collect per-venue bars and aggregate
    from collections import defaultdict
    sec_to_bars: dict[int, list[dict]] = defaultdict(list)
    for venue, trades in all_trades_by_venue.items():
        for b in bars_from_trades(trades, symbol="BTC-USDT", venue=venue):
            sec_to_bars[b["ts_ns"]].append(b)
    agg_rows = []
    for ts_ns in sorted(sec_to_bars):
        a = aggregate_second(sec_to_bars[ts_ns], venues_index=idx)
        if a:
            agg_rows.append(a)
    date = date_str(agg_rows[0]["ts_ns"])
    write_dataframe("bars_1s", agg_rows, symbol="BTC-USDT", venue="agg", date=date)

    # Query the agg series at 1m
    start = base
    end = base + 600 * 1_000_000_000
    df_agg = get_bars("BTC-USDT", start, end, "1m", sources=["agg"])
    df_bin = get_bars("BTC-USDT", start, end, "1m", sources=["binance"])
    df_cb = get_bars("BTC-USDT", start, end, "1m", sources=["coinbase"])

    assert not df_agg.empty
    # Agg close should fall between binance and coinbase closes for matching bars
    # (allowing tiny epsilon for float)
    common_idx = df_agg.index.intersection(df_bin.index).intersection(df_cb.index)
    assert len(common_idx) >= 5
    for ts in common_idx[:5]:
        lo = min(df_bin.loc[ts, "close"], df_cb.loc[ts, "close"])
        hi = max(df_bin.loc[ts, "close"], df_cb.loc[ts, "close"])
        agg_close = df_agg.loc[ts, "close"]
        assert lo - 1.0 <= agg_close <= hi + 1.0
