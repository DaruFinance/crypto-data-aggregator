"""Consolidated tape: k-way merge ordering, robust outlier filtering, provenance."""
from cryptodata.core.consolidated import build_agg_bars, merge_trades
from cryptodata.core.symbols import venue_bitmask_index


def _t(ts_ns, price, size, venue, recv_ns=None):
    return {"ts_ns": ts_ns, "recv_ns": recv_ns if recv_ns is not None else ts_ns,
            "symbol": "BTC-USD", "venue": venue, "price": price, "size": size, "side": 1}


def test_merge_trades_orders_by_ts_then_recv():
    s = 1_700_000_000 * 1_000_000_000
    per_venue = {
        "binance": [_t(s + 200, 100.0, 1.0, "binance"), _t(s + 800, 100.1, 1.0, "binance")],
        "coinbase": [_t(s + 100, 100.05, 1.0, "coinbase"), _t(s + 500, 100.2, 1.0, "coinbase")],
        "kraken": [_t(s + 300, 99.95, 1.0, "kraken")],
    }
    merged = merge_trades(per_venue)
    assert [m["ts_ns"] for m in merged] == sorted(m["ts_ns"] for m in merged)
    assert len(merged) == 5
    # first trade is the coinbase one at +100
    assert merged[0]["venue"] == "coinbase" and merged[0]["ts_ns"] == s + 100


def test_build_agg_bars_basic_ohlc_and_mask():
    s = 1_700_000_000 * 1_000_000_000
    idx = venue_bitmask_index(["binance", "coinbase", "kraken", "okx"])
    # one second: three venues, prices 100.0 / 100.2 / 99.9, sizes 2 / 1 / 1
    trades = [
        _t(s + 10_000_000, 100.0, 2.0, "binance"),
        _t(s + 20_000_000, 100.2, 1.0, "coinbase"),
        _t(s + 30_000_000, 99.9, 1.0, "kraken"),
    ]
    bars = build_agg_bars(trades, symbol="BTC-USD", venues_index=idx)
    assert len(bars) == 1
    b = bars[0]
    assert b["venue"] == "agg" and b["ts_ns"] == s
    assert b["open"] == 100.0          # first surviving trade
    assert b["close"] == 99.9          # last surviving trade
    assert b["high"] == 100.2 and b["low"] == 99.9
    assert b["volume"] == 4.0
    assert abs(b["vwap"] - (100.0 * 2 + 100.2 * 1 + 99.9 * 1) / 4) < 1e-9
    for v in ("binance", "coinbase", "kraken"):
        assert b["sources_mask"] & idx[v]
    assert not (b["sources_mask"] & idx["okx"])
    assert b["trades"] == 3
    assert b["ingested_at_ns"] > 0


def test_build_agg_bars_drops_fat_finger_print():
    s = 1_700_000_000 * 1_000_000_000
    idx = venue_bitmask_index(["binance", "coinbase", "kraken", "rogue"])
    trades = [
        _t(s + 1_000_000, 100.0, 1.0, "binance"),
        _t(s + 2_000_000, 100.01, 1.0, "coinbase"),
        _t(s + 3_000_000, 99.99, 1.0, "kraken"),
        _t(s + 4_000_000, 250.0, 1.0, "rogue"),     # 2.5x — must be filtered
    ]
    bars = build_agg_bars(trades, symbol="BTC-USD", venues_index=idx, mad_k=5.0, min_venues_for_filter=3)
    assert len(bars) == 1
    b = bars[0]
    assert not (b["sources_mask"] & idx["rogue"])
    assert b["high"] < 110.0           # the 250 print is gone
    assert b["volume"] == 3.0


def test_build_agg_bars_two_venues_not_filtered():
    """With < min_venues_for_filter venues, a print is never silently dropped."""
    s = 1_700_000_000 * 1_000_000_000
    trades = [_t(s + 1_000_000, 100.0, 1.0, "binance"), _t(s + 2_000_000, 130.0, 1.0, "coinbase")]
    bars = build_agg_bars(trades, symbol="BTC-USD", min_venues_for_filter=3)
    assert len(bars) == 1
    assert bars[0]["high"] == 130.0    # kept — only 2 venues, filter abstains


def test_build_agg_bars_stale_lag_excluded():
    s = 1_700_000_000 * 1_000_000_000
    trades = [
        _t(s + 1_000_000, 100.0, 1.0, "binance", recv_ns=s + 1_000_000),
        _t(s + 2_000_000, 999.0, 1.0, "coinbase", recv_ns=s + 2_000_000 + 10 * 1_000_000_000),  # 10s late
    ]
    bars = build_agg_bars(trades, symbol="BTC-USD", stale_recv_lag_ms=5000, min_venues_for_filter=2)
    assert len(bars) == 1
    assert bars[0]["high"] == 100.0    # the 10s-stale 999.0 print was dropped before bar-build
    assert bars[0]["sources_mask"] >= 0
