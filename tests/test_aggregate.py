from cryptodata.core.aggregate import aggregate_second, bars_from_trades
from cryptodata.core.symbols import venue_bitmask_index


def _trade(ts_ns, price, size, side=1, venue="binance"):
    return {
        "ts_ns": ts_ns, "recv_ns": ts_ns,
        "symbol": "BTC-USDT", "venue": venue,
        "price": price, "size": size, "side": side, "trade_id": "x",
    }


def test_bars_from_trades_groups_by_second():
    ts0 = 1_700_000_000 * 1_000_000_000
    trades = [
        _trade(ts0 + 100_000_000, 100.0, 1.0),
        _trade(ts0 + 200_000_000, 101.0, 2.0),
        _trade(ts0 + 1_500_000_000, 102.0, 1.0),  # next second
    ]
    bars = bars_from_trades(trades, symbol="BTC-USDT", venue="binance")
    assert len(bars) == 2
    b0 = bars[0]
    assert b0["ts_ns"] == ts0
    assert b0["open"] == 100.0
    assert b0["close"] == 101.0
    assert b0["high"] == 101.0
    assert b0["low"] == 100.0
    assert b0["volume"] == 3.0
    # vwap = (100*1 + 101*2) / 3
    assert abs(b0["vwap"] - (302.0 / 3.0)) < 1e-9
    assert b0["trades"] == 2

    b1 = bars[1]
    assert b1["ts_ns"] == ts0 + 1_000_000_000
    assert b1["open"] == b1["close"] == 102.0


def test_aggregate_second_combines_venues():
    ts0 = 1_700_000_000 * 1_000_000_000
    per_venue = [
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "binance",
         "open": 100, "high": 101, "low": 99, "close": 100.5,
         "volume": 10, "vwap": 100.2, "trades": 50, "sources_mask": 0},
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "coinbase",
         "open": 100.1, "high": 101.2, "low": 99.1, "close": 100.6,
         "volume": 5, "vwap": 100.3, "trades": 30, "sources_mask": 0},
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "kraken",
         "open": 100.05, "high": 101.05, "low": 99.05, "close": 100.55,
         "volume": 2, "vwap": 100.25, "trades": 10, "sources_mask": 0},
    ]
    idx = venue_bitmask_index(["binance", "coinbase", "kraken"])
    agg = aggregate_second(per_venue, venues_index=idx)
    assert agg is not None
    assert agg["venue"] == "agg"
    assert agg["volume"] == 17.0
    # All three should be present in mask
    assert agg["sources_mask"] == sum(idx.values())
    # vwap = sum(vwap*volume) / sum(volume)
    expected_vwap = (100.2 * 10 + 100.3 * 5 + 100.25 * 2) / 17
    assert abs(agg["vwap"] - expected_vwap) < 1e-9
    assert agg["high"] == 101.2
    assert agg["low"] == 99


def test_aggregate_second_drops_outlier():
    ts0 = 1_700_000_000 * 1_000_000_000
    # Three normal venues + one wild outlier at 2x price
    per_venue = [
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "binance",
         "open": 100, "high": 100, "low": 100, "close": 100,
         "volume": 1, "vwap": 100, "trades": 1, "sources_mask": 0},
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "coinbase",
         "open": 100, "high": 100, "low": 100, "close": 100,
         "volume": 1, "vwap": 100, "trades": 1, "sources_mask": 0},
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "kraken",
         "open": 100, "high": 100, "low": 100, "close": 100,
         "volume": 1, "vwap": 100, "trades": 1, "sources_mask": 0},
        {"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "rogue",
         "open": 200, "high": 200, "low": 200, "close": 200,
         "volume": 1, "vwap": 200, "trades": 1, "sources_mask": 0},
    ]
    idx = venue_bitmask_index(["binance", "coinbase", "kraken", "rogue"])
    agg = aggregate_second(per_venue, outlier_sigma=2.0, venues_index=idx)
    assert agg is not None
    # rogue's bit should NOT be set
    assert agg["sources_mask"] & idx["rogue"] == 0
    # Three good venues should all be set
    for v in ("binance", "coinbase", "kraken"):
        assert agg["sources_mask"] & idx[v] != 0
    assert agg["volume"] == 3.0
    assert abs(agg["vwap"] - 100.0) < 1e-9


def test_aggregate_returns_none_when_no_volume():
    ts0 = 1_700_000_000 * 1_000_000_000
    assert aggregate_second([]) is None
    only_zero = [{"ts_ns": ts0, "symbol": "BTC-USDT", "venue": "binance",
                  "open": 0, "high": 0, "low": 0, "close": 0,
                  "volume": 0, "vwap": 0, "trades": 0, "sources_mask": 0}]
    assert aggregate_second(only_zero) is None
