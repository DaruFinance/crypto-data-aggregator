from cryptodata.core.symbols import (
    all_canonical,
    perpetuals,
    spots,
    symbols_for_venue,
    to_canonical,
    to_native,
    venue_bitmask_index,
)


def test_canonical_roundtrip_binance():
    assert to_native("BTC-USDT", "binance") == "BTCUSDT"
    assert to_canonical("BTCUSDT", "binance") == "BTC-USDT"


def test_canonical_roundtrip_coinbase():
    assert to_native("BTC-USD", "coinbase") == "BTC-USD"
    assert to_canonical("BTC-USD", "coinbase") == "BTC-USD"


def test_perp_isolation():
    perps = set(perpetuals())
    sp = set(spots())
    assert perps & sp == set()
    assert "BTC-USDT-PERP" in perps
    assert "BTC-USDT" in sp


def test_symbols_for_venue_filters_correctly():
    bn = dict(symbols_for_venue("binance"))
    assert bn.get("BTC-USDT") == "BTCUSDT"
    # binance shouldn't carry perp-only entries
    assert "BTC-USDT-PERP" not in bn


def test_venue_bitmask_index_stable():
    idx = venue_bitmask_index(["binance", "coinbase", "kraken"])
    # Each venue has a unique power-of-2 bit
    bits = list(idx.values())
    assert len(set(bits)) == len(bits)
    assert all(b > 0 and (b & (b - 1)) == 0 for b in bits)


def test_unknown_lookup_returns_none():
    assert to_native("NONEXISTENT-USDT", "binance") is None
    assert to_canonical("NEVER", "binance") is None
    assert to_native("BTC-USDT", "no_such_venue") is None


def test_all_canonical_nonempty():
    syms = all_canonical()
    assert len(syms) >= 20
    assert "BTC-USDT" in syms
    assert "ETH-USDT" in syms
