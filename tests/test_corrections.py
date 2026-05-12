"""Bitemporal corrections log + point-in-time symbol_map."""
import time

from cryptodata.core.corrections import (
    list_corrections,
    record_correction,
    retire_mapping,
    seed_symbol_map,
    symbol_map_asof,
)
from cryptodata.storage.duckdb_views import init_db


def test_seed_symbol_map_idempotent():
    init_db()
    n1 = seed_symbol_map()
    n2 = seed_symbol_map()
    assert n1 == n2 and n1 > 20
    df = symbol_map_asof(time.time_ns())
    assert not df.empty
    row = df[(df["canonical"] == "BTC-USDT") & (df["venue"] == "binance")]
    assert not row.empty
    assert row.iloc[0]["native"] == "BTCUSDT"
    assert row.iloc[0]["asset_class"] == "spot"
    perp = df[(df["canonical"] == "BTC-USDT-PERP")]
    assert not perp.empty and (perp["asset_class"] == "perp").all()


def test_record_and_list_corrections():
    init_db()
    cid = record_correction(table_name="trades", kind="backfill", severity="info",
                            symbol="BTC-USD", venue="coinbase",
                            effective_from_ns=1, effective_to_ns=2, rows_affected=42, note="test")
    assert cid > 0
    df = list_corrections(table_name="trades", symbol="BTC-USD")
    assert len(df) == 1
    r = df.iloc[0]
    assert r["kind"] == "backfill" and r["venue"] == "coinbase" and int(r["rows_affected"]) == 42


def test_retire_mapping_closes_interval_and_logs():
    init_db()
    seed_symbol_map()
    cutoff = 1_900_000_000 * 1_000_000_000
    retire_mapping("BTC-USDT", "binance", effective_to_ns=cutoff, note="hypothetical delisting")
    after = symbol_map_asof(cutoff + 1)
    assert after[(after["canonical"] == "BTC-USDT") & (after["venue"] == "binance")].empty
    before = symbol_map_asof(cutoff - 1)
    assert not before[(before["canonical"] == "BTC-USDT") & (before["venue"] == "binance")].empty
    # the retirement is itself a logged correction
    log = list_corrections(table_name="symbol_map", symbol="BTC-USDT")
    assert any(c["kind"] == "restatement" for _, c in log.iterrows())


def test_bad_kind_and_severity_rejected():
    init_db()
    import pytest
    with pytest.raises(ValueError):
        record_correction(table_name="trades", kind="oops", effective_from_ns=0, effective_to_ns=0)
    with pytest.raises(ValueError):
        record_correction(table_name="trades", kind="note", severity="catastrophic", effective_from_ns=0, effective_to_ns=0)
