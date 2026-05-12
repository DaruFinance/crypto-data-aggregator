"""Adapter normalization tests — every venue, no network.

Each adapter's WebSocket layer is monkeypatched to replay recorded-shape trade
frames (`tests/fixtures/ws_trade_frames.json`); we then drain `stream_trades` and
assert the normalized `Trade` rows. This is the layer that breaks silently when a
venue tweaks its payload, so it's the one worth pinning down.
"""
from __future__ import annotations

import json

import pytest

from cryptodata.sources.registry import make_source

# --------------------------------------------------------------------------- #
# fake WS plumbing
# --------------------------------------------------------------------------- #

class _FakeWS:
    """Minimal async-iterable websocket: yields frames, `.send()` is a no-op."""

    def __init__(self, frames):
        self._frames = list(frames)

    async def send(self, *_a, **_k):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._frames:
            raise StopAsyncIteration
        return self._frames.pop(0)


async def _connect_once(frames):
    yield _FakeWS(frames)


async def _agen(items):
    for x in items:
        yield x


def _patch_ws(adapter, kind: str, frames, monkeypatch):
    if kind == "decoded_dict":
        # binance / binance_futures style: `_ws_messages(url)` yields decoded dicts
        monkeypatch.setattr(adapter, "_ws_messages", lambda url: _agen(frames), raising=True)
    elif kind == "raw_str":
        # `_connect()` yields a connected ws that iterates raw JSON strings
        monkeypatch.setattr(adapter, "_connect", lambda: _connect_once(frames), raising=True)
    else:  # pragma: no cover
        raise ValueError(kind)


async def _drain(aiter, limit: int = 50):
    out = []
    async for x in aiter:
        out.append(x)
        if len(out) >= limit:
            break
    return out


# --------------------------------------------------------------------------- #
# the test
# --------------------------------------------------------------------------- #

def _load_cases(fixtures_dir):
    data = json.loads((fixtures_dir / "ws_trade_frames.json").read_text())
    return {k: v for k, v in data.items() if not k.startswith("_")}


@pytest.mark.parametrize("venue", [
    "binance", "coinbase", "kraken", "bybit", "okx", "bitstamp", "gemini", "bitfinex",
])
def test_stream_trades_normalization(venue, fixtures_dir, monkeypatch):
    case = _load_cases(fixtures_dir)[venue]
    adapter = make_source(venue)
    _patch_ws(adapter, case["kind"], case["frames"], monkeypatch)

    import asyncio
    trades = asyncio.run(_drain(adapter.stream_trades([case["canonical"]])))
    assert trades, f"{venue}: no trades parsed from the fixture frame"
    t = trades[0]
    row = t.to_row()
    exp = case["expect"]
    assert row["symbol"] == exp["symbol"]
    assert row["venue"] == exp["venue"]
    assert abs(row["price"] - exp["price"]) < 1e-6
    assert abs(row["size"] - exp["size"]) < 1e-9
    assert row["side"] == exp["side"]
    assert str(row["trade_id"]) == str(exp["trade_id"])
    assert row["ts_ns"] > 0
    assert row["recv_ns"] > 0
    if "ts_ns" in exp:
        assert row["ts_ns"] == exp["ts_ns"]


def test_all_registered_adapters_have_a_fixture(fixtures_dir):
    """Guard: if someone adds a new venue adapter, they must add a parse fixture for it."""
    from cryptodata.sources.registry import all_sources
    cases = _load_cases(fixtures_dir)
    missing = [v for v in all_sources() if v not in cases and v != "binance_futures"]
    assert not missing, f"venues without a WS-trade parse fixture: {missing}"
