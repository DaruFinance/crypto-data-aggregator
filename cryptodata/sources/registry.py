"""Source registry — single place to construct adapter instances by name."""
from __future__ import annotations

from cryptodata.sources.base import Source
from cryptodata.sources.binance import BinanceSpot
from cryptodata.sources.binance_futures import BinanceFutures
from cryptodata.sources.bitfinex import BitfinexSpot
from cryptodata.sources.bitstamp import BitstampSpot
from cryptodata.sources.bybit import BybitPerp
from cryptodata.sources.coinbase import Coinbase
from cryptodata.sources.gemini import GeminiSpot
from cryptodata.sources.kraken import Kraken
from cryptodata.sources.okx import OKXSpot

_ADAPTERS: dict[str, type] = {
    "binance": BinanceSpot,
    "binance_futures": BinanceFutures,
    "bybit": BybitPerp,
    "coinbase": Coinbase,
    "kraken": Kraken,
    "okx": OKXSpot,
    "bitstamp": BitstampSpot,
    "gemini": GeminiSpot,
    "bitfinex": BitfinexSpot,
}

# Venues that carry spot trades / quotes (eligible to contribute price to the
# consolidated tape). Perp/derivatives venues are kept out of the spot
# consolidated price by construction.
SPOT_VENUES: tuple[str, ...] = ("binance", "coinbase", "kraken", "okx", "bitstamp", "gemini", "bitfinex")
PERP_VENUES: tuple[str, ...] = ("binance_futures", "bybit")


def make_source(name: str) -> Source:
    cls = _ADAPTERS.get(name)
    if cls is None:
        raise KeyError(f"unknown source: {name!r}; known: {sorted(_ADAPTERS)}")
    return cls()


def all_sources() -> list[str]:
    return list(_ADAPTERS.keys())
