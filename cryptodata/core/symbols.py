"""Canonical symbol mapping across venues.

Canonical form:
    spot:  BASE-QUOTE        e.g. BTC-USDT, ETH-USD
    perp:  BASE-QUOTE-PERP   e.g. BTC-USDT-PERP

The mapping is sourced from `config/symbols.toml`. Use `to_native(canonical, venue)`
when calling a venue API, and `to_canonical(native, venue)` when parsing venue payloads.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache

from cryptodata.paths import load_symbols


@dataclass(frozen=True, slots=True)
class SymbolMap:
    canonical: str
    venue_native: dict[str, str]  # venue -> native ticker

    def native_for(self, venue: str) -> str | None:
        return self.venue_native.get(venue)


@lru_cache(maxsize=1)
def _build_index() -> tuple[dict[str, SymbolMap], dict[tuple[str, str], str]]:
    """Returns (canonical -> SymbolMap, (venue, native) -> canonical)."""
    cfg = load_symbols()
    by_canonical: dict[str, SymbolMap] = {}
    reverse: dict[tuple[str, str], str] = {}
    for entry in cfg.get("symbols", []):
        canonical = entry["canonical"]
        natives = {k: v for k, v in entry.items() if k != "canonical"}
        by_canonical[canonical] = SymbolMap(canonical=canonical, venue_native=natives)
        for venue, native in natives.items():
            reverse[(venue, native.upper())] = canonical
    return by_canonical, reverse


def all_canonical() -> list[str]:
    by_canonical, _ = _build_index()
    return list(by_canonical.keys())


def to_native(canonical: str, venue: str) -> str | None:
    by_canonical, _ = _build_index()
    sym = by_canonical.get(canonical)
    return sym.native_for(venue) if sym else None


def to_canonical(native: str, venue: str) -> str | None:
    _, reverse = _build_index()
    return reverse.get((venue, native.upper()))


def symbols_for_venue(venue: str) -> list[tuple[str, str]]:
    """Return [(canonical, native), ...] for symbols that this venue carries."""
    by_canonical, _ = _build_index()
    result: list[tuple[str, str]] = []
    for canonical, sym in by_canonical.items():
        native = sym.native_for(venue)
        if native:
            result.append((canonical, native))
    return result


def perpetuals() -> list[str]:
    return [c for c in all_canonical() if c.endswith("-PERP")]


def spots() -> list[str]:
    return [c for c in all_canonical() if not c.endswith("-PERP")]


def venue_bitmask_index(venues: Iterable[str]) -> dict[str, int]:
    """Stable bit position per venue, used for the sources_mask column on agg bars."""
    return {v: 1 << i for i, v in enumerate(sorted(set(venues)))}
