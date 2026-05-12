"""Bitemporal corrections log + symbol-map maintenance.

A data vendor's most important promise is that you can reconstruct *what the dataset
said at a given time*. Two mechanisms back that here:

1. Every row carries ``ingested_at_ns`` (raw tables natively; ``bars_1s``/``bars_ref``
   as a nullable column), so an "as-of" read just filters on it.
2. Anything that doesn't reduce to a row append — a restatement, a known-bad range,
   a discovered gap, a backfill batch — is written to the ``corrections`` table with
   both a *valid time* (``recorded_at_ns``: when we learned it) and an *event time*
   range (``effective_from_ns``..``effective_to_ns``: the data it applies to).

This module also maintains ``symbol_map`` (the point-in-time reference data:
canonical→native per venue, with listing/delisting effective dates), seeded from
``config/symbols.toml`` and extendable for ticker renames.
"""
from __future__ import annotations

import time

import pandas as pd

from cryptodata.core.symbols import _build_index  # noqa: PLC2701  (intentional internal use)
from cryptodata.storage.duckdb_views import connect

_SEVERITIES = {"info", "minor", "major", "critical"}
_KINDS = {"backfill", "restatement", "bad_range", "gap", "note"}


# --------------------------------------------------------------------------- #
# corrections log
# --------------------------------------------------------------------------- #

def record_correction(
    *,
    table_name: str,
    kind: str,
    severity: str = "info",
    symbol: str | None = None,
    venue: str | None = None,
    effective_from_ns: int,
    effective_to_ns: int,
    rows_affected: int | None = None,
    note: str | None = None,
) -> int:
    """Append one row to the corrections log. Returns the new ``correction_id``."""
    if kind not in _KINDS:
        raise ValueError(f"kind must be one of {sorted(_KINDS)}, got {kind!r}")
    if severity not in _SEVERITIES:
        raise ValueError(f"severity must be one of {sorted(_SEVERITIES)}, got {severity!r}")
    cid = time.time_ns()
    with connect() as con:
        con.execute(
            """INSERT INTO corrections
               (correction_id, recorded_at_ns, effective_from_ns, effective_to_ns,
                table_name, symbol, venue, kind, severity, rows_affected, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [cid, cid, int(effective_from_ns), int(effective_to_ns), table_name,
             symbol, venue, kind, severity, rows_affected, note],
        )
    return cid


def list_corrections(
    *,
    table_name: str | None = None,
    symbol: str | None = None,
    venue: str | None = None,
    since=None,
) -> pd.DataFrame:
    """Return corrections rows, optionally filtered."""
    clauses: list[str] = []
    params: list = []
    if table_name:
        clauses.append("table_name = ?")
        params.append(table_name)
    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if venue:
        clauses.append("venue = ?")
        params.append(venue)
    if since is not None:
        ts = pd.Timestamp(since)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        clauses.append("recorded_at_ns >= ?")
        params.append(int(ts.value))
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with connect() as con:
        try:
            return con.execute(f"SELECT * FROM corrections {where} ORDER BY correction_id", params).fetchdf()
        except Exception:
            return pd.DataFrame()


# --------------------------------------------------------------------------- #
# symbol_map (point-in-time reference data)
# --------------------------------------------------------------------------- #

def _asset_class(canonical: str) -> str:
    return "perp" if canonical.endswith("-PERP") else "spot"


def _base_quote(canonical: str) -> tuple[str, str]:
    parts = canonical.replace("-PERP", "").split("-")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return canonical, ""


def seed_symbol_map(effective_from_ns: int | None = None) -> int:
    """(Re)seed ``symbol_map`` from ``config/symbols.toml``.

    Idempotent — uses ``ON CONFLICT DO NOTHING`` on (canonical, venue, effective_from_ns),
    so re-running it after editing the config only adds new mappings. To record a
    ticker rename, call :func:`retire_mapping` then re-seed.

    Returns the number of (canonical, venue) mappings present after seeding.
    """
    by_canonical, _ = _build_index()
    eff = int(effective_from_ns if effective_from_ns is not None else 0)  # 0 = "since the beginning of our coverage"
    rows = []
    for canonical, sym in by_canonical.items():
        base, quote = _base_quote(canonical)
        ac = _asset_class(canonical)
        for venue, native in sym.venue_native.items():
            rows.append([canonical, venue, native, ac, base, quote, eff, None, "seeded from config/symbols.toml"])
    with connect() as con:
        con.executemany(
            """INSERT INTO symbol_map
               (canonical, venue, native, asset_class, base, quote, effective_from_ns, effective_to_ns, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
            rows,
        )
        n = con.execute("SELECT COUNT(*) FROM symbol_map").fetchone()[0]
    return int(n)


def retire_mapping(canonical: str, venue: str, *, effective_to_ns: int, note: str | None = None) -> None:
    """Close out a (canonical, venue) mapping at ``effective_to_ns`` (e.g. a delisting)."""
    with connect() as con:
        con.execute(
            "UPDATE symbol_map SET effective_to_ns = ?, note = COALESCE(?, note) "
            "WHERE canonical = ? AND venue = ? AND effective_to_ns IS NULL",
            [int(effective_to_ns), note, canonical, venue],
        )
    record_correction(
        table_name="symbol_map", kind="restatement", severity="info",
        symbol=canonical, venue=venue, effective_from_ns=int(effective_to_ns),
        effective_to_ns=int(effective_to_ns), note=f"mapping retired: {note or ''}",
    )


def symbol_map_asof(asof_ns: int) -> pd.DataFrame:
    """Return the symbol_map as it stood at ``asof_ns`` (effective_from <= asof < effective_to)."""
    with connect() as con:
        try:
            return con.execute(
                "SELECT * FROM symbol_map WHERE effective_from_ns <= ? "
                "AND (effective_to_ns IS NULL OR effective_to_ns > ?) ORDER BY canonical, venue",
                [int(asof_ns), int(asof_ns)],
            ).fetchdf()
        except Exception:
            return pd.DataFrame()
