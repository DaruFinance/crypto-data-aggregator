"""Metadata queries: list_symbols, venue_status, get_open_interest."""
from __future__ import annotations

import pandas as pd

from cryptodata.core.symbols import all_canonical
from cryptodata.storage.duckdb_views import connect


def list_symbols(venue: str | None = None) -> list[str]:
    """Return canonical symbols known to the project.

    If `venue` is given, return only canonical symbols this venue carries.
    """
    if venue is None:
        return all_canonical()
    from cryptodata.core.symbols import symbols_for_venue
    return [c for c, _ in symbols_for_venue(venue)]


def venue_status(since: str | pd.Timestamp | None = None) -> pd.DataFrame:
    """Return rows from the venue_status metadata table."""
    where = ""
    params: list = []
    if since is not None:
        ts = pd.Timestamp(since)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        where = "WHERE ts_ns >= ?"
        params.append(int(ts.value))
    sql = f"SELECT * FROM venue_status {where} ORDER BY ts_ns"
    with connect() as con:
        try:
            return con.execute(sql, params).fetchdf()
        except Exception:
            return pd.DataFrame()


def get_open_interest(
    symbol: str,
    start,
    end,
    venues: list[str] | str | None = "all",
    tz: str = "UTC",
) -> pd.DataFrame:
    def _to_ns(t) -> int:
        if isinstance(t, int):
            return t
        ts = pd.Timestamp(t)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return int(ts.value)

    start_ns = _to_ns(start)
    end_ns = _to_ns(end)
    venue_filter = ""
    if venues not in (None, "all"):
        if isinstance(venues, str):
            venues = [venues]
        venue_list = ",".join(f"'{v}'" for v in venues)
        venue_filter = f"AND venue IN ({venue_list})"
    sql = f"""
        SELECT ts_ns, venue, oi_base, oi_quote
        FROM open_interest
        WHERE symbol = ?
          AND ts_ns >= ?
          AND ts_ns < ?
          {venue_filter}
        ORDER BY ts_ns, venue
    """
    with connect() as con:
        try:
            df = con.execute(sql, [symbol, start_ns, end_ns]).fetchdf()
        except Exception:
            return pd.DataFrame(columns=["ts", "venue", "oi_base", "oi_quote"])
    if df.empty:
        return df
    idx = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    if tz != "UTC":
        idx = idx.tz_convert(tz)
    df = df.set_index(idx).drop(columns=["ts_ns"])
    df.index.name = "ts"
    return df
