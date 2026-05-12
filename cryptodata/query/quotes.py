"""get_quotes — top-of-book queries."""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from cryptodata.storage.duckdb_views import connect


def _to_ns(t) -> int:
    if isinstance(t, int):
        return t
    ts = pd.Timestamp(t)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value)


def get_quotes(
    symbol: str,
    start,
    end,
    venues: Iterable[str] | str | None = None,
    tz: str = "UTC",
) -> pd.DataFrame:
    start_ns = _to_ns(start)
    end_ns = _to_ns(end)
    if venues is None or venues == "all":
        venue_filter = ""
    else:
        if isinstance(venues, str):
            venues = [venues]
        venue_list = ",".join(f"'{v}'" for v in venues)
        venue_filter = f"AND venue IN ({venue_list})"

    sql = f"""
        SELECT ts_ns, recv_ns, symbol, venue, bid_px, ask_px, bid_sz, ask_sz
        FROM quotes
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
            return pd.DataFrame(columns=["ts", "venue", "bid_px", "ask_px", "bid_sz", "ask_sz"])
    if df.empty:
        return df
    idx = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    if tz != "UTC":
        idx = idx.tz_convert(tz)
    df = df.set_index(idx).drop(columns=["ts_ns", "symbol"])
    df.index.name = "ts"
    return df
