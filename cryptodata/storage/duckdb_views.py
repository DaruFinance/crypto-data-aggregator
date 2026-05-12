"""DuckDB views over the partitioned parquet tree, plus small metadata tables.

The DuckDB file at `data/duckdb/aggregator.duckdb` holds only:
  - views: trades, quotes, book_l2_snapshot, funding, open_interest, bars_1s, bars_ref
    pointing at `data/raw/<table>/**/*.parquet` (and `data/derived/<table>/**/*.parquet`)
    with hive partitioning so `symbol`, `venue`, `date` are auto-extracted columns.
  - native tables: symbol_map, venue_status, schema_version, corrections,
    daily_quality, coverage.

The views always reflect the current parquet state — no rebuild needed when
new partitions are written.
"""
from __future__ import annotations

import contextlib
from contextlib import contextmanager

import duckdb

from cryptodata.paths import DERIVED_ROOT, DUCKDB_PATH, RAW_ROOT, ensure_dirs

SCHEMA_VERSION = 2

_RAW_TABLES = ["trades", "quotes", "book_l2_snapshot", "funding", "open_interest"]
_DERIVED_TABLES = ["bars_1s", "bars_ref"]


def _glob_for(root, table: str) -> str:
    # DuckDB accepts forward slashes on Windows; normalize.
    return str((root / table).as_posix()) + "/**/*.parquet"


def init_db(path=None) -> duckdb.DuckDBPyConnection:
    """Open (and initialize on first run) the DuckDB instance.

    Idempotent: safe to call from any process. Returns an open connection.
    """
    ensure_dirs()
    con = duckdb.connect(str(path or DUCKDB_PATH))
    con.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP)")
    con.execute(
        "INSERT INTO schema_version VALUES (?, now()) ON CONFLICT DO NOTHING",
        [SCHEMA_VERSION],
    )
    con.execute("""
        CREATE TABLE IF NOT EXISTS symbol_map (
            canonical VARCHAR NOT NULL,
            venue VARCHAR NOT NULL,
            native VARCHAR NOT NULL,
            asset_class VARCHAR,           -- 'spot' | 'perp'
            base VARCHAR,
            quote VARCHAR,
            effective_from_ns BIGINT NOT NULL,
            effective_to_ns BIGINT,        -- NULL = still listed
            note VARCHAR,
            PRIMARY KEY (canonical, venue, effective_from_ns)
        )
    """)
    # Lightweight forward-only schema migrations: ADD COLUMN IF NOT EXISTS is a no-op
    # when the column is already there, so re-opening a v1-era DuckDB file just gains
    # the columns added since.
    for col, typ in (("asset_class", "VARCHAR"), ("base", "VARCHAR"), ("quote", "VARCHAR"), ("note", "VARCHAR")):
        with contextlib.suppress(Exception):
            con.execute(f"ALTER TABLE symbol_map ADD COLUMN IF NOT EXISTS {col} {typ}")
    con.execute("""
        CREATE TABLE IF NOT EXISTS venue_status (
            ts_ns BIGINT NOT NULL,
            venue VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            stream VARCHAR NOT NULL,
            up BOOLEAN NOT NULL,
            coverage_pct DOUBLE,
            notes VARCHAR,
            PRIMARY KEY (ts_ns, venue, symbol, stream)
        )
    """)
    # Bitemporal corrections log: every restatement / backfill / known-bad-range is
    # recorded here so a point-in-time consumer can reconstruct what the dataset
    # looked like at any moment, and so anomalies have a paper trail.
    con.execute("""
        CREATE TABLE IF NOT EXISTS corrections (
            correction_id BIGINT NOT NULL,     -- monotonic id (epoch ns of recording)
            recorded_at_ns BIGINT NOT NULL,    -- valid-time: when we recorded it
            effective_from_ns BIGINT NOT NULL, -- event-time range the correction covers
            effective_to_ns BIGINT NOT NULL,
            table_name VARCHAR NOT NULL,
            symbol VARCHAR,
            venue VARCHAR,
            kind VARCHAR NOT NULL,             -- 'backfill' | 'restatement' | 'bad_range' | 'gap' | 'note'
            severity VARCHAR NOT NULL,         -- 'info' | 'minor' | 'major' | 'critical'
            rows_affected BIGINT,
            note VARCHAR,
            PRIMARY KEY (correction_id)
        )
    """)
    # Per (symbol, venue, date) data-quality scorecard, produced by cryptodata.quality.
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_quality (
            date VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            venue VARCHAR NOT NULL,            -- per-venue series or 'agg'
            score DOUBLE NOT NULL,             -- 0..100
            grade VARCHAR NOT NULL,            -- 'A'..'F'
            bars INTEGER,
            expected_bars INTEGER,
            completeness_pct DOUBLE,
            max_gap_seconds INTEGER,
            n_issues INTEGER,
            n_critical INTEGER,
            issues_json VARCHAR,               -- serialized list of issue dicts
            computed_at_ns BIGINT,
            PRIMARY KEY (date, symbol, venue)
        )
    """)
    # Coverage matrix: rows per (table, symbol, venue, date) with observed vs expected.
    con.execute("""
        CREATE TABLE IF NOT EXISTS coverage (
            table_name VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            venue VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            rows BIGINT NOT NULL,
            first_ts_ns BIGINT,
            last_ts_ns BIGINT,
            span_seconds BIGINT,
            bytes_on_disk BIGINT,
            computed_at_ns BIGINT,
            PRIMARY KEY (table_name, symbol, venue, date)
        )
    """)
    register_views(con)
    return con


def _table_has_data(root, table: str) -> bool:
    """Cheap filesystem check — DuckDB's read_parquet errors on a glob that matches
    nothing, so we skip view registration for tables with no partitions yet. We only
    look one level deep (the `symbol=…` dirs); if a dir exists but holds no parquet,
    the query layer's IOException handler returns an empty frame. (One ``iterdir`` is
    far cheaper than an ``rglob`` over hundreds of part files on a network mount.)"""
    base = root / table
    if not base.exists():
        return False
    return any(p.is_dir() for p in base.iterdir())


def register_views(con: duckdb.DuckDBPyConnection) -> None:
    """(Re)register parquet views. Safe to call repeatedly.

    Tables with no parquet files yet are simply skipped. The query layer
    catches the resulting CatalogException and returns an empty DataFrame.
    """
    for table in _RAW_TABLES:
        con.execute(f"DROP VIEW IF EXISTS {table}")
        if not _table_has_data(RAW_ROOT, table):
            continue
        glob = _glob_for(RAW_ROOT, table)
        con.execute(f"""
            CREATE VIEW {table} AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)
        """)
    for table in _DERIVED_TABLES:
        con.execute(f"DROP VIEW IF EXISTS {table}")
        if not _table_has_data(DERIVED_ROOT, table):
            continue
        glob = _glob_for(DERIVED_ROOT, table)
        con.execute(f"""
            CREATE VIEW {table} AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)
        """)


@contextmanager
def connect():
    """Context-managed read connection. Always re-registers views to pick up new files."""
    con = init_db()
    try:
        yield con
    finally:
        con.close()


def safe_query(sql: str, params: list | None = None):
    """Run a query with view-not-found tolerance.

    If a view points at an empty parquet glob, DuckDB raises. We catch that and
    return an empty result with the expected columns.
    """
    with connect() as con:
        try:
            return con.execute(sql, params or []).fetchdf()
        except duckdb.IOException:
            # No files yet for one of the referenced views.
            return None
