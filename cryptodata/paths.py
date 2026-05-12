"""Single source of truth for project paths and config loading."""
from __future__ import annotations

import os
import tomllib
from functools import lru_cache
from pathlib import Path

# Project root = three levels up from this file (.../cryptodata/paths.py -> project root).
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Allow env override so tests can redirect to a tmpdir.
DATA_ROOT = Path(os.environ.get("CRYPTODATA_DATA_ROOT", PROJECT_ROOT / "data"))
CONFIG_ROOT = Path(os.environ.get("CRYPTODATA_CONFIG_ROOT", PROJECT_ROOT / "config"))
LOG_ROOT = Path(os.environ.get("CRYPTODATA_LOG_ROOT", PROJECT_ROOT / "logs"))

RAW_ROOT = DATA_ROOT / "raw"
DERIVED_ROOT = DATA_ROOT / "derived"
META_ROOT = DATA_ROOT / "meta"
DUCKDB_PATH = DATA_ROOT / "duckdb" / "aggregator.duckdb"


def ensure_dirs() -> None:
    for p in (RAW_ROOT, DERIVED_ROOT, META_ROOT, DUCKDB_PATH.parent, LOG_ROOT):
        p.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def load_venues() -> dict:
    with open(CONFIG_ROOT / "venues.toml", "rb") as f:
        return tomllib.load(f)


@lru_cache(maxsize=1)
def load_symbols() -> dict:
    with open(CONFIG_ROOT / "symbols.toml", "rb") as f:
        return tomllib.load(f)


@lru_cache(maxsize=1)
def load_ingest() -> dict:
    with open(CONFIG_ROOT / "ingest.toml", "rb") as f:
        return tomllib.load(f)


def partition_path(table: str, symbol: str, venue: str, date: str) -> Path:
    """Hive-style partition path for a raw or derived table."""
    if table.startswith("bars"):
        root = DERIVED_ROOT / table
    else:
        root = RAW_ROOT / table
    return root / f"symbol={symbol}" / f"venue={venue}" / f"date={date}"
