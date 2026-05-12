"""pytest configuration. Redirect CRYPTODATA_DATA_ROOT to a tmpdir so tests
never touch the live data store."""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest

# Modules that cache `cryptodata.paths` symbols at import time and must be reloaded
# after we redirect the data root.
_RELOAD = (
    "cryptodata.storage.parquet",
    "cryptodata.storage.duckdb_views",
    "cryptodata.core.corrections",
    "cryptodata.query.bars",
    "cryptodata.query.trades",
    "cryptodata.query.quotes",
    "cryptodata.query.funding",
    "cryptodata.query.meta",
    "cryptodata.query.ref",
    "cryptodata.query.books",
    "cryptodata.quality.checks",
    "cryptodata.quality.report",
    "cryptodata.obs.status",
    "cryptodata",
)


@pytest.fixture(autouse=True)
def isolate_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CRYPTODATA_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("CRYPTODATA_LOG_ROOT", str(tmp_path / "logs"))
    import cryptodata.paths as paths
    importlib.reload(paths)
    for mod in _RELOAD:
        try:
            importlib.reload(__import__(mod, fromlist=["*"]))
        except Exception:
            pass
    yield


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"
