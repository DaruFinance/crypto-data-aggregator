"""Nightly part-file compaction: merge hourly parts into one file per (table, symbol, venue, date)."""
from __future__ import annotations

import logging
from pathlib import Path

import pyarrow.parquet as pq

from cryptodata.paths import DERIVED_ROOT, RAW_ROOT
from cryptodata.storage.schemas import schema_for

log = logging.getLogger("cryptodata.compact")


def compact_partition(part_dir: Path, table: str) -> int:
    """Merge all .parquet files in part_dir into a single compacted file.

    Returns the number of input files merged. Leaves a single
    `compacted-<ns>.parquet` and removes the originals.
    """
    files = sorted(part_dir.glob("part-*.parquet"))
    if len(files) <= 1:
        return 0
    schema = schema_for(table)
    # ParquetFile.read() avoids hive-partition inference which collides with
    # the symbol/venue columns stored in the file itself.
    tables = [pq.ParquetFile(f).read().cast(schema) for f in files]
    import pyarrow as pa
    merged = pa.concat_tables(tables, promote_options="default")
    import time
    out_path = part_dir / f"compacted-{time.time_ns()}.parquet"
    pq.write_table(
        merged.cast(schema),
        out_path,
        compression="zstd",
        compression_level=6,
        use_dictionary=["symbol", "venue"],
    )
    for f in files:
        f.unlink()
    log.info("compacted dir=%s files=%d rows=%d", part_dir, len(files), merged.num_rows)
    return len(files)


def compact_all() -> int:
    """Compact every partition under raw/ and derived/."""
    total = 0
    for root in (RAW_ROOT, DERIVED_ROOT):
        if not root.exists():
            continue
        for table_dir in root.iterdir():
            if not table_dir.is_dir():
                continue
            table = table_dir.name
            try:
                schema_for(table)
            except KeyError:
                continue
            for part_dir in table_dir.rglob("date=*"):
                if part_dir.is_dir():
                    total += compact_partition(part_dir, table)
    return total
