"""Live ingest entry point. Runs forever.

Usage:
    python -m scripts.run_ingest

Stops on Ctrl-C. All worker tasks are cancelled, the writer flushes pending
batches, then the process exits.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from cryptodata.core.ingest import run_ingest_forever


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run_ingest_forever())
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
