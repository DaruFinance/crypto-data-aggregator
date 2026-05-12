"""crypto-data-aggregator — multi-source crypto market data: a consolidated tape
across spot venues (trade-level VWAP, robust outlier filtering, full provenance),
perpetual funding + open interest, L2 order-book snapshots, a data-quality scorecard,
and a point-in-time-correct, pandas-first query API over partitioned Parquet."""
from cryptodata.query.bars import get_bars
from cryptodata.query.books import book_quality, book_top, get_book_snapshots
from cryptodata.query.funding import get_funding
from cryptodata.query.meta import get_open_interest, list_symbols, venue_status
from cryptodata.query.quotes import get_quotes
from cryptodata.query.ref import get_ref_bars
from cryptodata.query.trades import get_trades

__version__ = "0.2.0"
__all__ = [
    "get_bars",
    "get_trades",
    "get_quotes",
    "get_funding",
    "get_open_interest",
    "get_ref_bars",
    "get_book_snapshots",
    "book_top",
    "book_quality",
    "list_symbols",
    "venue_status",
]
