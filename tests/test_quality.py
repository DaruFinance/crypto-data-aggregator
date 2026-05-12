"""Data-quality checks + scorecard."""
import numpy as np
import pandas as pd

from cryptodata.quality import grade_for, run_checks
from cryptodata.quality.checks import Severity


def _bars(n=600, start_s=1_700_000_000):
    ts = (np.arange(n) + start_s) * 1_000_000_000
    px = 100.0 + np.cumsum(np.random.default_rng(0).normal(0, 0.01, n))
    return pd.DataFrame({
        "ts_ns": ts, "open": px, "high": px + 0.05, "low": px - 0.05, "close": px,
        "volume": np.ones(n), "vwap": px, "trades": np.ones(n, dtype="int32"),
        "sources_mask": np.zeros(n, dtype="int32"),
    })


def test_clean_bars_no_issues():
    issues = run_checks(bars=_bars())
    # a clean synthetic series may legitimately flag a flatline only if rng repeats;
    # assert no MAJOR/CRITICAL issues.
    assert all(i.severity < Severity.MAJOR for i in issues), [i.to_dict() for i in issues]


def test_ohlc_violation_flagged():
    b = _bars(100)
    b.loc[10, "high"] = b.loc[10, "low"] - 1.0    # high < low
    b.loc[20, "volume"] = -5.0                    # negative volume
    issues = run_checks(bars=b)
    checks = {i.check for i in issues}
    assert "bar.high_below_low" in checks
    assert "bar.negative_volume" in checks
    assert any(i.severity == Severity.CRITICAL for i in issues)


def test_gap_detected():
    b = _bars(200)
    # drop a 5-minute block in the middle → a 300s gap
    b = pd.concat([b.iloc[:100], b.iloc[100:].assign(ts_ns=lambda d: d["ts_ns"] + 300 * 1_000_000_000)],
                  ignore_index=True)
    issues = run_checks(bars=b)
    gap = [i for i in issues if i.check == "bar.gaps"]
    assert gap and gap[0].severity >= Severity.MINOR


def test_trade_duplicate_id_flagged():
    n = 50
    base = 1_700_000_000 * 1_000_000_000
    df = pd.DataFrame({
        "ts_ns": [base + i * 1_000_000 for i in range(n)],
        "recv_ns": [base + i * 1_000_000 for i in range(n)],
        "price": [100.0 + (i % 3) for i in range(n)],
        "size": [0.1] * n, "side": [1] * n,
        "trade_id": [str(i % 10) for i in range(n)],   # heavy duplication
    })
    issues = run_checks(trades=df)
    assert any(i.check == "trade.duplicate_trade_id" for i in issues)


def test_quote_crossed_book_flagged():
    n = 30
    base = 1_700_000_000 * 1_000_000_000
    df = pd.DataFrame({
        "ts_ns": [base + i * 1_000_000 for i in range(n)],
        "bid_px": [100.0] * n, "ask_px": [100.0] * (n - 1) + [99.0],   # last one crossed
        "bid_sz": [1.0] * n, "ask_sz": [1.0] * n,
    })
    issues = run_checks(quotes=df)
    assert any(i.check == "quote.crossed_book" for i in issues)


def test_agg_outside_venue_range_flagged():
    s = 1_700_000_000
    ts = (np.arange(5) + s) * 1_000_000_000
    def mk(close):
        return pd.DataFrame({"ts_ns": ts, "open": close, "high": close, "low": close, "close": close,
                             "volume": np.ones(5), "vwap": close, "trades": np.ones(5, "int32"),
                             "sources_mask": np.zeros(5, "int32")})
    binance = mk(np.array([100.0] * 5))
    coinbase = mk(np.array([100.1] * 5))
    # agg deliberately wrong: way above both
    agg = mk(np.array([200.0] * 5))
    issues = run_checks(agg_bars=agg, per_venue_bars={"binance": binance, "coinbase": coinbase})
    assert any(i.check == "agg.outside_venue_range" for i in issues)


def test_grades():
    assert grade_for(99) == "A"
    assert grade_for(90) == "B"
    assert grade_for(75) == "C"
    assert grade_for(55) == "D"
    assert grade_for(10) == "F"


# --------------------------------------------------------------------------- #
# scorecard: the agg series is graded on coverage of the contributing venues,
# not on raw second-by-second density, and a thin venue is never penalized for sparsity.
# --------------------------------------------------------------------------- #

def _write_bar_seconds(symbol, venue, date, seconds, px=100.0):
    from cryptodata.storage.parquet import write_dataframe
    base = int(pd.Timestamp(date + " 00:00:00", tz="UTC").value)
    rows = [{"ts_ns": base + s * 1_000_000_000, "symbol": symbol, "venue": venue,
             "open": px, "high": px, "low": px, "close": px, "volume": 1.0, "vwap": px,
             "trades": 1, "sources_mask": 0, "ingested_at_ns": base} for s in seconds]
    write_dataframe("bars_1s", rows, symbol=symbol, venue=venue, date=date)


def test_agg_scored_on_coverage_of_union_not_density():
    from cryptodata.quality.report import score_day
    from cryptodata.storage.duckdb_views import init_db
    init_db()
    date = "2026-05-12"
    # venue A dense (every second 0..99), venue B + C sparse — union of traded seconds = {0..99}
    _write_bar_seconds("BTC-USD", "binance", date, range(100))
    _write_bar_seconds("BTC-USD", "coinbase", date, [0, 50, 99])
    _write_bar_seconds("BTC-USD", "kraken", date, [10, 20, 30])
    # agg covers the whole union
    _write_bar_seconds("BTC-USD", "agg", date, range(100))

    card_agg = score_day("BTC-USD", "agg", date)
    assert card_agg["score_basis"] == "coverage_of_contributing_venues"
    assert card_agg["agg_coverage_of_union_pct"] == 100.0
    assert card_agg["score_base_pct"] == 100.0
    assert card_agg["grade"] == "A" and card_agg["score"] == 100.0

    # the sparse single venue: density is tiny but the series is correct → graded A, not penalized
    card_b = score_day("BTC-USD", "coinbase", date)
    assert card_b["completeness_pct"] < 5.0          # ~3 bars over a 100s span
    assert card_b["score_basis"] == "clean_baseline"
    assert card_b["grade"] == "A"


def test_agg_penalised_when_it_misses_seconds_the_venues_had():
    from cryptodata.quality.report import score_day
    from cryptodata.storage.duckdb_views import init_db
    init_db()
    date = "2026-05-13"
    _write_bar_seconds("ETH-USD", "binance", date, range(100))     # union = {0..99}
    _write_bar_seconds("ETH-USD", "kraken", date, [0, 99])
    _write_bar_seconds("ETH-USD", "agg", date, range(90))          # agg drops 10 of the 100 union seconds
    card = score_day("ETH-USD", "agg", date)
    assert card["agg_coverage_of_union_pct"] == 90.0
    assert card["score_base_pct"] == 90.0
    assert card["grade"] == "B" and card["score"] == 90.0
