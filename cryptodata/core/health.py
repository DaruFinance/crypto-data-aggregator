"""Per-(venue, symbol, stream) health tracker.

Tracks recent message rates. If 5-min trade rate < 10% of trailing 1h baseline,
mark the stream DOWN in venue_status. Aggregator reads venue_status to skip
DOWN venues when building agg bars.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class _RateWindow:
    timestamps: deque = field(default_factory=lambda: deque(maxlen=20000))

    def record(self, ns: int | None = None) -> None:
        self.timestamps.append(ns if ns is not None else time.time_ns())

    def rate_per_sec(self, window_ns: int) -> float:
        if not self.timestamps:
            return 0.0
        now = time.time_ns()
        cutoff = now - window_ns
        count = sum(1 for t in self.timestamps if t >= cutoff)
        return count / (window_ns / 1_000_000_000)


class HealthTracker:
    """Thread-safe in-memory tracker. The orchestrator periodically dumps
    summary rows into the `venue_status` DuckDB table."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._windows: dict[tuple[str, str, str], _RateWindow] = defaultdict(_RateWindow)
        self._down: dict[tuple[str, str, str], bool] = {}

    def record(self, venue: str, symbol: str, stream: str) -> None:
        key = (venue, symbol, stream)
        with self._lock:
            self._windows[key].record()

    def evaluate(self, *, recent_window_s: int = 300, baseline_window_s: int = 3600, threshold_ratio: float = 0.10) -> dict[tuple[str, str, str], dict]:
        """Return current status per (venue, symbol, stream).

        `up=True` unless the recent rate is < threshold_ratio * baseline rate.
        """
        report: dict[tuple[str, str, str], dict] = {}
        with self._lock:
            for key, win in self._windows.items():
                recent = win.rate_per_sec(recent_window_s * 1_000_000_000)
                baseline = win.rate_per_sec(baseline_window_s * 1_000_000_000)
                up = baseline == 0 or recent >= threshold_ratio * baseline
                self._down[key] = not up
                report[key] = {"recent_per_s": recent, "baseline_per_s": baseline, "up": up}
        return report

    def is_down(self, venue: str, symbol: str, stream: str) -> bool:
        return self._down.get((venue, symbol, stream), False)


TRACKER = HealthTracker()
