"""Per-venue token-bucket rate limiter.

Sized to ~70% of published limits (configured in `venues.toml`). On HTTP 429
the caller should bump backoff, not hammer this bucket.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class TokenBucket:
    rate_per_sec: float
    capacity: float
    tokens: float
    last_refill: float

    @classmethod
    def per_minute(cls, requests_per_minute: float, burst: float | None = None) -> TokenBucket:
        rate = requests_per_minute / 60.0
        cap = burst if burst is not None else max(1.0, rate * 2.0)
        return cls(rate_per_sec=rate, capacity=cap, tokens=cap, last_refill=time.monotonic())

    async def acquire(self, n: float = 1.0) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
            self.last_refill = now
            if self.tokens >= n:
                self.tokens -= n
                return
            need = n - self.tokens
            await asyncio.sleep(need / self.rate_per_sec)


class RateLimiterRegistry:
    """One bucket per venue, lazily constructed from `venues.toml`."""

    def __init__(self) -> None:
        self._buckets: dict[str, TokenBucket] = {}

    def get(self, venue: str) -> TokenBucket:
        if venue not in self._buckets:
            from cryptodata.paths import load_venues
            cfg = load_venues().get(venue, {})
            rpm = float(cfg.get("rest_requests_per_minute", 60))
            self._buckets[venue] = TokenBucket.per_minute(rpm)
        return self._buckets[venue]


REGISTRY = RateLimiterRegistry()
