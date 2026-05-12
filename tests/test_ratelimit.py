import asyncio
import time

from cryptodata.core.ratelimit import TokenBucket


def test_token_bucket_throttles():
    async def go():
        # Bucket configured for 60 req/min => 1 req/s. Burst capacity 2.
        bucket = TokenBucket.per_minute(60, burst=2)
        t0 = time.monotonic()
        # The first 2 should be instantaneous; the 3rd should wait ~1s for refill.
        await bucket.acquire()
        await bucket.acquire()
        await bucket.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.9, f"expected throttling, elapsed={elapsed}"

    asyncio.run(go())
