import os

from limits import RateLimitItemPerHour, storage
from limits.strategies import MovingWindowRateLimiter
import time

# Define the rate limit (e.g., 2 calls per second)
max_calls = 9500
# max_calls = int(max_calls)
if max_calls <= 0:
    raise Exception("AV_MAX_CALLS must be an integer greater than 0")
rate_limit = RateLimitItemPerHour(max_calls)
memory_storage = storage.MemoryStorage()
limiter = MovingWindowRateLimiter(memory_storage)


class RateLimiterContext:
    def __enter__(self):
        # Attempt to hit the rate limiter for the given key
        while not limiter.hit(rate_limit, "global"):
            # If rate limit exceeded, sleep for a short period before retrying
            time.sleep(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing special to do here; could log or handle exceptions if needed
        pass
