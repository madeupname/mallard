import os
import configparser
import time
from datetime import datetime, timedelta
import threading

# Get config file
config_file = os.getenv('MALLARD_CONFIG')
if not config_file:
    raise Exception("Environment variable MALLARD_CONFIG not set")
config = configparser.ConfigParser()
config.read(config_file)

max_calls = config.getint('tiingo', 'max_calls')
if max_calls <= 0:
    raise Exception("AV_MAX_CALLS must be an integer greater than 0")


class WallClockHourRateLimiter:
    def __init__(self, max_calls_per_hour):
        self.max_calls = max_calls_per_hour
        self.current_hour = None
        self.call_count = 0
        self.lock = threading.Lock()
    
    def can_make_call(self):
        """Check if we can make a call within the current wall clock hour"""
        with self.lock:
            now = datetime.now()
            current_hour = now.replace(minute=0, second=0, microsecond=0)
            
            # Reset counter if we're in a new hour
            if self.current_hour != current_hour:
                self.current_hour = current_hour
                self.call_count = 0
            
            # Check if we can make the call
            if self.call_count < self.max_calls:
                self.call_count += 1
                return True
            return False
    
    def seconds_until_reset(self):
        """Calculate seconds until the rate limit resets (next wall clock hour)"""
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return (next_hour - now).total_seconds()


# Global rate limiter instance
wall_clock_limiter = WallClockHourRateLimiter(max_calls)


class RateLimiterContext:
    def __enter__(self):
        # Attempt to make a call within the rate limit
        while not wall_clock_limiter.can_make_call():
            # If rate limit exceeded, wait until the next wall clock hour
            wait_time = wall_clock_limiter.seconds_until_reset()
            print(f"Rate limit exceeded. Waiting {wait_time:.0f} seconds until next hour...")
            time.sleep(wait_time + 1)  # Add 1 second buffer to ensure we're in the new hour

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing special to do here; could log or handle exceptions if needed
        pass
