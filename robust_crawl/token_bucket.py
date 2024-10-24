import time
import logging
from gevent import sleep
from gevent.lock import Semaphore
from collections import defaultdict

from .singleton_meta import SingletonMeta

logger = logging.getLogger(__name__)


class TokenBucket(metaclass=SingletonMeta):
    def __init__(self, config=None):
        self.default_tokens_per_minute = config.get("tokens_per_minute", 5)
        self.default_bucket_capacity = config.get(
            "bucket_capacity", self.default_tokens_per_minute
        )
        url_specific_tokens = config.get("url_specific_tokens", {})
        self.token_buckets = defaultdict(
            lambda: {
                "tokens": self.default_bucket_capacity,
                "last_refresh": time.time(),
            }
        )
        self.token_bucket_lock = defaultdict(lambda: Semaphore(1))
        self.url_specific_tokens = url_specific_tokens or {}

    def get_tokens(self, *args):
        keywords = [str(k) for k in args]
        keyword = "_".join(keywords)

        with self.token_bucket_lock[keyword]:
            bucket = self.token_buckets[keyword]
            current_time = time.time()
            time_passed = current_time - bucket["last_refresh"]

            # Find the most specific tokens_per_minute and bucket_capacity
            tokens_per_minute = self.default_tokens_per_minute
            bucket_capacity = self.default_bucket_capacity
            for key in keywords:
                for k in self.url_specific_tokens.keys():
                    if k in key:
                        tokens_per_minute = self.url_specific_tokens[k].get(
                            "tokens_per_minute", tokens_per_minute
                        )
                        bucket_capacity = self.url_specific_tokens[k].get(
                            "bucket_capacity", bucket_capacity
                        )

            new_tokens = int(time_passed * (tokens_per_minute / 60))

            if new_tokens > 0:
                bucket["tokens"] = min(bucket["tokens"] + new_tokens, bucket_capacity)
                bucket["last_refresh"] = current_time

            if bucket["tokens"] < 1:
                sleep_time = (1 - bucket["tokens"]) / (tokens_per_minute / 60)
                logger.debug(
                    f"{keyword} bucket is empty, sleeping for {sleep_time} seconds"
                )
                sleep(sleep_time)
                bucket["tokens"] = 1

            bucket["tokens"] -= 1
