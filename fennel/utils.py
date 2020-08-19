import base64
import importlib
import multiprocessing as mp
import random
import time
import uuid
from contextlib import contextmanager
from typing import Any, Generator

import aredis
import redis

EMPTY = object()  # A unique value used to distinguish emptiness from None


def backoff(retries: int, jitter: bool = True) -> int:
    """
    Compute duration (seconds) to wait before retrying using exponential backoff with
    jitter based on the number of retries a message has already experienced.

    The minimum returned value is 1s
    The maximum returned value is 604800s (7 days)

    With max_retries=9, you will have roughly 30 days to fix and redeploy the the task
    code.

    Parameters
    ----------
    retries : int
        How many retries have already been attemped.
    jitter : bool
        Whether to add random noise to the return value (recommended).

    Notes
    -----
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """
    x = 6**(retries + 1)
    if jitter:
        x = random.randrange(x // 3, x * 2)
    return min(604_800, x)


def base64uuid() -> str:
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode("ascii")


def now() -> float:
    return time.time()


@contextmanager
def duration(logger, event: str, **extra: Any) -> Generator:
    logger.info(f"started-{event}", **extra)
    start = time.perf_counter()
    yield
    duration = time.perf_counter() - start
    logger.info(f"ended-{event}", duration=duration, **extra)


def get_object(name: str) -> Any:
    module_name, instance_name = name.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, instance_name)


def get_redis(app):
    return redis.Redis.from_url(
        app.settings.redis_url,
        decode_responses=True,
    )


def get_aredis(app):
    return aredis.StrictRedis.from_url(
        app.settings.redis_url,
        decode_responses=True,
    )


def get_mp_context():
    # The default changed to 'spawn' in 3.8 on macOS due to https://bugs.python.org/issue33725
    return mp.get_context("fork")
