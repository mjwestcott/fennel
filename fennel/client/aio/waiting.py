import asyncio
from typing import Iterable

from fennel.client.aio.results import AsyncResult


async def gather(results: Iterable[AsyncResult], task_timeout=10, return_exceptions=True):
    """
    Multi-result version of .get() -- wait for all tasks to complete and return all of
    their results in order.

    Has the same semantics as `asyncio.gather`.
    """
    aws = [r.get(timeout=task_timeout) for r in results]
    return await asyncio.gather(*aws, return_exceptions=return_exceptions)


async def wait(results: Iterable[AsyncResult], timeout: int, return_when="ALL_COMPLETED"):
    """
    Wait for all tasks to complete and return two sets of Futures (done, pending).

    Has the same semantics as `asyncio.wait`.
    """
    aws = [r.get() for r in results]
    return await asyncio.wait(aws, timeout=timeout, return_when=return_when)
