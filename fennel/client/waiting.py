import asyncio
from typing import Iterable

from fennel.client.results import AsyncResult


def gather(results: Iterable[AsyncResult], task_timeout=10, return_exceptions=True):
    """
    Multi-result version of .get() -- wait for all tasks to complete and return all of
    their results in order.

    Has the same semantics as `asyncio.gather`.
    """
    async def _gather():
        async def _get(result, timeout):
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, result.get, timeout)

        aws = [_get(r, task_timeout) for r in results]
        return await asyncio.gather(*aws, return_exceptions=return_exceptions)

    return asyncio.run(_gather())


def wait(results: Iterable[AsyncResult], timeout: int, return_when="ALL_COMPLETED"):
    """
    Wait for all tasks to complete and return two sets of Futures (done, pending).

    Has the same semantics as `asyncio.wait`.
    """
    async def _wait():
        async def _get(result):
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, result.get)

        aws = [_get(r) for r in results]
        return await asyncio.wait(aws, timeout=timeout, return_when=return_when)

    return asyncio.run(_wait())
