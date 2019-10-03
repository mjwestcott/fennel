import asyncio

import pytest

from fennel import App
from fennel.client.aio import gather, wait
from fennel.exceptions import TaskFailed, Timeout
from tests.helpers import executor


@pytest.fixture
def app():
    return App(
        name="testapp", interface="async", processes=1, concurrency=2, task_timeout=1
    )


@pytest.mark.asyncio
async def test_gather(app):
    @app.task
    async def foo(n):
        await asyncio.sleep(0.01)
        return n

    with executor(app):
        results = [await foo.delay(i) for i in range(10)]
        gathered = await gather(results)

    assert set(gathered) == set(range(10))


@pytest.mark.asyncio
async def test_gather_timeout(app):
    @app.task(retries=10)
    async def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = [await foo.delay(i) for i in range(10)]
        gathered = await gather(results, task_timeout=1)

    for i, x in enumerate(gathered):
        if i == 5:
            assert type(x) == Timeout
        else:
            assert x == i


@pytest.mark.asyncio
async def test_gather_failure(app):
    @app.task(retries=0)
    async def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = [await foo.delay(i) for i in range(10)]
        gathered = await gather(results)

    for i, x in enumerate(gathered):
        if i == 5:
            assert x.original_type == "Exception"
            assert x.original_args == ["foo"]
        else:
            assert x == i


@pytest.mark.asyncio
async def test_wait(app):
    @app.task
    async def foo(n):
        await asyncio.sleep(0.01)
        return n

    with executor(app):
        results = [await foo.delay(i) for i in range(10)]
        done, pending = await wait(results, timeout=10)

    assert len(pending) == 0
    assert set(f.result() for f in done) == set(range(10))


@pytest.mark.asyncio
async def test_wait_timeout(app):
    @app.task
    async def foo(n):
        await asyncio.sleep(1)
        return n

    with executor(app):
        results = [await foo.delay(i) for i in range(10)]
        done, pending = await wait(results, timeout=0.2)

    assert len(done) == 0
    assert len(pending) == 10


@pytest.mark.asyncio
async def test_wait_failure(app):
    @app.task(retries=0)
    async def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = {await foo.delay(i): i for i in range(10)}
        done, pending = await wait(results, timeout=0.4)

    assert len(done) == 10
    assert len(pending) == 0

    for f in done:
        try:
            await f
        except TaskFailed as e:
            assert e.original_type == "Exception"
            assert e.original_args == ["foo"]
