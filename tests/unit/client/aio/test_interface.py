import asyncio
import time

import pytest

from fennel import App
from fennel.exceptions import TaskFailed
from tests.helpers import executor


@pytest.fixture
def app():
    return App(
        name="testapp",
        interface="async",
        processes=1,
        concurrency=2,
    )


@pytest.mark.asyncio
async def test_task(app):
    @app.task
    def foo(n):
        return n

    assert foo(7) == 7

    with executor(app):
        assert (await (await foo.delay(7)).get()) == 7


@pytest.mark.asyncio
async def test_task_with_kwargs(app):
    @app.task(retries=4)
    def foo(n):
        time.sleep(0.01)
        return n

    with executor(app):
        x = await foo.delay(3)
        assert await x.get() == 3

    assert foo.max_retries == 4


@pytest.mark.asyncio
async def test_asyncio(app):
    @app.task
    async def foo(n):
        await asyncio.sleep(0.01)
        return n

    with executor(app):
        x = await foo.delay(3)
        assert await x.get() == 3


@pytest.mark.asyncio
async def test_raise_on_failure(app):
    @app.task(retries=0)
    async def foo(n):
        raise Exception("baz")

    with executor(app):
        x = await foo.delay(3)
        with pytest.raises(TaskFailed) as excinfo:
            await x.get(timeout=1)

    assert excinfo.value.original_type == "Exception"
    assert excinfo.value.original_args == ["baz"]
