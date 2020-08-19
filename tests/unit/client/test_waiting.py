import time

import pytest

from fennel import App
from fennel.client import gather, wait
from fennel.exceptions import TaskFailed, Timeout
from tests.helpers import executor


@pytest.fixture
def app():
    return App(
        name="testapp",
        interface="sync",
        processes=1,
        read_timeout=500,
        task_timeout=1,
    )


def test_gather(app):
    @app.task
    def foo(n):
        time.sleep(0.01)
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        gathered = gather(results)

    assert set(gathered) == set(range(10))


def test_gather_timeout(app):
    @app.task(retries=10)
    def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        gathered = gather(results, task_timeout=1)

    for i, x in enumerate(gathered):
        if i == 5:
            assert type(x) == Timeout
        else:
            assert x == i


def test_gather_failure(app):
    @app.task(retries=0)
    def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        gathered = gather(results)

    for i, x in enumerate(gathered):
        if i == 5:
            assert x.original_type == "Exception"
            assert x.original_args == ["foo"]
        else:
            assert x == i


def test_wait(app):
    @app.task
    def foo(n):
        time.sleep(0.01)
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        done, pending = wait(results, timeout=10)

    assert len(pending) == 0
    assert set(f.result() for f in done) == set(range(10))


def test_wait_timeout(app):
    @app.task
    def foo(n):
        time.sleep(0.5)
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        done, pending = wait(results, timeout=0.1)

    assert len(done) == 0
    assert len(pending) == 10


def test_wait_failure(app):
    @app.task(retries=0)
    def foo(n):
        if n == 5:
            raise Exception("foo")
        return n

    with executor(app):
        results = [foo.delay(i) for i in range(10)]
        done, pending = wait(results, timeout=1)

        assert len(done) == 10
        assert len(pending) == 0

    for f in done:
        try:
            f.result()
        except TaskFailed as e:
            assert e.original_type == "Exception"
            assert e.original_args == ["foo"]
