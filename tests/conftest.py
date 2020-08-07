import aioredis
import pytest
import redis

from fennel import App
from fennel.client.actions import send
from fennel.settings import Settings
from fennel.status import DEAD, RETRY
from fennel.utils import base64uuid
from fennel.worker import Broker, Executor
from tests.helpers.job import random_job
from tests.helpers.mocking import AsyncMock


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture(autouse=True)
def __reset(settings):
    client = redis.Redis.from_url(settings.redis_url)
    try:
        client.flushall()
        yield
        client.flushall()
    finally:
        client.close()


@pytest.fixture
def app():
    return App(
        name="testapp",
        processes=1,
        concurrency=2,
        read_timeout=500,
    )


@pytest.fixture
def executor(app):
    e = Executor(app)
    e.broker = AsyncMock()
    return e


@pytest.fixture
async def broker(app):
    b = await Broker.for_app(app)
    assert set(b.scripts) == {"maintenance", "schedule"}
    return b


@pytest.fixture
def task(app):
    @app.task
    def foo(n):
        return n

    return foo


@pytest.fixture
def failing_task(app):
    @app.task
    def foo(n):
        raise Exception("foo")

    return foo


@pytest.fixture
def xid():
    return "1567977050186-0"


@pytest.fixture
def executor_id():
    return f"{base64uuid()}"


@pytest.fixture
def consumer_id(executor_id):
    return f"{executor_id}:1"


@pytest.fixture
def job(task):
    return random_job(task=task.name)


@pytest.fixture
def failing_job(failing_task):
    return random_job(task=failing_task.name)


@pytest.fixture
def unknown_job():
    return random_job()


@pytest.fixture
def retrying_job():
    return random_job(task=failing_task.name, status=RETRY)


@pytest.fixture
def dead_job():
    return random_job(
        task=failing_task.name,
        status=DEAD,
        exception={
            "type": "Exception",
            "args": ["foo"],
        },
    )


@pytest.fixture
def jobs():
    return [(f"0-{i}", random_job()) for i in range(1, 11)]


@pytest.fixture
def message(app, job, xid):
    send(app, job, xid=xid)


@pytest.fixture
def messages(app, jobs):
    for xid, job in jobs:
        send(app, job, xid=xid)


@pytest.fixture
def failing_message(app, failing_job, xid):
    send(app, failing_job, xid=xid)


@pytest.fixture
def value():
    return {"foo": "bar"}


@pytest.fixture
async def async_client(app):
    return await aioredis.create_redis(app.settings.redis_url, encoding="utf-8")
