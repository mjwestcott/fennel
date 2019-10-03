import pytest

from fennel.client.aio.actions import purge_dead, read_dead, replay_dead
from fennel.client.aio.state import get_job, get_state
from fennel.exceptions import JobNotFound
from fennel.status import DEAD, SENT
from fennel.utils import get_aioredis
from tests.helpers import random_job


@pytest.fixture
@pytest.mark.asyncio
async def dead_job(app, failing_job):
    app.aioclient = await get_aioredis(app, app.settings.client_poolsize)

    failing_job.tries = 1
    failing_job.max_retries = 0
    failing_job.status = DEAD
    key = app.keys.status(failing_job)
    await app.aioclient.hmset_dict(key, failing_job.serialise())
    await app.aioclient.xadd(app.keys.dead, {"uuid": failing_job.uuid})

    state = await get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 1
    job = await get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == DEAD


@pytest.mark.asyncio
async def test_read(app, broker, failing_job, dead_job):
    results = await read_dead(app)
    assert len(results) == 1

    state = await get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 1
    job = await get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == DEAD


@pytest.mark.asyncio
async def test_replay(app, broker, failing_job, dead_job):
    results = await replay_dead(app)
    assert len(results) == 1

    state = await get_state(app)
    assert len(state.queue.messages) == 1
    assert len(state.dead.messages) == 0
    job = await get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == SENT


@pytest.mark.asyncio
async def test_purge(app, broker, failing_job, dead_job):
    results = await purge_dead(app)
    assert len(results) == 1

    state = await get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 0

    with pytest.raises(JobNotFound):
        await get_job(app, failing_job.uuid)


@pytest.mark.asyncio
async def test_replay_filter(app, broker, failing_job):
    app.aioclient = await get_aioredis(app, app.settings.client_poolsize)

    @app.task
    def mytask():
        return "1"

    for _ in range(10):
        job = random_job(tries=1, max_retries=0, status=DEAD, task=mytask.name)
        await app.aioclient.hmset_dict(app.keys.status(job), job.serialise())
        await app.aioclient.xadd(app.keys.dead, {"uuid": job.uuid})

    @app.task
    def anothertask():
        return "2"

    for _ in range(5):
        job = random_job(tries=4, max_retries=3, status=DEAD, task=anothertask.name)
        await app.aioclient.hmset_dict(app.keys.status(job), job.serialise())
        await app.aioclient.xadd(app.keys.dead, {"uuid": job.uuid})

    state = await get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 15

    results = await replay_dead(app, filter=lambda job: job.task.endswith("mytask"))
    assert len(results) == 10

    state = await get_state(app)
    assert len(state.queue.messages) == 10
    assert len(state.dead.messages) == 5
