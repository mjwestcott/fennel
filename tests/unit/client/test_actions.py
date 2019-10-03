import pytest

from fennel.client.actions import purge_dead, read_dead, replay_dead
from fennel.client.state import get_job, get_state
from fennel.exceptions import JobNotFound
from fennel.status import DEAD, SENT
from tests.helpers import random_job


@pytest.fixture
def dead_job(app, failing_job):
    failing_job.tries = 1
    failing_job.max_retries = 0
    failing_job.status = DEAD
    app.client.hmset(app.keys.status(failing_job), failing_job.serialise())
    app.client.xadd(app.keys.dead, {"uuid": failing_job.uuid})

    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 1
    job = get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == DEAD


def test_read(app, broker, failing_job, dead_job):
    results = read_dead(app)
    assert len(results) == 1

    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 1
    job = get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == DEAD


def test_replay(app, broker, failing_job, dead_job):
    results = replay_dead(app)
    assert len(results) == 1

    state = get_state(app)
    assert len(state.queue.messages) == 1
    assert len(state.dead.messages) == 0
    job = get_job(app, failing_job.uuid)
    assert job.tries == 1
    assert job.max_retries == 0
    assert job.status == SENT


def test_purge(app, broker, failing_job, dead_job):
    results = purge_dead(app)
    assert len(results) == 1

    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 0

    with pytest.raises(JobNotFound):
        get_job(app, failing_job.uuid)


def test_replay_filter(app, broker, failing_job):
    @app.task
    def mytask():
        return "1"

    for _ in range(10):
        job = random_job(tries=1, max_retries=0, status=DEAD, task=mytask.name)
        app.client.hmset(app.keys.status(job), job.serialise())
        app.client.xadd(app.keys.dead, {"uuid": job.uuid})

    @app.task
    def anothertask():
        return "2"

    for _ in range(5):
        job = random_job(tries=4, max_retries=3, status=DEAD, task=anothertask.name)
        app.client.hmset(app.keys.status(job), job.serialise())
        app.client.xadd(app.keys.dead, {"uuid": job.uuid})

    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert len(state.dead.messages) == 15

    results = replay_dead(app, filter=lambda job: job.task.endswith("mytask"))
    assert len(results) == 10

    state = get_state(app)
    assert len(state.queue.messages) == 10
    assert len(state.dead.messages) == 5
