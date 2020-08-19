import random
from unittest import mock

import pytest

from fennel import App
from fennel.client.state import count_results
from fennel.exceptions import Chaos
from fennel.job import Job
from tests.helpers import all_results, wait_for_results, worker


@pytest.fixture
def app():
    """
    An App with 100 tasks already in the queue. Tasks will randomly fail when executed.
    """
    app = App(
        name="testapp",
        processes=1,
        concurrency=4,
        prefetch_count=1,
        retry_backoff=lambda retries: 0.01,
        maintenance_interval=0.1,
        schedule_interval=0.1,
        heartbeat_interval=0.1,
        heartbeat_timeout=1,
        grace_period=1,
    )

    @app.task
    def example(n):
        if random.random() < 0.2:
            raise Chaos("Random task failure")
        return n

    for i in range(100):
        example.delay(i)

    return app


def test_job_failure(app):
    """
    Ensure that jobs which sometimes randomly fail will be rescheduled and eventually
    succeed. Job failures should not crash the worker.
    """
    with worker(app):
        state = wait_for_results(app, length=100, sleep=0.2, maxwait=4)

    # Tasks have been delivered and executed.
    assert set(r.return_value for r in all_results(app)) == set(range(100))
    assert len(state.queue.messages) == 0

    # Consumer groups behaved properly.
    assert state.queue.info.groups == 1
    assert state.queue.groups[0].pending == 0

    # Nothing in the DLQ.
    assert len(state.dead.messages) == 0

    # Any scheduled tasks completed and removed.
    assert len(state.schedule) == 0


def test_worker_failure(app):
    """
    This test is designed to crash the worker during processing and ensure that when we
    bring up a new worker, the system fully recovers. To achieve this, we're using a
    'sentinel' job which triggers an exception when we try to deserialise it.
    """
    sentinel = 50

    def mocked_deserialise(fields):
        job = Job.deserialise(fields)
        if job.args == [sentinel]:
            raise Chaos("Found sentinel job")
        return job

    with mock.patch("fennel.worker.broker.Job", wraps=Job) as mocked_job:
        mocked_job.deserialise.side_effect = mocked_deserialise

        # The worker crashes on the 50th job execution, wait times out.
        with worker(app):
            state = wait_for_results(app, length=100, sleep=0.1, maxwait=1)

    assert count_results(app) < 100
    assert sentinel not in (r.return_value for r in all_results(app))
    assert len(state.queue.messages) >= 1
    assert len(state.queue.groups) == 1
    assert state.queue.groups[0].pending >= 1
    assert len(state.heartbeats) == 1
    dead_executor_id = state.heartbeats[0].executor_id

    # Complete the job processing with a new worker (must wait long enough for
    # maintenance to happen and the dead worker's pending jobs to be reassigned).
    with worker(app):
        state = wait_for_results(app, length=100, sleep=0.2, maxwait=4)

    assert count_results(app) == 100
    assert set(r.return_value for r in all_results(app)) == set(range(100))
    assert len(state.queue.messages) == 0
    assert state.queue.info.groups == 1
    assert state.queue.groups[0].pending == 0
    assert len(state.heartbeats) == 1
    assert state.heartbeats[0].executor_id != dead_executor_id
