import asyncio

import pytest

from fennel import App
from fennel.client.state import get_state
from fennel.exceptions import TaskFailed
from tests.helpers import wait_for_results, worker


@pytest.mark.parametrize("grace_period", [0.5, 2])
def test_cancellation(grace_period):
    app = App(
        name="testapp",
        default_retries=0,
        processes=1,
        concurrency=1,
        prefetch_count=1,
        read_timeout=50,
        grace_period=grace_period,
    )

    @app.task
    async def example():
        await asyncio.sleep(1)
        return True

    x = example.delay()

    with worker(app):
        wait_for_results(app, length=1, sleep=0.02, maxwait=0.5)

    if grace_period == 0.5:
        # Ensure that the task was forcefully cancelled since the grace period
        # expired before completion.
        state = get_state(app)
        assert len(state.dead.messages) == 1
        assert len(state.queue.messages) == 0
        with pytest.raises(TaskFailed):
            x.get()
    else:
        # Ensure that the task had a chance to complete (ack and store its result) even
        # though the executor received SIGTERM partway through.
        state = get_state(app)
        assert len(state.dead.messages) == 0
        assert len(state.queue.messages) == 0
        assert x.get() == True
