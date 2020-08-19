from fennel import App
from fennel.client.actions import replay_dead
from fennel.client.state import count_results, get_job, get_state
from fennel.exceptions import Chaos
from tests.helpers import wait_for_results, worker


def test_dead_e2e():
    app = App(
        name="testapp",
        retry_backoff=lambda retries: 0.01,
        schedule_interval=0.1,
        heartbeat_interval=0.1,
        maintenance_interval=0.1,
        processes=1,
        concurrency=4,
        prefetch_count=1,
        grace_period=1,
    )

    @app.task(retries=0)
    def example():
        raise Chaos("Task failure")

    x = example.delay()

    # Process the queue, move the failure to the DLQ.
    with worker(app):
        state = wait_for_results(app, length=1, sleep=0.02, maxwait=1)

    assert len(state.dead.messages) == 1
    assert len(state.queue.messages) == 0
    assert get_job(app, x.job.uuid).max_retries == 0
    assert count_results(app) == 1

    # Process the DLQ, move the tasks back to the main queue.
    replay_dead(app)
    state = get_state(app)

    assert len(state.dead.messages) == 0
    assert len(state.queue.messages) == 1
    assert get_job(app, x.job.uuid).max_retries == 0
    assert count_results(app) == 1
