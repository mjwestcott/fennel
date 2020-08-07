import random
import time

import pytest

from fennel import App
from fennel.client.state import count_results
from fennel.status import SENT
from fennel.worker import EXIT_COMPLETE, start
from tests.helpers import random_job


@pytest.mark.parametrize("task", ["cpu", "sleep"])
@pytest.mark.parametrize("processes", [1, 4])
@pytest.mark.parametrize("concurrency", [32, 64])
def test_worker_loop(benchmark, concurrency, processes, task):
    app = App(
        name="benchmark",
        processes=processes,
        concurrency=concurrency,
        prefetch_count=1,
        results_enabled=True,
    )

    @app.task
    def sleep():
        time.sleep(random.expovariate(2))

    @app.task
    def cpu():
        list(random.random() for _ in range(500_000)).sort()

    length = 100

    def setup():
        app.client.flushall()
        with app.client.pipeline(transaction=False) as pipe:
            _load(pipe, app, cpu.name if task == "cpu" else sleep.name, n=length)
            pipe.execute()

    benchmark.pedantic(
        start,
        args=(app, ),
        kwargs={"exit": EXIT_COMPLETE},
        setup=setup,
        rounds=3,
        iterations=1,
    )
    assert count_results(app) == length


def _load(pipe, app, task, n):
    for i in range(n):
        job = random_job(task=task, args=[], status=SENT)
        pipe.hmset(app.keys.status(job), job.serialise())
        pipe.xadd(app.keys.queue, {"uuid": job.uuid})
