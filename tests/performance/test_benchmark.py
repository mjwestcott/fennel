import random
import time

import pytest

from fennel import App
from fennel.client.state import count_results
from fennel.status import SENT
from fennel.worker import EXIT_COMPLETE, start
from tests.helpers import random_job


def test_worker_loop(benchmark):
    app = App(
        name="benchmark",
        processes=1,
        concurrency=32,
        prefetch_count=1,
        results_enabled=True,
    )

    @app.task
    def sleep():
        time.sleep(0)

    length = 1000

    def setup():
        app.client.flushall()
        with app.client.pipeline(transaction=False) as pipe:
            _load(pipe, app, sleep.name, n=length)
            pipe.execute()

    benchmark.pedantic(
        start,
        args=(app, ),
        kwargs={"exit": EXIT_COMPLETE},
        setup=setup,
        rounds=10,
        iterations=1,
    )
    assert count_results(app) == length


def _load(pipe, app, task, n):
    for i in range(n):
        job = random_job(task=task, args=[], status=SENT)
        pipe.hset(app.keys.status(job), mapping=job.serialise())
        pipe.xadd(app.keys.queue, {"uuid": job.uuid})
