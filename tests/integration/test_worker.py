from fennel import App
from fennel.client.state import count_results
from fennel.status import SENT
from fennel.worker import EXIT_COMPLETE, start
from tests.helpers import random_job


def test_entrypoint():
    app = App(
        name="integration",
        processes=2,
        concurrency=4,
        read_timeout=100,
        prefetch_count=1,
        results_enabled=True,
        maintenance_interval=0.1,
        schedule_interval=0.1,
        heartbeat_interval=0.1,
        grace_period=1,
    )

    @app.task
    def foo(n):
        return n

    length = 50

    with app.client.pipeline(transaction=False) as pipe:
        for i in range(length):
            job = random_job(task=foo.name, args=[i], status=SENT)
            pipe.hmset(app.keys.status(job), job.serialise())
            pipe.xadd(app.keys.queue, fields={"uuid": job.uuid})
        pipe.execute()

    start(app, exit=EXIT_COMPLETE)
    assert count_results(app) == length
