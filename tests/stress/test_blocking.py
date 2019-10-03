import time

import numpy as np
import pytest

from fennel import App
from fennel.client.state import get_heartbeats
from tests.helpers import worker


@pytest.fixture(params=["cpu", "sleep"])
def app(request):
    app = App(
        name=f"{request.param}",
        processes=1,
        concurrency=8,
        maintenence_interval=2,
        schedule_interval=2,
        heartbeat_interval=0.1,  # <-- Ensure these arrive
        heartbeat_timeout=2,
    )

    @app.task
    def sleep(n):
        time.sleep(1)
        return n

    @app.task
    def cpu(n):
        np.sort(np.random.random(7_000_000))
        return n

    for i in range(32):
        if request.param == "cpu":
            cpu.delay(i)
        else:
            sleep.delay(i)

    return app


def test_heartbeats_arrive(app):
    """
    Ensure that heartbeats continue to arrive roughly every `heartbeat_interval`
    seconds even with long-running tasks.
    """
    with worker(app) as proc:
        beats = []

        for _ in range(200):
            proc.join(0.01)  # Wait 0.01s 200 times = 2s total

            result = get_heartbeats(app)
            if result:
                assert len(result) == 1
                beats.append(result[0].timestamp)

    beats = sorted(set(beats))
    diff = [(b - a).total_seconds() for a, b in zip(beats, beats[1:])]
    assert max(diff) <= 0.25  # The biggest difference between two consecutive beats
    assert len(diff) >= 15  # Ensure a minimum number arrived in 2s
