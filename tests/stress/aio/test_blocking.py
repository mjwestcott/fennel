import asyncio
import random
from statistics import mean

import pytest

from fennel import App
from fennel.client.state import get_heartbeats
from tests.helpers import worker


@pytest.fixture(params=["cpu", "sleep"])
async def app(request):
    app = App(
        name=f"{request.param}",
        interface="async",
        processes=1,
        concurrency=8,
        maintenance_interval=2,
        schedule_interval=2,
        heartbeat_interval=0.1,  # <-- Ensure these arrive
        heartbeat_timeout=2,
    )

    @app.task
    async def sleep(n):
        await asyncio.sleep(5)
        return n

    @app.task
    def cpu(n):
        list(random.random() for _ in range(1_000_000)).sort()
        return n

    for i in range(32):
        if request.param == "cpu":
            await cpu.delay(i)
        else:
            await sleep.delay(i)

    return app


def test_heartbeats_arrive(app):
    """
    Ensure that heartbeats continue to arrive roughly every `heartbeat_interval`
    seconds even with long-running cpu- or io-bound tasks.
    """
    with worker(app) as proc:
        beats = []

        for _ in range(500):
            proc.join(0.01)  # Wait 0.01s 500 times = 5s total

            result = get_heartbeats(app)
            if result:
                assert len(result) == 1
                beats.append(result[0].timestamp)

    beats = sorted(set(beats))
    diff = [(b - a).total_seconds() for a, b in zip(beats, beats[1:])]
    assert 0.1 <= mean(diff) <= 0.5  # The mean difference between two consecutive beats (0.1 perfect)
    assert len(diff) >= 5  # Ensure a minimum number arrived in 5s (50 perfect)
