from datetime import datetime

import pytest
from freezegun import freeze_time

from fennel.client.state import (
    count_results,
    get_heartbeats,
    get_state,
    get_status,
    get_stream,
)
from fennel.status import EXECUTING, RETRY


@pytest.mark.asyncio
async def test_read(app, broker, messages, consumer_id, jobs):
    assert get_stream(app, app.keys.queue).groups[0].pending == 0

    found = [(xid, dict(m)) for xid, m in await broker.read(consumer_id, count=10)]
    expected = [(xid, {"uuid": job.uuid}) for xid, job in jobs]
    assert found == expected
    assert get_stream(app, app.keys.queue).groups[0].pending == 10


@pytest.mark.asyncio
async def test_process_schedule(app, broker, jobs, messages):
    with freeze_time("2020-01-01"):
        for xid, job in jobs:
            job.status = EXECUTING
            await broker.ack_and_schedule(xid, job)

        state = get_state(app)
        assert len(state.queue.messages) == 0
        assert state.queue.groups[0].pending == 0
        assert len(state.schedule) == 10
        assert len(state.dead.messages) == 0
        assert count_results(app) == 0
        assert all(get_status(app, job.uuid) == RETRY for _, job in jobs)

    with freeze_time("1970-01-01"):
        # We're before the schedule time, no jobs should have moved.
        scheduled = await broker.process_schedule()
        assert len(scheduled) == 0

        state = get_state(app)
        assert len(state.queue.messages) == 0
        assert state.queue.groups[0].pending == 0
        assert len(state.schedule) == 10
        assert len(state.dead.messages) == 0
        assert count_results(app) == 0
        assert all(get_status(app, job.uuid) == RETRY for _, job in jobs)

    with freeze_time("2100-01-01"):
        # After the schedule time, all jobs should be moved to the task queue.
        scheduled = await broker.process_schedule()
        assert len(scheduled) == 10

        state = get_state(app)
        assert len(state.queue.messages) == 10
        assert {x.uuid for x in state.queue.messages} == {y.uuid for (_, y) in jobs}
        assert state.queue.groups[0].pending == 0
        assert len(state.schedule) == 0
        assert len(state.dead.messages) == 0
        assert count_results(app) == 0
        assert all(get_status(app, job.uuid) == RETRY for _, job in jobs)


@pytest.mark.asyncio
async def test_heartbeat(app, broker, executor_id):
    with freeze_time("2020-01-01"):
        await broker.heartbeat(executor_id)

        beats = get_heartbeats(app)
        assert len(beats) == 1
        assert beats[0].timestamp == datetime(2020, 1, 1)
        assert beats[0].executor_id == executor_id


@pytest.mark.asyncio
async def test_maintenance(app, broker, messages, executor_id, consumer_id):
    with freeze_time("2020-01-01 00:00:00"):
        await broker.read(consumer_id, count=5)
        await broker.heartbeat(executor_id)

        state = get_state(app)
        xids = {m.id for m in state.queue.messages}
        assert len(state.queue.messages) == 10
        assert state.queue.groups[0].pending == 5
        assert state.queue.groups[0].consumers == 1
        assert len(state.schedule) == 0
        assert len(state.dead.messages) == 0
        assert len(state.heartbeats) == 1
        assert count_results(app) == 0

    with freeze_time("2020-01-01 00:00:30"):  # 30 seconds later, not passed threshold
        await broker.maintenance(threshold=59)

        state = get_state(app)
        new_xids = {m.id for m in state.queue.messages}
        assert new_xids == xids
        assert len(state.queue.messages) == 10
        assert state.queue.groups[0].pending == 5
        assert state.queue.groups[0].consumers == 1
        assert len(state.schedule) == 0
        assert len(state.dead.messages) == 0
        assert len(state.heartbeats) == 1
        assert count_results(app) == 0

    with freeze_time("2020-01-01 00:01:00"):  # 1 minute later, passed threshold
        await broker.maintenance(threshold=59)

        state = get_state(app)
        new_xids = {m.id for m in state.queue.messages}
        assert new_xids != xids
        assert len(state.queue.messages) == 10
        assert state.queue.groups[0].pending == 0
        assert state.queue.groups[0].consumers == 0
        assert len(state.schedule) == 0
        assert len(state.dead.messages) == 0
        assert len(state.heartbeats) == 0
        assert count_results(app) == 0
