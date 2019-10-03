import pytest

from fennel import status
from fennel.client.state import count_results, get_state, get_status


@pytest.mark.asyncio
async def test_ack(app, broker, message, xid, consumer_id, job):
    await broker.read(consumer_id, count=1)
    state = get_state(app)
    assert len(state.queue.messages) == 1
    assert state.queue.groups[0].pending == 1
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, job.uuid) == status.SENT

    await broker.ack(xid, job)
    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert state.queue.groups[0].pending == 0
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, job.uuid) == status.SUCCESS


@pytest.mark.asyncio
async def test_ack_and_store(app, broker, message, xid, consumer_id, job):
    await broker.read(consumer_id, count=1)
    state = get_state(app)
    assert len(state.queue.messages) == 1
    assert state.queue.groups[0].pending == 1
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, job.uuid) == status.SENT

    await broker.ack_and_store(xid, job)
    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert state.queue.groups[0].pending == 0
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 1
    assert get_status(app, job.uuid) == status.SUCCESS


@pytest.mark.asyncio
async def test_ack_and_schedule(
    app, broker, failing_message, xid, consumer_id, failing_job
):
    await broker.read(consumer_id, count=1)
    state = get_state(app)
    assert len(state.queue.messages) == 1
    assert state.queue.groups[0].pending == 1
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, failing_job.uuid) == status.SENT

    await broker.ack_and_schedule(xid, failing_job)
    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert state.queue.groups[0].pending == 0
    assert state.schedule[0].uuid == failing_job.uuid
    assert len(state.schedule) == 1
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, failing_job.uuid) == status.RETRY


@pytest.mark.asyncio
async def test_ack_and_dead(
    app, broker, failing_message, xid, consumer_id, failing_job
):
    await broker.read(consumer_id, count=1)
    state = get_state(app)
    assert len(state.queue.messages) == 1
    assert state.queue.groups[0].pending == 1
    assert len(state.schedule) == 0
    assert len(state.dead.messages) == 0
    assert count_results(app) == 0
    assert get_status(app, failing_job.uuid) == status.SENT

    await broker.ack_and_dead(xid, failing_job)
    state = get_state(app)
    assert len(state.queue.messages) == 0
    assert state.queue.groups[0].pending == 0
    assert len(state.schedule) == 0
    assert state.dead.messages[0].uuid == failing_job.uuid
    assert len(state.dead.messages) == 1
    assert count_results(app) == 1
    assert get_status(app, failing_job.uuid) == status.DEAD
