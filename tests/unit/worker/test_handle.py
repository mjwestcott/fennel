import pytest

from fennel import status


@pytest.mark.asyncio
async def test_ack_results_disabled(mocker, executor, xid, job, consumer_id):
    executor.app.settings.results_enabled = False
    await executor._handle(xid, job.replace(status=status.EXECUTING), consumer_id)

    expected = job.replace(
        status=status.EXECUTING,
        exception={},
        return_value=job.args[0],
        tries=job.tries + 1,
    )
    assert executor.broker.ack.call_args == mocker.call(xid, expected)
    assert not executor.broker.ack_and_store.called
    assert not executor.broker.ack_and_schedule.called
    assert not executor.broker.ack_and_dead.called


@pytest.mark.asyncio
async def test_ack_results_enabled(mocker, executor, xid, job, consumer_id):
    executor.app.settings.results_enabled = True
    await executor._handle(xid, job.replace(status=status.EXECUTING), consumer_id)

    expected = job.replace(
        status=status.EXECUTING,
        exception={},
        return_value=job.args[0],
        tries=job.tries + 1,
    )
    assert executor.broker.ack_and_store.call_args == mocker.call(xid, expected)
    assert not executor.broker.ack.called
    assert not executor.broker.ack_and_schedule.called
    assert not executor.broker.ack_and_dead.called


@pytest.mark.parametrize("results", [True, False])
@pytest.mark.asyncio
async def test_ack_on_failure(mocker, executor, xid, failing_job, consumer_id, results):
    failing_job.max_retries = 1
    executor.app.settings.results_enabled = results
    await executor._handle(
        xid, failing_job.replace(status=status.EXECUTING), consumer_id
    )

    expected = failing_job.replace(
        status=status.EXECUTING,
        exception={"original_type": "Exception", "original_args": ["foo"]},
        return_value=None,
        tries=failing_job.tries + 1,
    )
    assert executor.broker.ack_and_schedule.call_args == mocker.call(xid, expected)
    assert not executor.broker.ack.called
    assert not executor.broker.ack_and_store.called
    assert not executor.broker.ack_and_dead.called


@pytest.mark.parametrize("results", [True, False])
@pytest.mark.asyncio
async def test_ack_and_dead(mocker, executor, xid, failing_job, consumer_id, results):
    failing_job.max_retries = 0
    executor.app.settings.results_enabled = results
    await executor._handle(
        xid, failing_job.replace(status=status.EXECUTING), consumer_id
    )

    expected = failing_job.replace(
        status=status.EXECUTING,
        exception={"original_type": "Exception", "original_args": ["foo"]},
        return_value=None,
        tries=failing_job.tries + 1,
    )
    assert executor.broker.ack_and_dead.call_args == mocker.call(xid, expected)
    assert not executor.broker.ack.called
    assert not executor.broker.ack_and_store.called
    assert not executor.broker.ack_and_schedule.called
