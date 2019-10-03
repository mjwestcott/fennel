import pytest

from fennel import status
from fennel.worker import EXIT_COMPLETE


@pytest.mark.asyncio
async def test_loop_completion(mocker, executor, consumer_id):
    executor.broker.read.return_value = []
    mocker.spy(executor, "_execute")

    assert await executor._loop(consumer_id, exit=EXIT_COMPLETE) == None
    assert not executor._execute.called


@pytest.mark.asyncio
async def test_loop_iteration(mocker, executor, consumer_id, job, failing_job):
    count = 0

    def mock_read(*args, **kwargs):
        nonlocal count
        count += 1

        if count == 1:
            return [(f"0-{i}", job.serialise()) for i in range(0, 4)]
        elif count == 2:
            return [(f"0-{i}", failing_job.serialise()) for i in range(4, 8)]
        else:
            return []

    def mock_executing(uuid):
        if uuid == job.uuid:
            return job.replace(status=status.EXECUTING)
        return failing_job.replace(status=status.EXECUTING)

    executor.broker.read.side_effect = mock_read
    executor.broker.executing.side_effect = mock_executing
    mocker.spy(executor, "_execute")

    assert await executor._loop(consumer_id, exit=EXIT_COMPLETE) == None
    assert executor._execute.call_count == 8
    assert executor.broker.ack_and_store.call_count == 4
    assert executor.broker.ack_and_schedule.call_count == 4
