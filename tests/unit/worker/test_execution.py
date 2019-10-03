import pytest

from fennel.exceptions import UnknownTask


@pytest.mark.asyncio
async def test_unknown(executor, unknown_job):
    with pytest.raises(UnknownTask):
        await executor._execute(unknown_job)


@pytest.mark.asyncio
async def test_job_succeeded(executor, job):
    x = await executor._execute(job)
    assert x.exception == {}
    assert x.return_value is not None
    assert x.status == job.status
    assert x.tries == job.tries


@pytest.mark.asyncio
async def test_job_failed(executor, failing_job):
    x = await executor._execute(failing_job)
    assert x.exception == {"original_type": "Exception", "original_args": ["foo"]}
    assert x.return_value is None
    assert x.status == failing_job.status
    assert x.tries == failing_job.tries
