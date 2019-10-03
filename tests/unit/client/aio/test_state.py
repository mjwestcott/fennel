import pytest

from fennel.client.aio import actions, state
from fennel.exceptions import JobNotFound
from fennel.models import Message
from fennel.status import SENT
from tests.helpers import executor, wait_for_results


@pytest.mark.asyncio
async def test_module(app, job, xid):
    await actions.send(app, job, xid=xid)

    assert (await state.get_status(app, job.uuid)) == SENT
    assert (await state.get_job(app, job.uuid)).status == SENT
    assert (await state.count_jobs(app)) == 1
    assert (await state.get_messages(app, app.keys.queue)) == [
        Message(id=xid, uuid=job.uuid)
    ]

    with executor(app):
        wait_for_results(app, length=1)

    assert (await actions.result(app, job, timeout=1))["return_value"] == job.args[0]
    assert (await state.get_info(app, app.keys.queue)).groups == 1
    assert (await state.get_groups(app, app.keys.queue))[0].name == app.keys.group
    assert (await state.get_stream(app, app.keys.queue)).key == app.keys.queue
    assert (await state.count_results(app)) == 1

    with pytest.raises(JobNotFound):
        await state.get_job(app, "nope")
