import pytest

from fennel.client import actions, state
from fennel.exceptions import JobNotFound
from fennel.models import Message
from fennel.status import SENT
from tests.helpers import executor, wait_for_results


def test_module(app, job, xid):
    actions.send(app, job, xid=xid)

    assert state.get_status(app, job.uuid) == SENT
    assert state.get_job(app, job.uuid).status == SENT
    assert state.count_jobs(app) == 1
    assert state.get_messages(app, app.keys.queue) == [Message(id=xid, uuid=job.uuid)]

    with executor(app):
        wait_for_results(app, length=1)

    assert actions.result(app, job, timeout=1)["return_value"] == job.args[0]
    assert state.get_info(app, app.keys.queue).groups == 1
    assert state.get_groups(app, app.keys.queue)[0].name == app.keys.group
    assert state.get_stream(app, app.keys.queue).key == app.keys.queue
    assert state.count_results(app) == 1

    with pytest.raises(JobNotFound):
        state.get_job(app, "nope")
