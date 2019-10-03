from click.testing import CliRunner

from fennel import App
from fennel.cli import dlq, get_object, info, task, worker
from fennel.worker import Executor


def test_worker(mocker, app):
    mocked_start = mocker.patch("fennel.cli.start")
    mocked_get_object = mocker.patch("fennel.cli.get_object")
    mocked_get_object.return_value = app
    assert app.settings.processes != 3
    assert app.settings.concurrency != 15

    CliRunner().invoke(worker, ["--app", "foo:bar", "-p", "3", "-c", "15"])
    assert mocked_get_object.call_args == mocker.call("foo:bar")
    assert mocked_start.call_args[0][0].settings.processes == 3
    assert mocked_start.call_args[0][0].settings.concurrency == 15


def test_dlq(mocker, app):
    mocked_client = mocker.patch("fennel.cli.client")
    mocked_get_object = mocker.patch("fennel.cli.get_object")
    mocked_get_object.return_value = app

    CliRunner().invoke(dlq, ["--app", "foo:bar", "read"])
    assert mocked_client.read_dead.called

    CliRunner().invoke(dlq, ["--app", "foo:bar", "replay"])
    assert mocked_client.replay_dead.called

    CliRunner().invoke(dlq, ["--app", "foo:bar", "purge"])
    assert mocked_client.purge_dead.called


def test_task(mocker, app):
    mocked_get_job = mocker.patch("fennel.cli.get_job")
    mocked_get_object = mocker.patch("fennel.cli.get_object")
    mocked_get_object.return_value = app

    CliRunner().invoke(task, ["--app", "foo:bar", "--uuid", "baz"])
    assert mocked_get_job.call_args == mocker.call(app, "baz")


def test_info(mocker, app):
    mocked_get_state = mocker.patch("fennel.cli.get_state")
    mocked_get_object = mocker.patch("fennel.cli.get_object")
    mocked_get_object.return_value = app

    CliRunner().invoke(info, ["--app", "foo:bar"])
    assert mocked_get_state.call_args[0][0] == app


def test_get_object():
    assert get_object("fennel:App") == App
    assert get_object("fennel.worker:Executor") == Executor
