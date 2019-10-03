import pytest

from fennel.job import Job
from fennel.status import EXECUTING, SUCCESS
from fennel.utils import base64uuid


@pytest.fixture
def uuid():
    return base64uuid()


@pytest.fixture
def job(uuid):
    return Job(
        task="test",
        args=["foo"],
        kwargs={"bar": "baz"},
        tries=0,
        max_retries=1,
        exception={},
        return_value=None,
        status=EXECUTING,
        uuid=uuid,
    )


@pytest.fixture
def as_json(uuid):
    return (
        f"{{"
        f'"task": "test", '
        f'"args": ["foo"], '
        f'"kwargs": {{"bar": "baz"}}, '
        f'"tries": 0, '
        f'"max_retries": 1, '
        f'"exception": {{}}, '
        f'"return_value": null, '
        f'"status": "{EXECUTING}", '
        f'"uuid": "{uuid}"'
        f"}}"
    )


@pytest.fixture
def as_dict(uuid):
    return {
        "task": "test",
        "args": '["foo"]',
        "kwargs": '{"bar": "baz"}',
        "tries": 0,
        "max_retries": 1,
        "exception": "{}",
        "return_value": "null",
        "status": f"{EXECUTING}",
        "uuid": f"{uuid}",
    }


def test_increment(job):
    assert job.tries == 0
    assert job.increment().tries == 1
    assert job.increment().increment().tries == 2


def test_replace(job):
    assert job.status == EXECUTING
    assert job.replace(status=SUCCESS).status == SUCCESS
    assert job.replace(task="foo").task == "foo"


def test_to_json(job, as_json):
    assert job.to_json() == as_json


def test_from_json(job, as_json):
    assert Job.from_json(as_json) == job


def test_serialise(job, as_dict):
    assert job.serialise() == as_dict


def test_deserialise(job, as_dict):
    assert Job.deserialise(as_dict) == job


def test_result(job):
    assert job.result == f'{{"return_value": null, "exception": {{}}}}'
