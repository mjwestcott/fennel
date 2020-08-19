import json

from fennel.exceptions import Timeout
from fennel.job import Job
from fennel.status import SENT


def send(app, job, xid=None):
    """
    Send the job to the main task queue with the status SENT.
    """
    job = job.replace(status=SENT)
    with app.client.pipeline() as pipe:
        pipe.hset(app.keys.status(job), mapping=job.serialise())
        pipe.xadd(app.keys.queue, fields={"uuid": job.uuid}, id=xid or "*")
        return pipe.execute()


def result(app, job, timeout):
    """
    Retrieve the result of executing the given `job`, blocking for `timeout` seconds
    before raising Timeout.
    """
    key = app.keys.result(job)
    value = app.client.brpoplpush(key, key, timeout)

    if value is None:
        raise Timeout
    return json.loads(value)


def read_dead(app, batchsize=100):
    """
    Iterate over the dead-letter queue and return all job data.
    """
    return list(_iter_dlq(app, batchsize=batchsize))


def replay_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and replay any jobs for which filter(job)
    evaluates to True. The default is to replay all jobs.
    """
    jobs = list(_iter_dlq(app, batchsize=batchsize))
    results = []
    with app.client.pipeline() as pipe:
        for xid, job in jobs:
            if filter(job):
                pipe.hset(app.keys.status(job), "status", SENT)
                pipe.xadd(app.keys.queue, {"uuid": job.uuid})
                pipe.xdel(app.keys.dead, xid)
                results.append((xid, job))
        pipe.execute()
    return results


def purge_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and delete any jobs for which filter(job)
    evaluates to True. The default is to delete all jobs.
    """
    jobs = list(_iter_dlq(app, batchsize=batchsize))
    results = []
    with app.client.pipeline() as pipe:
        for xid, job in jobs:
            if filter(job):
                pipe.delete(app.keys.status(job))
                pipe.xdel(app.keys.dead, xid)
                results.append((xid, job))
        pipe.execute()
    return results


def _iter_dlq(app, batchsize=100):
    xid = "0"

    while True:
        response = app.client.xread({app.keys.dead: xid}, count=batchsize)
        if not response:
            return

        for stream, messages in response:
            assert stream == app.keys.dead

            if not messages:
                return
            else:
                xid = messages[-1][0]

            with app.client.pipeline() as pipe:
                for xid, fields in messages:
                    pipe.hgetall(app.keys.status_prefix + f":{fields['uuid']}")
                response = pipe.execute()

            yield from zip(
                [xid for xid, _ in messages],
                [Job.deserialise(fields) for fields in response],
            )
