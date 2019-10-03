import json
from typing import Any, List

from fennel.client.aio.utils import aioclient
from fennel.exceptions import Timeout
from fennel.job import Job
from fennel.status import SENT


@aioclient
async def send(app, job, xid=None) -> List:
    """
    Send the job to the main task queue with the status SENT.
    """
    job = job.replace(status=SENT)
    tx = app.aioclient.multi_exec()
    tx.xadd(app.keys.queue, fields={"uuid": job.uuid}, message_id=xid or "*")
    tx.hmset_dict(app.keys.status(job), job.serialise())
    return await tx.execute()


@aioclient
async def result(app, job, timeout) -> Any:
    """
    Retrieve the result of executing the given `job`, blocking for `timeout` seconds
    before raising Timeout.
    """
    key = app.keys.result(job)
    value = await app.aioclient.brpoplpush(key, key, timeout)

    if value is None:
        raise Timeout
    return json.loads(value)


@aioclient
async def read_dead(app, batchsize=100):
    """
    Iterate over the dead-letter queue and return all job data.
    """
    return [x async for x in _iter_dlq(app, batchsize=batchsize)]


@aioclient
async def replay_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and replay any jobs for which filter(job)
    evaluates to True. The default is to replay all jobs.
    """
    jobs = [x async for x in _iter_dlq(app, batchsize=batchsize)]
    results = []
    tx = app.aioclient.multi_exec()
    for xid, job in jobs:
        if filter(job):
            tx.hset(app.keys.status(job), "status", SENT)
            tx.xadd(app.keys.queue, {"uuid": job.uuid})
            tx.xdel(app.keys.dead, xid)
            results.append((xid, job))
    await tx.execute()
    return results


@aioclient
async def purge_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and delete any jobs for which filter(job)
    evaluates to True. The default is to delete all jobs.
    """
    jobs = [x async for x in _iter_dlq(app, batchsize=batchsize)]
    results = []
    tx = app.aioclient.multi_exec()
    for xid, job in jobs:
        if filter(job):
            tx.delete(app.keys.status(job))
            tx.xdel(app.keys.dead, xid)
            results.append((xid, job))
    await tx.execute()
    return results


async def _iter_dlq(app, batchsize=100):
    """
    Iterate over the dead-letter queue and replay any jobs for which filter(job)
    evaluates to True. The default is to replay all jobs.
    """
    xid = "0"

    while True:
        messages = await app.aioclient.xread(
            [app.keys.dead], latest_ids=[xid], count=batchsize, timeout=None
        )
        if not messages:
            return

        assert all(stream == app.keys.dead for stream, _, _ in messages)
        xid = messages[-1][1]

        tx = app.aioclient.multi_exec()
        for _, xid, fields in messages:
            tx.hgetall(app.keys.status_prefix + f":{fields['uuid']}")
        response = await tx.execute()

        jobs = zip(
            [xid for _, xid, _ in messages],
            [Job.deserialise(fields) for fields in response],
        )

        for xid, job in jobs:
            yield (xid, job)
