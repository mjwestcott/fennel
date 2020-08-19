import json
from typing import Any, List

from fennel.exceptions import Timeout
from fennel.job import Job
from fennel.status import SENT


async def send(app, job, xid=None) -> List:
    """
    Send the job to the main task queue with the status SENT.
    """
    job = job.replace(status=SENT)

    async with await app.aioclient.pipeline() as pipe:
        await pipe.xadd(app.keys.queue, entry={"uuid": job.uuid}, stream_id=xid or "*")
        await pipe.hmset(app.keys.status(job), job.serialise())
        return await pipe.execute()


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


async def read_dead(app, batchsize=100):
    """
    Iterate over the dead-letter queue and return all job data.
    """
    return [x async for x in _iter_dlq(app, batchsize=batchsize)]


async def replay_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and replay any jobs for which filter(job)
    evaluates to True. The default is to replay all jobs.
    """
    jobs = [x async for x in _iter_dlq(app, batchsize=batchsize)]
    results = []

    async with await app.aioclient.pipeline() as pipe:
        for xid, job in jobs:
            if filter(job):
                await pipe.hset(app.keys.status(job), "status", SENT)
                await pipe.xadd(app.keys.queue, {"uuid": job.uuid})
                await pipe.xdel(app.keys.dead, xid)
                results.append((xid, job))
        await pipe.execute()
    return results


async def purge_dead(app, filter=lambda job: True, batchsize=100):
    """
    Iterate over the dead-letter queue and delete any jobs for which filter(job)
    evaluates to True. The default is to delete all jobs.
    """
    jobs = [x async for x in _iter_dlq(app, batchsize=batchsize)]
    results = []

    async with await app.aioclient.pipeline() as pipe:
        for xid, job in jobs:
            if filter(job):
                await pipe.delete(app.keys.status(job))
                await pipe.xdel(app.keys.dead, xid)
                results.append((xid, job))
        await pipe.execute()
    return results


async def _iter_dlq(app, batchsize=100):
    """
    Iterate over the dead-letter queue and replay any jobs for which filter(job)
    evaluates to True. The default is to replay all jobs.
    """
    xid = "0"

    while True:
        response = await app.aioclient.xread(
            count=batchsize,
            block=None,
            **{app.keys.dead: xid},
        )
        if not response:
            return

        for stream, messages in response.items():
            assert stream == app.keys.dead

            if not messages:
                return
            else:
                xid = messages[-1][0]
                print(xid)

            async with await app.aioclient.pipeline() as pipe:
                for xid, fields in messages:
                    await pipe.hgetall(app.keys.status_prefix + f":{fields['uuid']}")
                response = await pipe.execute()

            jobs = zip(
                [xid for xid, _ in messages],
                [Job.deserialise(fields) for fields in response],
            )

            for xid, job in jobs:
                yield (xid, job)
