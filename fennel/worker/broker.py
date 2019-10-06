from pathlib import Path
from typing import Any, Callable, Dict, List

import aioredis
import structlog

from fennel import status
from fennel.app import App
from fennel.job import Job
from fennel.keys import Keys
from fennel.utils import get_aioredis, now

logger = structlog.get_logger("fennel.worker")


class Broker:
    def __init__(
        self,
        client: aioredis.Redis,
        keys: Keys,
        results_ttl: int,
        retry_backoff: Callable,
    ):
        """
        The `Broker` is responsible for managing communication with Redis.

        Instances are expected to be used in an event loop context and so should be
        created using :func:`fennel.worker.broker.Broker.for_app`

        Parameters
        ----------
        client : aioredis.Redis
            Used to communicate with Redis via a connection pool.
        keys : fennel.keys.Keys
            A collection of Redis keys configured by the app.
        results_ttl : int
            How many seconds to store task return values in Redis.
        retry_backoff : Callable
            A function of two parameters (retries: int, jitter: bool) used to set the
            time for failed tasks to wait before they are reprocessed.
        """
        self.client = client
        self.keys = keys
        self.results_ttl = results_ttl
        self.retry_backoff = retry_backoff
        self.scripts: Dict[str, str] = {}  # Mapping name to sha1 digest.

    @classmethod
    async def for_app(cls, app: App) -> "Broker":
        """
        Construct the broker in an async context using this method. Actions:
            1. Make a connection pool.
            2. Create the consumer group if it doesn't already exist.
            3. Load Lua scripts.
        """
        broker = cls(
            client=await get_aioredis(app, poolsize=app.settings.concurrency),
            keys=app.keys,
            results_ttl=app.settings.results_ttl,
            retry_backoff=app.settings.retry_backoff,
        )
        await broker.maybe_create_group()
        await broker.register_scripts()
        return broker

    async def read(
        self, consumer: str, count: int, timeout: int = 4000, recover: bool = False
    ) -> List:
        """
        Read `count` jobs from the stream.
        """
        with await self.client as c:
            response = await c.xread_group(
                group_name=self.keys.group,
                consumer_name=consumer,
                streams=[self.keys.queue],
                latest_ids=["0" if recover else ">"],
                count=count,
                timeout=timeout,
            )
        return [(xid, fields) for _, xid, fields in response]

    async def executing(self, uuid: str) -> Job:
        """
        Set the status entry for the given uuid to status.EXECUTING
        and return the associated Job.
        """
        key = self.keys.status_prefix + f":{uuid}"
        tx = self.client.multi_exec()
        tx.hmset_dict(key, status=status.EXECUTING)
        tx.hgetall(key)
        _, fields = await tx.execute()
        return Job.deserialise(fields)

    def _ack(self, tx, xid):
        key = self.keys.queue
        tx.xack(key, self.keys.group, xid)
        tx.xdel(key, xid)

    def _store(self, tx, job):
        key = self.keys.result(job)
        tx.delete(key)
        tx.lpush(key, job.result)
        tx.expire(key, self.results_ttl)

    def _status(self, tx, job, ttl=None):
        key = self.keys.status(job)
        tx.hmset_dict(key, job.serialise())
        if ttl:
            tx.expire(key, ttl)

    def _schedule(self, tx, job, eta):
        tx.zadd(self.keys.schedule, eta, job.uuid)

    def _dead(self, tx, job):
        tx.xadd(self.keys.dead, {"uuid": job.uuid})

    async def ack(self, xid: str, job: Job) -> List:
        """
        Acknowledge receipt of the ID:
            1. Remove it from the consumer's PEL.
            2. Delete the message from the stream.
            3. Set the status entry for the job to status.SUCCESS.
            4. Set expiry for the status entry.
        """
        job = job.replace(status=status.SUCCESS)

        tx = self.client.multi_exec()
        self._ack(tx, xid)
        self._status(tx, job, ttl=self.results_ttl)
        return await tx.execute()

    async def ack_and_store(self, xid: str, job: Job) -> List:
        """
        Acknowledge receipt of the ID and store the result:
            1. Remove it from the consumer's PEL.
            2. Delete the message from the stream.
            3. Delete any existing results (just in case it already exists).
            4. Store the result in a list so that clients can wait via BRPOPLPUSH.
            5. Set expiry for the result.
            6. Set the status entry for the job to status.SUCCESS.
            7. Set expiry for the status entry.
        """
        job = job.replace(status=status.SUCCESS)

        tx = self.client.multi_exec()
        self._ack(tx, xid)
        self._store(tx, job)
        self._status(tx, job, ttl=self.results_ttl)
        return await tx.execute()

    async def ack_and_schedule(self, xid: str, job: Job) -> List:
        """
        Acknowledge receipt of the ID and schedule the job for reprocessing:
            1. Remove it from the consumer's PEL.
            2. Delete the message from the stream.
            3. Add the job to the schedule sorted set so that consumers can poll it.
            4. Set the status entry for the job to status.RETRY.
        """
        job = job.replace(status=status.RETRY)
        eta = now() + int(self.retry_backoff(job.tries))

        tx = self.client.multi_exec()
        self._ack(tx, xid)
        self._schedule(tx, job, eta)
        self._status(tx, job, ttl=None)
        return await tx.execute()

    async def ack_and_dead(self, xid: str, job: Job) -> List:
        """
        Acknowledge receipt of the ID and add the job to the dead-letter queue:
            1. Remove it from the consumer's PEL.
            2. Delete the message from the stream.
            3. Add the message to the DLQ.
            4. Delete any existing results (just in case it already exists).
            5. Store the result in a list so that clients can wait via BRPOPLPUSH.
            6. Set expiry for the result.
            7. Set the status entry for the job to status.DEAD.
        """
        job = job.replace(status=status.DEAD)

        tx = self.client.multi_exec()
        self._ack(tx, xid)
        self._dead(tx, job)
        self._store(tx, job)
        self._status(tx, job, ttl=None)
        return await tx.execute()

    async def process_schedule(self) -> List:
        """
        Retrieve any jobs whose ETA has passed and add them to the stream.
        """
        return await self.client.evalsha(
            digest=self.scripts["schedule"],
            keys=[self.keys.schedule, self.keys.queue],
            args=[now()],
        )

    async def heartbeat(self, executor_id: str) -> List:
        """
        Publish a heartbeat timestamp for the given worker.
        """
        return await self.client.hset(self.keys.heartbeats, executor_id, now())

    async def maintenance(self, threshold: int) -> List:
        """
        Execute the maintenance script:
            1. Find dead consumers (their worker heartbeats are missing for greater than
            settings.heartbeat_timeout).
            2. Delete their pending messages and put them back in the stream for other
            consumers to process.
            3. Delete the dead consumers (and the worker's last heartbeat).
        """
        return await self.client.evalsha(
            digest=self.scripts["maintenance"],
            keys=[self.keys.queue, self.keys.heartbeats],
            args=[self.keys.group, now(), threshold],
        )

    async def create_group(self) -> Any:
        """
        Create the consumer group and the streams.
        """
        for key in [self.keys.queue, self.keys.dead]:
            cmd = ["XGROUP", "CREATE", key, self.keys.group, "0", "MKSTREAM"]
            await self.client.execute(*cmd)

    async def maybe_create_group(self) -> Any:
        """
        Create the consumer group (and the streams) if they don't exist.
        """
        try:
            return await self.create_group()
            logger.debug("group-created", group=self.keys.group)
        except aioredis.errors.ReplyError as e:
            if str(e).startswith("BUSYGROUP"):
                logger.debug("group-exists", group=self.keys.group)
            else:
                raise

    async def register_scripts(self) -> None:
        """
        Load the Lua scripts into Redis.
        """
        for script in (Path(__file__).parent / "lua").glob("*.lua"):
            sha1 = await self.client.script_load(script.read_text())
            self.scripts[script.stem] = sha1
