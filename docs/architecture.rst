Architecture
============

Fundamentals
------------

Fennel's architecture is similar to other job queue systems like Celery, Dramatiq, RQ::

             +-Redis--------------------+
             |                          |
             |  * The Job Queue         |
             |  * The Dead-Letter Queue |
             |  * The Schedule          |
             |  * Results Storage       |
             |  * Job Metadata          |
             |  * Worker State          |
             |                          |
             +--------------------------+
                 ^                 |
                 | send            | receive
                 | jobs            | jobs
                 |                 |
                 |                 v
    +---------------------+   +---------------------+
    |                     |   |                     |
    |  Your Application   |   |    Fennel Worker    |
    |                     |   |                     |
    +---------------------+   +---------------------+

When your application sends jobs via :func:`fennel.client.Task.delay`, they are
persisted in Redis. Meanwhile a background worker process is waiting to receive jobs and
execute them using the Python function decorated with :func:`fennel.App.task`.

In the normal course of events, the job will be added to a Redis Stream (to notify
workers) and a Redis Hash (to store metadata such as the current status and number of
retries to perform). When execution is finished, the return value will be persisted in a
Redis List (to allow workers to block awaiting it's arrival) and set to expire after a
configurable duration (:attr:`fennel.settings.Settings.result_ttl`).

In case of execution failure (meaning an exception is raised), if the job is configured
for retries it will be scheduled in a Redis Sorted Set (so workers can poll to discover
jobs whose ETA has elapsed). If retries are exhausted, the job will be added to the
dead-letter queue (another Redis Stream). From there, manual intervention is required to
either purge or replay the job.


Redis Streams
-------------

Under the hood, Fennel uses Redis Streams as the fundamental 'queue' data structure.
This provides useful functionality for distributing jobs to individual workers and
keeping track of which tasks have been read and later acknowledged.

Our use of Streams is arguably non-standard. The expectation is that messages accumulate
in the stream, which is periodically trimmed to some maximum length governed by memory
limits. In our case, we don't need to maintain a long history of messages in memory and
we don't want the trim operation to remove any unacknowledged meessages, so we take
advantage of the `XDEL` operation and delete messages when they are acknowledged, like
a traditional job queue.


The Worker
----------

Workers are launched via the :doc:`cli`. Below is a diagram representing a worker with
the settings ``processes=2`` and ``concurrency=8``::

    +-Worker--------------------------------------------------------------------+
    |                                                                           |
    |   +-Executor---------------------+     +-Executor---------------------+   |
    |   |                              |     |                              |   |
    |   |    8x consumer coroutines    |     |    8x consumer coroutines    |   |
    |   |                              |     |                              |   |
    |   |    1x heartbeat coroutine    |     |    1x heartbeat coroutine    |   |
    |   |                              |     |                              |   |
    |   |    1x maintenece coroutine   |     |    1x maintenece coroutine   |   |
    |   |                              |     |                              |   |
    |   |    1x scheduler coroutine    |     |    1x scheduler coroutine    |   |
    |   |                              |     |                              |   |
    |   +------------------------------+     +------------------------------+   |
    |                                                                           |
    +---------------------------------------------------------------------------+

The worker process itself simply spawns 2 executor processes and monitors their health.
The executors themselves run 8 consumer coroutines which are responsible for waiting to
receive jobs from the queue and then executing them. If the job is a coroutine function,
it is awaited in the running asyncio event loop, otherwise it is run in a
`ThreadPoolExecutor` so as not to block the loop.

The other coroutines maintain the health of the system by publshing heartbeats, polling
for scheduled jobs, and responding to the death of other workers or executors.

CPU-bound tasks benefit from multiple processes. We default to running
``multiprocessing.cpu_count()`` executors for this reason. IO-bound tasks will benefit
from high executor concurrency and we default to running 32 consumer coroutines in each
executor.


Job Lifecycle
-------------

Python functions become tasks when they are decorated with :func:`fennel.App.task`. When
they are enqueued using :func:`fennel.client.Task.delay`, they become jobs in the Fennel
queue.

Jobs transition between a number of statuses according to the logic below::

                                                                 +-----------+
                                                                 |           |
                                                                 |           |
                                                              5  |  SUCCESS  |
    +-----------+      +-----------+      +-----------+    +---->|           |
    |           |      |           |      |           |    |     |           |
    |           |  1   |           |  2   |           |    |     +-----------+
    |  UNKNOWN  |----->|   SENT    |----->| EXECUTING |----+
    |           |      |           |      |           |    |
    |           |      |           |      |           |    |     +-----------+
    +-----------+      +-----------+      +-----------+    +---->|           |
                                              |   ^           6  |           |
                                              |   |              |   DEAD    |
                                            3 |   | 4            |           |
                                              |   |              |           |
                                              v   |              +-----------+
                                          +-----------+
                                          |           |
                                          |           |
                                          |   RETRY   |
                                          |           |
                                          |           |
                                          +-----------+

1. Client code sends a job to the queue via :func:`fennel.client.Task.delay`.
2. A worker reads the job from the queue and begins executing it.
3. Execution fails (an exception was raised) and the job's max_retries has not been
   exceeded. The job is placed in the schedule (a Redis sorted set), which workers
   periodically poll.
4. A job is pulled from the schedule and execution is attempted again. (This can
   repeat many times.)
5. Execution succeeds (no exceptions raised).
6. Execution fails (an exception was raised) and retries have been exhausted, so the job
   is now in the dead-letter queue where it will remain until manual intervention (via
   the CLI or client code).

Job status can be retrieved via the AsyncResult object::

    >>> import time
    >>> from fennel import App
    ...
    >>> app = App(name='myapp')
    ...
    >>> @app.task
    >>> def foo(n):
    ...     time.sleep(n)
    ...     return n
    ...
    >>> x = foo.delay(4)
    >>> x.status()
    SENT
    >>> # Wait a few moments.
    >>> x.status()
    EXECUTING
    >>> # Wait for completion.
    >>> x.get()
    4
    >>> x.status()
    SUCCESS
