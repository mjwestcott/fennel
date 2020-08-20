Guide
=====

Fennel is a task queue library for Python.

It enables you to register your functions as tasks, which you can then enqueue in Redis.
In the background, worker processes will pull tasks from the queue and execute them.
This follows the same basic pattern established by Celery and other task queue systems.


Interfaces
----------

We support both sync and async interfaces to the fennel system. This means you can use
it in traditional web frameworks (e.g Django, Flask), but also newer frameworks built on
top of asyncio (e.g. Starlette, FastAPI, Quart). We default to ``interface='sync'``, but
you can select your interface as an option in your app configuration.

Sync
^^^^

.. code-block:: python

    import time

    from fennel import App
    from fennel.client import gather

    app = App(name='myapp', redis_url='redis://127.0.0.1')


    @app.task
    def foo(n):
        time.sleep(n)
        return n


    x = foo.delay(4)  # Enqueue a task to be executed in the background.
    x.get()           # Waits for completion and returns 4.

Async
^^^^^

.. code-block:: python

    import asyncio

    from fennel import App
    from fennel.client.aio import gather

    app = App(name='myapp', redis_url='redis://127.0.0.1', interface='async')


    @app.task
    async def foo(n):
        await asyncio.sleep(n)
        return n


    x = await foo.delay(4)  # Enqueue a task to be executed in the background.
    await x.get()           # Waits for completion and returns 4.


Two use-cases
-------------

1. Fire-and-forget
^^^^^^^^^^^^^^^^^^

The most common way to use a task queue is to fire off background tasks while processing
requests in a web application. You have some work that needs to happen (e.g. generating
image thumbnails, sending an email), but you don't want the user to wait for it to
complete before returning a response. In this case, it's important that the task
succeeds, but your code will not be waiting to ensure that it does. If failures happen,
you may want to automatically retry the task, or be notified through your monitoring
system.

This is the default scenario expected by Fennel. We support it by automatically retrying
tasks which fail, up to :attr:`fennel.settings.Settings.default_retries` times.
Individual tasks can be configured via the ``retries`` kwarg::

    @app.task(retries=3)
    def foo(n):
        time.sleep(n)
        return n

Retries will occur on a schedule provided by the ``retry_backoff`` function. By default,
Fennel will use an exponential backoff algorithm with jitter to avoid overloading the
workers in case a large number of failures happen simulatenously. (See
:func:`fennel.utils.backoff` for more details.)

If all retries are exhausted and the task still fails, it will be placed in the
'dead-letter queue', see :ref:`Errors` and :ref:`DLQ` below for details.

2. Compose parallel pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a second way to use a task queue: when you have a large amount of work you want
to perform in parallel, perhaps on dedicated high-performance machines. In this case
your code may want to wait for all tasks to complete before moving on to the next batch
of work.

This scenario is also supported by Fennel. You should set ``retries=0`` on your task (or
``default_retries=0`` in your app instance). The waiting primitives we supply are:

1. `gather`, when you want all tasks to complete and collect the results.
2. `wait`, to wait for a specific duration before timing out.

Sync:

.. code-block:: python

    @app.task
    sync def foo(n):
        time.sleep(n)
        return n


    results = [foo.delay(i) for i in range(6)]

    # Waits for completion and returns [1, 2, 3, 4, 5, 6].
    gather(results)

    # Instead, waits for 10 seconds, returns two sets of Futures.
    done, pending = wait(results, timeout=10)

Async:

.. code-block:: python

    @app.task
    async def foo(n):
        await asyncio.sleep(n)
        return n


    results = [await foo.delay(i) for i in range(6)]

    # Waits for completion and returns [1, 2, 3, 4, 5, 6]
    await gather(results)

    # Instead, waits for 10 seconds, returns two sets of Futures.
    done, pending = await wait(results, timeout=10)


.. _Errors:

Error handling
--------------

Fennel considers a task to have failed if any exception is raised during its execution.

If a task has retries enabled, it will be scheduled according by the ``retry_backoff``
function. By default, Fennel will use an exponential backoff algorithm with jitter to
avoid overloading the workers in case a large number of failures happen simulatenously
(see :func:`fennel.utils.backoff` for more details). When retries are exhausted the task
enters the dead-letter queue.

If you attempt to retrieve the result of a task that has failed, fennel will raise
:exc:`fennel.exceptions.TaskFailed` with the original exception information attached::

    >>> @app.task(retries=0)
    >>> async def foo(n):
    ...     raise Exception("baz")
    ...
    >>> x = await foo.delay(3)
    ...
    >>> try:
    ...     result = await x.get()
    >>> except TaskFailed as e:
    ...     assert e.original_type == "Exception"
    ...     assert e.original_args == ["baz"]


.. _DLQ:

The dead-letter queue
---------------------

The DLQ hold tasks which have failed and exhausted all their retry attempts. They now
require manual intervention, for instance you may need to redeploy your applicaiton code
to fix a bug before you replay the failed tasks.

You can read, replay, or purge the contents of the DLQ as follows::

    $ fennel dlq read --app mymodule:myapp
    $ fennel dlq replay --app mymodule:myapp
    $ fennel dlq purge --app mymodule:myapp

If you need more granular control, the Fennel client library also provides functions to
interact with the DLQ programmatically. For example you can replay all jobs matching
certain criteria (using the async client)::

    >>> from fennel.client.aio import replay_dead
    ...
    >>> from myapp.tasks import app  # <-- Your Fennel app instance
    ...
    >>> replay_dead(app, filter=lambda job: job.task == "tasks.mytask")
    [<Job>, ...]

To understand how jobs are represented internally, see :mod:`fennel.job`.


Workers
-------

Workers are launched via the CLI::

    $ fennel worker --app mymodule:myapp

You must specify the Python module and Fennel application instance whose tasks the
worker will execute. See the :doc:`cli` page for more information.


Logging
-------

Fennel supports structured logging out of the box. You can choose whether to use a
human-readable format, or JSON via :attr:`fennel.settings.Settings.log_format`


Limitations
-----------

1. Task args and kwargs must be JSON-serialisable.
2. Return values (if results storage is enabled) must be JSON-serialisable.
3. Processing order is not guaranteed (if you want to ensure all events for a given key
   are processed in-order, see `<https://github.com/mjwestcott/runnel>`_).
4. Tasks will be processed at least once (we acknowledge the underlying messages when a
   task returns without an exception, so any failures before then will happen again when
   retried).

This is similar to the approach taken by Celery, Dramatiq, and task queues in other
languages. As a result, you are advised to follow these best-practices:

* Keep task arguments and return values small (e.g. send the user_id not the User
  model instance)
* Ensure that tasks are idempotent -- if you process them more than once, the same
  result will occur.

Also, Redis is not a highly durable database system -- it's durability is configurable
and limited. You are advised to read the related `parts
<https://redis.io/topics/persistence>`_ of the Redis documentation.

This is a notable section of the `Streams Intro <https://redis.io/topics/streams-intro>`_:

    * AOF must be used with a strong fsync policy if persistence of messages is important
      in your application.
    * By default the asynchronous replication will not guarantee that XADD commands or
      consumer groups state changes are replicated: after a failover something can be
      missing depending on the ability of slaves to receive the data from the master.
    * The WAIT command may be used in order to force the propagation of the changes to a
      set of slaves. However note that while this makes very unlikely that data is lost,
      the Redis failover process as operated by Sentinel or Redis Cluster performs only a
      best effort check to failover to the slave which is the most updated, and under
      certain specific failures may promote a slave that lacks some data.

    So when designing application using Redis streams and consumer groups, make sure to
    understand the semantical properties your application should have during failures,
    and configure things accordingly, evaluating if it is safe enough for your use case.
