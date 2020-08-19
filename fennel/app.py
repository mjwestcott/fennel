from functools import wraps
from typing import Any, Callable, Dict

import structlog

from fennel.client import Task as SyncTask
from fennel.client.aio import Task as AsyncTask
from fennel.keys import Keys
from fennel.logging import init_logging
from fennel.settings import Settings
from fennel.utils import EMPTY, get_aredis, get_redis

logger = structlog.get_logger("fennel.client")


class App:
    def __init__(self, name: str, **kwargs):
        """
        The app is the main abstraction provided by Fennel. Python functions are
        decorated via ``@app.task`` to enable background processing. All settings are
        configured on this object.

        Parameters
        ----------
        name : str
            Used to identify this application, e.g. to set which tasks
            a worker will execute.
        kwargs
            Any settings found in :class:`fennel.settings.Settings`

        Examples
        --------
        >>> from fennel import App
        ...
        >>> app = App(
        ...     name='myapp',
        ...     redis_url='redis://127.0.0.1',
        ...     default_retries=3,
        ...     results_enabled=True,
        ...     log_level='info',
        ...     log_format='json',
        ...     autodiscover='**/tasks.py',
        ...     interface='sync',
        ... )
        ...
        >>> @app.task(retries=1)
        >>> def foo(x):
        ...     return x
        ...
        >>> x = foo.delay(7)  # Execute in the background.
        >>> x
        AsyncResult(uuid=Tjr75jM3QDOHoLTLyrsY1g)
        >>> x.get()  # Wait for the result.
        7

        If your code is running in an asynchronous event loop (e.g. via Starlette,
        FastAPI, Quart), you will want to use the async interface instead:

        >>> import asyncio
        ...
        >>> app = App(name='foo', interface='async')
        ...
        >>> @app.task
        >>> async def bar(x)
        ...     await asyncio.sleep(x)
        ...     return x
        ...
        >>> x = await bar.delay(5)
        >>> await x.status()
        SENT
        >>> await x.get()
        5
        """
        self.name: str = name
        self.settings: Settings = Settings(**kwargs)
        self.task_map: Dict[str, Callable] = {}
        self.keys: Keys = Keys.for_app(self)
        self.client = get_redis(self)
        self.aioclient = get_aredis(self)

        if self.settings.interface == "sync":
            self.task_class = SyncTask
        else:
            self.task_class = AsyncTask

        init_logging(level=self.settings.log_level, format=self.settings.log_format)

    def task(self, func: Callable = None, *, name=None, retries=EMPTY) -> Any:
        """
        A decorator to register a function with the app to enable background processing
        via the task queue.

        The worker process (see :class:`fennel.worker.worker`) will need to discover all
        registered tasks on startup. The means all the modules containing tasks need to
        be imported. Fennel will import modules found via
        :attr:`fennel.settings.Settings.autodiscover`, which by default is
        ``'**/tasks.py'``.

        Parameters
        ---------
        func : Callable
            The decorated function.
        name : str
            The representation used to uniquely identify this task.
        retries : int
            The number of attempts at execution after a task has failed (meaning raised
            any exception).

        Examples
        --------
        Exposes an interface similar to Celery:

        >>> @app.task(retries=1)
        >>> def foo(x):
        ...     return x

        Tasks can be enqueued for processing via:

        >>> foo.delay(8)
        AsyncResult(uuid=q_jb6KaUT-G4tOAoyQ0yaA)

        The can also be called normally, bypassing the Fennel system entirely:

        >>> foo(3)
        3

        By default, tasks are 'fire-and-forget', meaning we will not wait for their
        completion. They will be executed by worker process and will be retried
        automatically on failure (using exponential backoff), so we assume tasks are
        idempotent.

        You can also wait for the result:

        >>> x = foo.delay(4)
        >>> x.status()
        SENT
        >>> x.get(timeout=10)
        4

        If instead you have many tasks and wish to wait for them to complete you can use
        the waiting primitives provided (you will want to ensure all tasks have
        retries=0, which you can set by default with an app setting):

        >>> from fennel.client import gather, wait
        >>> results = [foo.delay(x) for x in range(10)]
        >>> gathered = gather(results)  # Or:
        >>> done, pending = wait(results, timeout=2)

        If your application is running in an event loop you can elect to use the async
        interface for your fennel app (see :attr:`fennel.settings.Settings.interface`),
        which uses `aioredis` under the hood to enqueue items, retrieve results, etc, so
        you will need to await those coroutines:

        >>> app = App(name='foo', interface='async')
        >>>
        >>> @app.task
        >>> async def bar(x)
        ...     await asyncio.sleep(x)
        >>>
        >>> x = await bar.delay(1)
        >>> await x.status()
        SUCCESS
        """
        @wraps(func)
        def wrapper(f: Callable):
            nonlocal retries
            retries = retries if retries is not EMPTY else self.settings.default_retries
            assert type(retries) == int, "retries must be an integer"
            assert retries >= 0, "retries must be an integer >= 0"

            nonlocal name
            name = name or f"{f.__module__}.{f.__qualname__}"
            assert type(name) == str, "name must be a str"
            assert name not in self.task_map, f"Task already found by app: {name}"

            logger.debug("found-task", name=name)
            self.task_map[name] = f
            return self.task_class(name, f, retries=retries, app=self)

        return wrapper(func) if func else wrapper

    def __repr__(self) -> str:
        return f"App(name={self.name}, redis={self.settings.redis_url})"
