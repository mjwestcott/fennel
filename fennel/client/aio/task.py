from typing import Any, Callable

from fennel.client.aio.actions import send
from fennel.client.aio.results import AsyncResult
from fennel.job import Job


class Task:
    def __init__(self, name: str, func: Callable, retries: int, app):
        self.name = name
        self.func = func
        self.max_retries = retries
        self.app = app

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """
        Enqueue a task for execution by the workers.

        Similar to asyncio.create_task (but also works with non-async functions and runs
        on our Redis-backed task queue with distributed workers, automatic retry, and
        result storage with configurable TTL).

        The `args` and `kwargs` will be passed through to the task when executed.

        Examples
        --------
        >>> @app.task(retries=1)
        >>> async def foo(x, bar=None):
        ...     asyncio.sleep(x)
        ...     if bar == "mystr":
        ...         return False
        ...     return True
        ...
        >>> await foo.delay(1)
        >>> await foo.delay(2, bar="mystr")
        """
        job = Job(
            task=self.name,
            args=list(args),
            kwargs=kwargs,
            tries=0,
            max_retries=self.max_retries,
        )
        await send(self.app, job)
        return AsyncResult(job=job, app=self.app)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Call the task-decorated function as a normal Python function. The fennel system
        will be completed bypassed.

        Examples
        --------
        >>> @app.task
        >>> def foo(x):
        ...     return x
        ...
        >>> foo(7)
        7
        """
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Task(name={self.name})"
