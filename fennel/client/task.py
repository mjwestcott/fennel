from typing import Any, Callable

from fennel.client.actions import send
from fennel.client.results import AsyncResult
from fennel.job import Job


class Task:
    def __init__(self, name: str, func: Callable, retries: int, app):
        self.name = name
        self.func = func
        self.max_retries = retries
        self.app = app

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """
        Traditional Celery-like interface to enqueue a task for execution by the
        workers.

        The `args` and `kwargs` will be passed through to the task when executed.

        Examples
        --------
        >>> @app.task
        >>> def foo(x, bar=None):
        ...     time.sleep(x)
        ...     if bar == "mystr":
        ...         return False
        ...     return True
        ...
        >>> foo.delay(1)
        >>> foo.delay(2, bar="mystr")
        """
        job = Job(
            task=self.name,
            args=list(args),
            kwargs=kwargs,
            tries=0,
            max_retries=self.max_retries,
        )
        send(self.app, job)
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
