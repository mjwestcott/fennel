from typing import List


class FennelException(Exception):
    pass


class TaskFailed(FennelException):
    def __init__(self, original_type: str, original_args: List):
        """
        This exception is returned by worker processes which experienced an exception
        when executing a task.

        Parameters
        ----------
        original_type : str
            The name of the original exception, e.g. ``'ValueError'``.
        original_args : List
            The arguments given to the original exception, e.g. ``['Not found']``

        Examples
        --------
        >>> @app.task(retries=0)
        >>> async def foo(n):
        ...     raise Exception("baz")
        ...
        >>> x = await foo.delay(3)
        >>> try:
        ...     result = await x.get()
        >>> except TaskFailed as e:
        ...     assert e.original_type == "Exception"
        ...     assert e.original_args == ["baz"]
        """
        self.original_type = original_type
        self.original_args = original_args


class ResultsDisabled(FennelException):
    """
    Raised when ``results_enabled=False`` and code attempts to access a tasks result via
    ``.get()``.
    """


class UnknownTask(FennelException):
    """
    Raised by a worker process if it is unable to find a Python function corresponding
    to the task it has read from the queue.
    """


class Timeout(FennelException):
    """
    Raised by client code when a given timeout is exceeded when waiting for results to arrive.
    """


class JobNotFound(FennelException):
    """
    Raised by client code when attempting to retrieve job information that cannot be
    found in Redis.
    """


class Chaos(FennelException):
    """
    Used in tests to ensure failures are handled properly.
    """


class Completed(FennelException):
    """
    Used internally to shutdown an Executor if the exit condition is completing all
    tasks.
    """
