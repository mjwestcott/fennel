from typing import Any, Dict

from fennel.client.actions import result
from fennel.client.state import get_status
from fennel.exceptions import ResultsDisabled, TaskFailed
from fennel.job import Job
from fennel.utils import EMPTY


class AsyncResult:
    def __init__(self, job: Job, app):
        """
        A handle for a task that is being processed by workers via the task queue.

        Conceptually similar to the `AsyncResult` from the mutliprocessing library.
        """
        self.job = job
        self.app = app
        self._result: Dict = None

    def status(self):
        """
        Return the status of the task execution.

        Examples
        --------
        >>> @app.task
        >>> def bar(x)
        ...     time.sleep(x)
        ...     return x
        ...
        >>> x = bar.delay(5)
        >>> x.status()
        SENT
        >>> x.status()  # After roughly 5 seconds...
        SUCCESS
        """
        return get_status(self.app, self.job.uuid)

    def get(self, timeout: int = EMPTY) -> Any:
        """
        Wait for the result to become available and return it.

        Raises
        ------
        :exc:`fennel.exceptions.TaskFailed`
            If the original function raised an exception.
        :exc:`fennel.exceptions.Timeout`
            If > `timeout` seconds elapse before a result is available.

        Examples
        --------
        >>> @app.task(retries=0)
        >>> def foo(x):
        ...     return x
        ...
        >>> x = foo.delay(7)
        >>> x.get()  # Wait for the result.
        7

        Warning
        -------
        You must have results storage enabled
        (:attr:`fennel.settings.Settings.results_enabled`)

        If you have retries enabled, they may be rescheduled many times, so you may
        prefer to use retries=0 for tasks whose result you intend to wait for.
        """
        if not self.app.settings.results_enabled:
            raise ResultsDisabled

        if timeout is EMPTY:
            timeout = self.app.settings.task_timeout

        if self._result is None:
            self._result = result(self.app, self.job, timeout)  # Raises Timeout

        exc, val = self._result["exception"], self._result["return_value"]

        if exc:
            raise TaskFailed(**exc)
        else:
            return val

    def __repr__(self) -> str:
        return f"AsyncResult(uuid={self.job.uuid})"
