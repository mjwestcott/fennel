import multiprocessing

from pydantic import BaseSettings, PyObject, validator


class Settings(BaseSettings):
    """
    Settings can be configured via environment variables or keyword arguments for the
    fennel.App instance (which take priority).

    Examples
    --------
    For environment variables, the prefix is ``FENNEL_``, for instance:

    | ``FENNEL_REDIS_URL=redis://127.0.0.1:6379``
    | ``FENNEL_DEFAULT_RETRIES=3``
    | ``FENNEL_RESULTS_ENABLED=true``

    Or via App kwargs:

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

    Parameters
    ----------
    redis_url : str
        Redis URL. Default ``'redis://127.0.0.1:6369'``
    interface : str
        Which client interface should we use -- sync or async? Default ``'sync'``
    processes : int
        How many executor processes to run in each worker. Default
        ``multiprocessing.cpu_count()``
    concurrency : int
        How many concurrent consumers to run (we make at least this many Redis
        connections) in each executor process. The default, 8, can handle 160 req/s in
        a single executor process if each task is IO-bound and lasts on average 50ms. If
        you have long running CPU-bound tasks, you will want to run multiple executor
        processes (and set heartbeat_timeout to greater than your maximum expected task
        duration). Default ``8``
    default_retries : int
        How many times to retry a task in case it raises an exception during execution.
        With 10 retries and the default :func:`fennel.utils.backoff` function, this will
        be approximately 30 days of retries. Default ``10``
    retry_backoff : Callable
        Which algorithm to use to determine the retry schedule. The default is
        exponential backoff via :func:`fennel.utils.backoff`.
    read_timeout : int
        How many milliseconds to wait for messages in the main task queue. Default
        ``4000``
    prefetch_count : int
        How many messages to read in a single call to `XREADGROUP`. Default ``1``
    heartbeat_timeout : float
        How many seconds before an executor is considered dead if heartbeats are missed.
        If you have long-running CPU-bound tasks, this value should be greater than your
        maximum expected task duration. Default ``60``
    heartbeat_interval : float
        How many seconds to sleep between heartbeats are stored for each executor process.
        Default ``6``
    schedule_interval : float
        How many seconds to sleep between polling for scheduled tasks. Default ``4``
    maintenance_interval : float
        How many seconds to sleep between running the maintenance script. Default ``8``
    task_timeout : int
        How long to wait for results to be computed when calling .get(), seconds.
        Default ``10``
    grace_period : int
        How many seconds to wait for in-flight tasks to complete before forcefully
        exiting. Default: ``30``
    restults_enabled : bool
        Whether to store results. Can be disabled if your only use-case is
        'fire-and-forget'. Default ``True``
    results_ttl : int
        How long before expiring results in seconds. Default ``3600`` (one hour).
    log_format : str
        Whether to pretty print a human-readable log ("console") or JSON ("json").
        Default ``'console'``
    log_level : str
        The minimum log level to emit. Default ``'debug'``
    autodiscover : str
        The pattern for :func:`pathlib.Path.glob` to find modules containing
        task-decorated functions, which the worker must import on startup. Will be
        called relative to current working directory. Can be set to the empty string to
        disable. Default ``'**/tasks.py'``
    """
    class Config:
        env_prefix = "FENNEL_"
        case_insensitive = True

    redis_url: str = "redis://127.0.0.1:6379"
    interface: str = "sync"
    processes: int = multiprocessing.cpu_count()
    concurrency: int = 8
    default_retries: int = 10
    retry_backoff: PyObject = "fennel.utils.backoff"
    read_timeout = 4000
    prefetch_count: int = 1
    heartbeat_timeout: float = 60
    heartbeat_interval: float = 6
    schedule_interval: float = 4
    maintenance_interval: float = 8
    task_timeout: int = 10
    grace_period: int = 30
    results_enabled: bool = True
    results_ttl: int = 60 * 60
    log_format: str = "console"
    log_level: str = "debug"
    autodiscover: str = "**/tasks.py"

    @validator("interface")
    def is_valid_interface(cls, value):
        assert value.lower() in ["sync", "async"]
        return value.lower()

    @validator("log_format")
    def is_valid_format(cls, value):
        assert value.lower() in ["console", "json"]
        return value.lower()

    @validator("autodiscover")
    def matches_python(cls, value):
        assert value == "" or value.endswith(".py")
        return value
