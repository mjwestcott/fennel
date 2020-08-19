import logging
import multiprocessing as mp
import signal
import time
from logging.handlers import QueueListener

import structlog

from fennel.utils import get_mp_context
from fennel.worker.discovery import autodiscover
from fennel.worker.executor import EXIT_SIGNAL, Executor

logger = structlog.get_logger("fennel.worker")


def start(app, exit=EXIT_SIGNAL):
    """
    The main entrypoint for the worker.

    The worker will create and monitor `N` :class:`fennel.worker.Executor` processes. Each
    `Executor` will spawn `M` coroutines via an asyncio event loop. `N` and `M` are
    controlled by :attr:`fennel.settings.Settings.processes` and
    :attr:`fennel.settings.Settings.concurrency` respectively.

    CPU-bound tasks benefit from multiple processes. IO-bound tasks will benefit from
    high executor concurrency.

    Parameters
    ----------
    app : fennel.App
        The application instance for which to start a background worker.
    exit : str
        The exit strategy. `EXIT_SIGNAL` is used when the worker should only stop on
        receipt of a interrupt or termination signal. `EXIT_COMPLETE` is used in tests
        to exit when all tasks from the queue have completed.

    Notes
    -----
    `signal.SIGINT` and `signal.SIGTERM` are handled by gracefully shutting down, which
    means giving the executor processes a chance to finish their current tasks.
    """
    autodiscover(app)
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

        if signum == 2:
            logger.critical("sigint")
        elif signum == 15:
            logger.critical("sigterm")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    ctx = get_mp_context()
    queue: mp.Queue = ctx.Queue()  # To avoid interleaving executor process logs.

    processes = [
        ctx.Process(target=Executor(app).start, args=(exit, queue), daemon=True)
        for _ in range(app.settings.processes)
    ]

    for p in processes:
        p.start()

    # Start the logging listener thread after forking.
    QueueListener(queue, logging.StreamHandler()).start()

    try:
        while running and all(p.is_alive() for p in processes):
            time.sleep(0.1)
    finally:
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
        logger.critical("exit")
