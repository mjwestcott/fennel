import time
from contextlib import contextmanager
from multiprocessing import Process

from fennel.client.state import count_results, get_state
from fennel.worker import EXIT_SIGNAL, Executor, start


@contextmanager
def worker(app):
    # For end-to-end integration tests, will spawn N more
    # executor processes and will shutdown gracefully.
    p = Process(target=start, args=(app,))
    try:
        p.start()
        yield p
    finally:
        p.terminate()
        p.join()


@contextmanager
def executor(app, graceful_shutdown=False, exit=EXIT_SIGNAL):
    # For unit tests which need to run an executor, spawn it
    # directly, kill it quickly with no cleanup.
    p = Process(target=Executor(app).start, daemon=True)
    try:
        p.start()
        yield p
    finally:
        if graceful_shutdown:
            p.terminate()
        else:
            p.kill()
        p.join()


def wait_for_results(app, length, sleep=0.01, maxwait=1):
    assert sleep <= maxwait
    tries = maxwait // sleep

    while tries and not count_results(app) == length:
        time.sleep(sleep)
        tries -= 1

    return get_state(app)
