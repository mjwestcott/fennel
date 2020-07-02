from typing import Optional

import click

from fennel import client
from fennel.client.state import get_job, get_state
from fennel.exceptions import JobNotFound
from fennel.models import render
from fennel.utils import get_object
from fennel.worker import start


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option(
    "-a",
    "--app",
    "application",
    required=True,
    help="""
    A colon-separated string identifying the :class:`fennel.App` instance for which to
    run a worker.

    If a file ``foo.py`` exists at the current working directory with the following
    contents:

    >>> from fennel import App
    >>>
    >>> app = App(name="myapp", redis_url="redis://127.0.0.1:6379")
    >>>
    >>> @app.task
    >>> def f():
    >>>     pass

    Then pass ``foo:app`` as the `app` option: ``$ fennel worker --app=foo:app``
""",
)
@click.option(
    "-p",
    "--processes",
    type=int,
    default=None,
    help="""
    How many executor processes to run in each worker. Default
    ``multiprocessing.cpu_count()``
""",
)
@click.option(
    "-c",
    "--concurrency",
    type=int,
    default=None,
    help="""
    How many concurrent consumers to run (we make at least this many Redis
    connections) in each executor process. The default, 8, can handle 160 req/s in
    a single worker process if each task is IO-bound and lasts on average 50ms. If
    you have long running CPU-bound tasks, you will want to run multiple executor
    processes. Default ``8``
""",
)
def worker(application: str, processes: int, concurrency: int) -> None:
    """
    Run the worker.
    """
    app = get_object(application)
    if processes:
        app.settings.processes = processes
    if concurrency:
        app.settings.concurrency = concurrency
    start(app)


@cli.command()
@click.option("-a", "--app", "application", required=True)
@click.argument("action", type=click.Choice(["read", "replay", "purge"]))
def dlq(application: str, action: str) -> None:
    """
    Interact with the dead-letter queue. Choices for the `action` argument:

    \b
    * read - Print all tasks from the dead-letter queue to stdout.
    * replay - Move all tasks from the dead-letter queue back to the main task queue for reprocessing.
    * purge - Remove all tasks from the dead-letter queue forever.
    """
    if action == "read":
        click.echo(render(client.read_dead(get_object(application))))
    elif action == "replay":
        click.echo(render(client.replay_dead(get_object(application))))
    elif action == "purge":
        click.echo(render(client.purge_dead(get_object(application))))


@cli.command()
@click.option("-a", "--app", "application", required=True)
@click.option("-u", "--uuid", required=True)
def task(application: str, uuid: Optional[str]) -> None:
    """
    Print a JSON-encoded summary of job information.
    """
    app = get_object(application)
    try:
        click.echo(render(get_job(app, uuid)))
    except JobNotFound:
        click.echo(render({"error": f"Task with uuid={uuid} not found"}))


@cli.command()
@click.option("-a", "--app", "application", required=True)
def info(application: str) -> None:
    """
    Print a JSON-encoded summary of application state.
    """
    click.echo(render(get_state(get_object(application))))
