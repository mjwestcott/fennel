"""
A collection of synchronous classes and functions to interact with the Fennel system.
"""

# flake8: noqa
from fennel.client.actions import purge_dead, read_dead, replay_dead
from fennel.client.results import AsyncResult
from fennel.client.task import Task
from fennel.client.waiting import gather, wait

__all__ = [
    "Task",
    "AsyncResult",
    "gather",
    "wait",
    "purge_dead",
    "read_dead",
    "replay_dead",
]
