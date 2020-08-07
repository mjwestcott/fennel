import asyncio
import random
import time

from fennel import App

app = App(name="example", results_ttl=120, interface="sync")


@app.task
def square(n: int) -> int:
    return n**2


@app.task
def sync_sleep(seconds: int) -> None:
    time.sleep(seconds)


@app.task
async def async_sleep(seconds: int) -> None:
    await asyncio.sleep(seconds)


@app.task
def sometimes_fail(*args, **kwargs) -> None:
    if random.random() < 0.1:
        raise ValueError("sometimes_fail")


@app.task(retries=1)
def always_fail(*args, **kwargs) -> None:
    raise ValueError("always_fail")
