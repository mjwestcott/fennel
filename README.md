## Fennel

A task queue for Python 3.7+ based on Redis Streams with a Celery-like API.

https://fennel.dev/

| Note: This is an *alpha* release. The project is under development, breaking changes are likely. |
| --- |

### Features

* Supports both sync (e.g. Django, Flask) and async (e.g. Starlette, FastAPI) code.
* Sane defaults: at least once processing semantics, tasks acknowledged on completion.
* Automatic retries with exponential backoff for fire-and-forget jobs.
* Clear task statuses available (e.g. sent, executing, success).
* Automatic task discovery (defaults to using ``**/tasks.py``).
* Exceptionally small and understandable codebase.

### Installation

```bash
pip install fennel
```

### Basic Usage

Run [Redis](https://redis.io) and then execute your code in `tasks.py`:
```python
from fennel import App

app = App(name='myapp', redis_url='redis://127.0.0.1')

@app.task
def foo(n):
    return n

# Enqueue a task to be executed in the background by a fennel worker process.
foo.delay(7)
```

Meanwhile, run the worker:
```bash
$ fennel worker --app tasks:app
```

### Asynchronous API

Fennel also supports an async API. If your code is running in an event loop
(e.g. via [Starlette](https://www.starlette.io/) or
[FastAPI](https://fastapi.tiangolo.com/)), you will want to use the async
interface instead:
```python
from fennel import App

app = App(name='myapp', redis_url='redis://127.0.0.1', interface='async')

@app.task
async def bar(x):
    return x

await bar.delay(5)
```
