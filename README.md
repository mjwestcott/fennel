## Fennel

A task queue for Python 3.7+ based on Redis Streams with a Celery-like API.

### Features

* Supports both sync (e.g. Django, Flask) and async (e.g. Starlette, FastAPI) code.
* Sane defaults: at least once processing semantics, tasks acknowledged on completion.
* Automatic retries with exponential backoff for fire-and-forget jobs.
* Clear task statuses available (SENT, EXECUTING, etc.)
* Exceptionally small and understandable codebase (core is ~1500 lines)
* Automatic task discovery (defaults to using ``**/tasks.py``)

### Basic Usage

Run [Redis](https://redis.io) and then execute your code in `tasks.py`:
```python
from fennel import App

app = App(name='myapp', redis_url='redis://127.0.0.1')

@app.task
def foo(n)
    return n

# Enqueue a task to be executed in the background by a fennel worker process.
foo.delay(7)
```

Meanwhile, run the worker:
```bash
$ fennel worker --app tasks:app
```

### Installation

```bash
pip install fennel
```
