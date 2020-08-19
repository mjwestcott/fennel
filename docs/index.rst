Introduction
============

Features
--------

Fennel is a task queue for Python 3.7+ based on Redis Streams with a Celery-like API.

* Supports both sync (e.g. Django, Flask) and async (e.g. Starlette, FastAPI) code.
* Sane defaults: at least once processing semantics, tasks acknowledged on completion.
* Automatic retries with exponential backoff for fire-and-forget jobs.
* Clear task statuses available (e.g. sent, executing, success).
* Automatic task discovery (defaults to using ``**/tasks.py``).
* Exceptionally small and understandable codebase.

.. Note::

    This is an *alpha* release. The project is under development, breaking changes are
    likely.


Installation
------------

.. code-block:: bash

    pip install fennel


Basic Usage
-----------

Run `Redis <https://redis.io>`_ and then write your code in ``tasks.py``:

.. code-block:: python

    from fennel import App

    app = App(name='myapp', redis_url='redis://127.0.0.1')


    @app.task
    def foo(n):
        return n


    # Enqueue a task to be executed in the background by a fennel worker process.
    foo.delay(7)

Meanwhile, run the worker::

    $ fennel worker --app tasks:app


Asynchronous API
----------------

Fennel also supports an async API. If your code is running in an event loop (e.g. via
`Starlette <https://www.starlette.io/>`_ or `FastAPI <https://fastapi.tiangolo.com/>`_),
you will want to use the async interface instead:

.. code-block:: python

    from fennel import App

    app = App(name='myapp', redis_url='redis://127.0.0.1', interface='async')


    @app.task
    async def bar(x):
        return x


    await bar.delay(5)


Contents
--------

.. toctree::
   :maxdepth: 2

    Guide <guide>
    Installation <installation>
    Motivation <motivation>
    Architecture <architecture>
    CLI <cli>
    API Reference <reference>
    Changelog <changelog>


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
