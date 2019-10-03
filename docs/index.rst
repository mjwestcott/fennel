.. Fennel documentation master file, created by
   sphinx-quickstart on Tue Sep 24 14:54:59 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

Features
--------

Fennel is a task queue for Python 3.7+ based on Redis Streams with a Celery-like API.

* Supports both sync (e.g. Django, Flask) and async (e.g. Starlette, FastAPI) code.
* Sane defaults: at least once processing semantics, tasks acknowledged on completion.
* Automatic retries with exponential backoff for fire-and-forget jobs.
* Clear task statuses available (SENT, EXECUTING, etc.)
* Exceptionally small and understandable codebase (core is ~1500 lines)
* Automatic task discovery (defaults to using ``**/tasks.py``)

.. Note::

    This is an *alpha* release. The project is under development, breaking changes are
    likely.

Basic Usage
-----------

Run `Redis <https://redis.io>`_ and then write your code in ``tasks.py``:

.. code-block:: python

    from fennel import App

    app = App(name='myapp', redis_url='redis://127.0.0.1')

    @app.task
    def foo(n)
        return n

    # Enqueue a task to be executed in the background by a fennel worker process.
    foo.delay(7)

Meanwhile, run the worker::

    $ python -m fennel worker --app tasks:app

Installation
------------

.. code-block:: bash

    pip install fennel

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
