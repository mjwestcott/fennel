import importlib
from pathlib import Path

import structlog

logger = structlog.get_logger("fennel.worker")


def autodiscover(app, basedir=None):
    """
    Automatically import Python modules matching the pattern in
    :attr:`fennel.settings.Settings.autodiscover`.

    Tasks are registered via decorators. When the worker process starts up, these
    decorators must be executed in order for the app to record which Python functions
    map to which task names.

    Parameters
    ----------
    app : fennel.App
        The app instance for which to discover tasks.
    basedir : str
        The directory relative to which we will search the filesystem.
    """
    pattern = app.settings.autodiscover

    if pattern:
        for filename in Path(basedir or Path('.')).glob(pattern):
            module = ".".join(filename.parts)[:-3]
            logger.debug("discovered", module=module)
            importlib.import_module(module)
