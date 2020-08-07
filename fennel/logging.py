import datetime
import logging
import logging.config
import os
from io import StringIO
from typing import Any, Dict

import colorama
import structlog


def init_logging(level="debug", format="console"):
    level = level.upper()

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "%(message)s",
            }
        },
        "handlers": {
            "fennel.client": {
                "level": level,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            }
        },
        "loggers": {
            "fennel.client": {
                "handlers": ["fennel.client"],
                "level": level,
                "propagate": False,
            },
            "fennel.worker": {
                # To be used with a custom QueueHandler to prevent interleaving
                # among the multiple processes.
                "level": level,
                "propagate": False,
            },
        },
    })

    structlog.configure(
        logger_factory=logging.getLogger,
        processors=[
            _add_meta,
            structlog.stdlib.filter_by_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if format == "json" else _renderer,
        ],
    )


def _add_meta(logger: str, method_name: str, event_dict: Dict) -> Dict:
    event_dict = {
        "event": event_dict.pop("event"),
        "exc_info": event_dict.pop("exc_info", None),
        "extra": event_dict,
    }

    if not event_dict["extra"]:
        event_dict.pop("extra")

    event_dict["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    event_dict["level"] = method_name.upper()
    event_dict["pid"] = os.getpid()
    return event_dict


def _renderer(logger: str, method_name: str, event_dict: Dict) -> str:
    reset = colorama.Style.RESET_ALL
    bright = colorama.Style.BRIGHT
    dim = colorama.Style.DIM

    red = colorama.Fore.RED
    yellow = colorama.Fore.YELLOW
    blue = colorama.Fore.BLUE
    green = colorama.Fore.GREEN

    colours = {
        "CRITICAL": red + bright,
        "EXCEPTION": red + bright,
        "ERROR": red + bright,
        "WARNING": yellow + bright,
        "INFO": green + bright,
        "DEBUG": blue + bright,
    }

    s = StringIO()

    ts = event_dict.pop("timestamp", None)
    if ts:
        s.write(f"{reset}{dim}{str(ts)}{reset} ")

    level = event_dict.pop("level", None)
    if level:
        s.write(f"{colours[level]}{level:>9}{reset} ")

    event = event_dict.pop("event")
    s.write(f"{bright}{event:<17}{reset} ")

    pid = event_dict.pop("pid", None)
    if pid:
        s.write(f"{blue}pid{reset}={pid} ")

    def _format_extra(key: str, value: Any) -> None:
        s.write(f"{blue}{key}{reset}={value}{reset} ")

    extra = event_dict.pop("extra", {})

    job = extra.pop("job", None)
    if job:
        for k, v in job.items():
            _format_extra(k, v)

    for key, value in extra.items():
        _format_extra(key, value)

    stack = event_dict.pop("stack", None)
    exc = event_dict.pop("exception", None)

    if stack is not None:
        s.write("\n" + stack)
        if exc is not None:
            s.write("\n\n" + "=" * 88 + "\n")
    if exc is not None:
        s.write("\n" + exc)

    return s.getvalue()
