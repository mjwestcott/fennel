import json
from dataclasses import asdict, field, replace
from typing import Any, Dict, List

from pydantic import validator
from pydantic.dataclasses import dataclass

from fennel.status import UNKNOWN
from fennel.utils import base64uuid


@dataclass
class Job:
    """
    The internal representation of a job.

    Parameters
    ----------
    task : str
        The name of the task. By default will use
        ``f"{func.__module__}.{func.__qualname__}"``, where `func` is the Python
        callable.
    args : List
        The job's args.
    kwargs : Dict
        The job's kwargs.
    tries : int
        The number of attempted executions.
    max_retries : int
        The maximum number of retries to attempt after failure.
    exception : Dict
        Exception information for the latest failure, contains
        'original_type' (str, e.g. 'ValueError') and
        'original_args' (List, e.g. ['Not found']).
    return_value : Any
        The return value of the Python callable when execution succeeds.
    status : str
        One of :mod:`fennel.status`, the current lifecycle stage.
    uuid : str
        Base64-encoded unique identifier.
    """
    task: str
    args: List
    kwargs: Dict
    tries: int = 0
    max_retries: int = 9
    exception: Dict = field(default_factory=dict)
    return_value: Any = None
    status: str = UNKNOWN
    uuid: str = field(default_factory=base64uuid)

    @validator("args", "kwargs", "exception", "return_value", pre=True, whole=True)
    def json_decode(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except ValueError:
                pass
        return v

    def increment(self) -> "Job":
        return replace(self, tries=self.tries + 1)

    def replace(self, **kwargs):
        return replace(self, **kwargs)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, s: str) -> "Job":
        return cls(**json.loads(s))

    def serialise(self) -> Dict:
        d = asdict(self)
        d["args"] = json.dumps(d["args"])
        d["kwargs"] = json.dumps(d["kwargs"])
        d["exception"] = json.dumps(d["exception"])
        d["return_value"] = json.dumps(d["return_value"])
        return d

    @classmethod
    def deserialise(cls, fields: Dict) -> "Job":
        return cls(**fields)

    @property
    def result(self) -> str:
        return json.dumps({
            "return_value": self.return_value,
            "exception": self.exception,
        })
