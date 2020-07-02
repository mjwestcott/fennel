import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pydantic import validator
from pydantic.dataclasses import dataclass


def render(content) -> bytes:
    s = json.dumps(content, cls=Encoder, indent=4, separators=(",", ": "))
    return s.encode("utf-8")


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if is_dataclass(obj):
            return asdict(obj)
        return json.JSONEncoder.default(self, obj)


@dataclass
class Message:
    id: str
    uuid: str


@dataclass
class Info:
    length: int
    radix_tree_keys: int
    radix_tree_nodes: int
    groups: int
    last_generated_id: str
    first_entry: Optional[Tuple[str, Dict]]
    last_entry: Optional[Tuple[str, Dict]]

    @validator("first_entry", "last_entry", pre=True, whole=True)
    def deserialise_entries(cls, value):
        if value is None:
            return None
        elif isinstance(value, Tuple):
            # From redis.Redis
            return value
        elif isinstance(value, List):
            # From aioredis.Redis, convert to the other form
            xid, entry = value
            it = iter(entry)
            return (xid, dict(zip(it, it)))
        raise ValueError


@dataclass
class Group:
    name: str
    consumers: int
    pending: int
    last_delivered_id: str


@dataclass
class Stream:
    key: str
    info: Info
    groups: List[Group]
    messages: List[Message]


@dataclass
class Result:
    return_value: Any
    exception: Dict


@dataclass
class Heartbeat:
    executor_id: str
    timestamp: datetime


@dataclass
class ScheduledJob:
    eta: datetime
    uuid: str


@dataclass
class State:
    queue: Optional[Stream]
    dead: Optional[Stream]
    heartbeats: List[Heartbeat]
    schedule: List[ScheduledJob]
