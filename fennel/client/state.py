import json
from datetime import datetime

from fennel import models
from fennel.exceptions import JobNotFound
from fennel.job import Job


def get_status(app, uuid):
    resp = app.client.hget(app.keys.status_prefix + f":{uuid}", "status")
    if not resp:
        raise JobNotFound(uuid)
    return resp


def get_job(app, uuid):
    resp = app.client.hgetall(app.keys.status_prefix + f":{uuid}")
    if not resp:
        raise JobNotFound(uuid)
    return Job(**resp)


def count_jobs(app):
    return len(list(app.client.scan_iter(app.keys.status_prefix + ":*")))


def get_messages(app, key, n=100):
    messages = app.client.xrevrange(key, count=n)
    return [models.Message(id=id, uuid=fields["uuid"]) for id, fields in messages]


def get_info(app, key):
    info = app.client.xinfo_stream(key)
    return models.Info(**{k.replace("-", "_"): v for k, v in info.items()})


def get_groups(app, key):
    groups = app.client.xinfo_groups(key)
    return [models.Group(**{k.replace("-", "_"): v for k, v in d.items()}) for d in groups]


def get_stream(app, key, n=100):
    return models.Stream(
        key=key,
        info=get_info(app, key),
        groups=get_groups(app, key),
        messages=get_messages(app, key, n=n),
    )


def get_result(app, key):
    value = json.loads(app.client.lrange(key, 0, 1)[0])
    return models.Result(return_value=value["return_value"], exception=value["exception"])


def count_results(app):
    return len(list(app.client.scan_iter(app.keys.result_prefix + ":*")))


def get_heartbeats(app):
    beats = app.client.hgetall(app.keys.heartbeats)
    return [
        models.Heartbeat(executor_id=k, timestamp=datetime.fromtimestamp(float(v)))
        for k, v in zip(beats.keys(), beats.values())
    ]


def get_schedule(app):
    return [
        models.ScheduledJob(eta=datetime.fromtimestamp(int(v)), uuid=k)
        for k, v in app.client.zscan_iter(app.keys.schedule)
    ]


def get_state(app, messages=100):
    return models.State(
        queue=get_stream(app, app.keys.queue, n=messages),
        dead=get_stream(app, app.keys.dead, n=messages),
        heartbeats=get_heartbeats(app),
        schedule=get_schedule(app),
    )
