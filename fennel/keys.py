from dataclasses import dataclass


@dataclass
class Keys:
    queue: str
    group: str
    dead: str
    schedule: str
    heartbeats: str
    status_prefix: str
    result_prefix: str

    @classmethod
    def for_app(cls, app):
        key = lambda x: f"__fennel:{x}:{app.name}"
        return cls(
            queue=key("queue"),
            group=key("group"),
            dead=key("dead"),
            schedule=key("sched"),
            heartbeats=key("beats"),
            status_prefix=key("status"),
            result_prefix=key("result"),
        )

    def status(self, job):
        return self.status_prefix + f":{job.uuid}"

    def result(self, job):
        return self.result_prefix + f":{job.uuid}"
