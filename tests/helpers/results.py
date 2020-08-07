import json

from fennel.app import App
from fennel.models import Result


# XXX: Only for testing.
def all_results(app: App):
    keys = list(app.client.scan_iter(app.keys.result_prefix + ":*"))
    with app.client.pipeline() as pipe:
        for key in keys:
            pipe.lrange(key, 0, 1)
        values = pipe.execute()
    results = [json.loads(v[0]) for v in values]

    return [Result(
        return_value=r["return_value"],
        exception=r["exception"],
    ) for r in results]
