import random

from fennel.job import Job
from fennel.status import UNKNOWN
from fennel.utils import base64uuid


def random_job(**kwargs):
    defaults = dict(
        task="unknown",
        args=[random.randint(1, 100)],
        kwargs={},
        tries=0,
        max_retries=9,
        exception={},
        return_value=None,
        uuid=base64uuid(),
        status=UNKNOWN,
    )
    return Job(**dict(defaults, **kwargs))
