"""
Jobs have a number of statuses through their lifecycle. This module contains the
constants. If you have enqueued a task for execution, then you can obtain its status
as follows:

>>> x = mytask.delay()
>>> x.status()
EXECUTING
"""

#: The job's status is not stored in Redis. Presumably no action has been taken on the
#: job.
UNKNOWN = "UNKNOWN"

#: The job has been sent to Redis, but execution has not yet started.
SENT = "SENT"

#: A worker has received the job from the queue and has begun executing it.
EXECUTING = "EXECUTING"

#: Execution was successful and the job's result is ready (if results storage is
#: enabled).
SUCCESS = "SUCCESS"

#: Execution was not successful (an exception was raised) and a retry is scheduled
#: to occur in the future.
RETRY = "RETRY"

#: Execution was not successful (an exception was raised) and retries have been
#: exhausted, so the job is now in the dead-letter queue where it will remain
#: until manual intervention (via the CLI or client code).
DEAD = "DEAD"
