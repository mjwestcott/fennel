from functools import wraps

from fennel.utils import get_aioredis


def aioclient(f):
    # Lazily initialise app.aioclient. This is needed because the app object is not
    # initialised in an async context (it's not awaited), but the functions in this
    # module will be used in an async context. We want to reuse a client with a
    # connection pool for efficiency.
    @wraps(f)
    async def wrapper(app, *args, **kwargs):
        if not app.aioclient:
            app.aioclient = await get_aioredis(app, app.settings.client_poolsize)
        return await f(app, *args, **kwargs)

    return wrapper
