"""
    trod.db
    ~~~~~~~
"""

from . import _impl


__all__ = (
    'binding',
    'execute',
    'unbinding',
    'poolstate',
)


@_impl.__ensure__(False)
async def binding(*args, **kwargs):
    """A coroutine that binding a database(create a connection pool).

    The pool is a singleton, repeated create will cause errors.
    Returns true after successful create

    For parameters, see ``_impl.Pool` and ``_impl.Pool.from_url``
    """

    if args or kwargs.get("url"):
        pool = await _impl.Pool.from_url(*args, **kwargs)
    else:
        pool = await _impl.Pool(*args, **kwargs)

    _impl.Executer.activate(pool)

    return True


@_impl.__ensure__(True)
async def execute(query, **kwargs):
    """A coroutine that execute sql and return the results of its execution

    :param sql ``trod.g.Query`` : sql query object
    """
    if not query:
        raise ValueError("No SQL query statement")

    return await _impl.Executer.do(query, **kwargs)


@_impl.__ensure__(True)
async def unbinding():
    """A coroutine that unbinding a database(close the connection pool)."""

    return await _impl.Executer.death()


@_impl.__ensure__(True)
def poolstate():
    """Returns the current state of the connection pool"""

    return _impl.Executer.poolstate()
