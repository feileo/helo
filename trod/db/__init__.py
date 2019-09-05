"""
    trod.db
    ~~~~~~~
"""

from . import __impl__


__all__ = (
    'binding',
    'execute',
    'unbinding',
    'poolmeta',
)


@__impl__.__ensure__(bound=False)
async def binding(*args, **kwargs):
    """A coroutine that binding a database(create a connection pool).

    The pool is a singleton, repeated create will cause errors.
    Returns true after successful create

    For parameters, see ``__impl__.Pool` and ``__impl__.Pool.from_url``
    """

    if args or kwargs.get("url"):
        pool = await __impl__.Pool.from_url(*args, **kwargs)
    else:
        pool = await __impl__.Pool(*args, **kwargs)

    __impl__.Executer.activate(pool)
    return True


@__impl__.__ensure__(bound=True)
async def execute(sql, params=None, mode=None, **kwargs):
    """A coroutine that execute sql and return the results of its execution

    :param int mode: read or write mode, see ``__impl__.R`` and ``__impl__.W``
    :param str sql: sql query statement
    :param params list/tuple: query values for sql
    """

    mode = mode or __impl__.detach(sql)
    db = kwargs.get("db")
    if mode == __impl__.R:
        return await __impl__.Executer.fetch(sql, params=params, db=db)
    return await __impl__.Executer.execute(
        sql, params=params, many=kwargs.get("many", False), db=db
    )


@__impl__.__ensure__(bound=True)
async def unbinding():
    """A coroutine that unbinding a database(close the connection pool)."""

    return await __impl__.Executer.death()


@__impl__.__ensure__(bound=True)
def poolmeta():
    """Returns the current state of the connection pool"""

    return __impl__.Executer.poolmeta()
