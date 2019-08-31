"""
    trod.db
    ~~~~~~~
"""

from . import __impl__


__all__ = (
    'binding',
    'execute',
    'unbinding',
)


@__impl__.__ensure__(needbind=False)
async def binding(*args, **kwargs):

    if args or kwargs.get("url"):
        pool = await __impl__.Pool.from_url(*args, **kwargs)
    else:
        pool = await __impl__.Pool(*args, **kwargs)

    __impl__.Executer.activate(pool)
    return True


@__impl__.__ensure__(needbind=True)
async def execute(sql, params=None, **kwargs):

    fetch = kwargs.get("fetch")
    db = kwargs.get("db")
    many = kwargs.get("many")

    sql = getattr(sql, "sql", sql)
    if fetch:
        return await __impl__.Executer.fetch(sql, params=params, db=db)
    return await __impl__.Executer.execute(sql, params=params, many=many, db=db)


async def unbinding():
    return await __impl__.Executer.death()
