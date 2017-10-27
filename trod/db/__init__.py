"""
    trod.db
    ~~~~~~~
"""

from .db import Pool, Executer, __ensure__


__all__ = (
    'binding',
    'exec',
    'close'
)


@__ensure__(needbind=False)
async def binding(*args, **kwargs):

    if args or kwargs.get("url"):
        pool = await Pool.from_url(*args, **kwargs)
    else:
        pool = await Pool(*args, **kwargs)

    Executer.activate(pool)
    return True


@__ensure__(needbind=True)
async def exec(sql, params=None, **kwargs):

    fetch = kwargs.get("fetch")
    db = kwargs.get("db")
    many = kwargs.get("many")

    if fetch:
        return await Executer.fetch(sql, params=params, db=db)
    return await Executer.execute(sql, params=params, many=many, db=db)


async def close():
    return await Executer.death()


#     @classmethod
#     def select_db(cls, db=None):
#         if db is None:  # pylint: disable=all
#             if not cls._pool:
#                 raise RuntimeError()
#             return cls.get_connmeta().db
#         elif not db or not isinstance(db, str):
#             raise ValueError()

#         with cls.dblock:
#             cls.selected = db

#         return cls.selected


# def current():
#     c_db = None
#     if Connector.selected:
#         c_db = Connector.selected
#     else:
#         connmeta = Connector.get_connmeta()
#         if connmeta:
#             c_db = connmeta.db
#     return c_db


# class Doer:

#     __slots__ = ('_model', '_sql', '_args')
#     __fetch__ = False

#     def __init__(self, model, sql=None, args=None):
#         self._model = model
#         self._sql = sql or []
#         self._args = args

#     def __str__(self):
#         args = f' % {self._args}' if self._args else ''
#         return f"Doer by {Connector.get_pool()}\n For SQL({self.sql}{args})"

#     __repr__ = __str__

#     @property
#     def sql(self):
#         if isinstance(self._sql, (list, tuple)):
#             if self._sql:
#                 self._sql.append(';')
#                 self._sql = ' '.join(self._sql)
#         return self._sql

#     async def do(self):

#         pool = Connector.get_pool()
#         db = Connector.selected

#         if self.__fetch__:
#             fetch_results = await Executer.fetch(
#                 pool, self.sql, args=self._args, db=db
#             )
#             tdicts = getattr(self, 'tdicts', True)
#             return loader.load(self._model, fetch_results, tdicts)

#         exec_results = await Executer.execute(
#             pool, self.sql, values=self._args,
#             is_batch=getattr(self, '_batch', False), db=db
#         )
#         return loader.ExecResults(*exec_results)
