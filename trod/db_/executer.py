import sys
import warnings

import aiomysql

from trod import errors, utils
from .connector import Connector


class Executer:
    """ MySQL SQL executer """

    pool = None

    __slots__ = ()

    # @classmethod
    # def __repr__(cls):
    #     return "<{} bind to '{}'>".format(
    #         cls.__class__, cls.pool.connmeta.db
    #     )

    # __str__ = __repr__

    @classmethod
    def pool_status(cls):

        return cls.pool.state

    @classmethod
    def pool_meta(cls):

        return cls.pool.connmeta

    @classmethod
    async def init(cls, *args, connector=None, **kwargs):
        """ A coroutine that to bind connector """
        if cls.pool:
            warnings.warn(
                'Duplicate database connector binding', errors.ProgrammingWarning
            )
            await cls.unbind()

        if connector:
            cls.pool = await Connector(*args, **kwargs)
        else:
            cls.pool = await Connector.from_url(*args, **kwargs)
        return cls

    @classmethod
    def autocommit(cls):
        """ Whether to automatically submit a transaction """

        return cls.pool.connmeta.autocommit

    @classmethod
    async def unbind(cls):
        """ A coroutine that call `clint.close()` to unbind connector"""

        if cls.pool:
            cls.pool = await cls.pool.close()
            return True

        warnings.warn('No binding db connection or closed', errors.ProgrammingWarning)
        return False

    @classmethod
    def is_usable(cls):
        """ return a bool is bind a connector """

        return bool(cls.pool)

    @classmethod
    @utils.troddict_formatter(is_async=True)
    async def _fetch(cls, sql, args=None, rows=None):

        if args:
            args = utils.tuple_formatter(args)

        async with cls.pool.get() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql.strip(), args or ())
                    if rows and rows == 1:
                        result = await cur.fetchone()
                    elif rows:
                        result = await cur.fetchmany(rows)
                    else:
                        result = await cur.fetchall()
                    return result
                except BaseException:
                    exc_type, exc_value, _ = sys.exc_info()
                    error = exc_type(exc_value)
                    raise error

    @classmethod
    async def _execute(cls, sql, args=None, batch=False):
        sql = sql.strip()
        if args:
            args = utils.tuple_formatter(args)

        async with cls.pool.get() as conn:
            if not cls.autocommit():
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if batch is True:
                        await cur.executemany(sql, args or ())
                    else:
                        await cur.execute(sql, args or ())
                    affected, last_id = cur.rowcount, cur.lastrowid
                if not cls.autocommit():
                    await conn.commit()
            except BaseException:
                if not cls.autocommit():
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
            return utils.TrodDict(last_id=last_id, affected=affected)

    @classmethod
    async def fetch(cls, sql, args=None, rows=None):
        """ A coroutine that proxy fetch sql request """

        return await cls._fetch(
            sql, args=args, rows=rows
        )

    @classmethod
    async def execute(cls, sql, values=None, is_batch=False):
        """ A coroutine that proxy execute sql request """

        return await cls._execute(
            sql, args=values, batch=is_batch
        )

    @classmethod
    async def exist(cls, exist_sql):
        """ A coroutine that return a bool is table is exist """

        result = await cls._fetch(exist_sql)
        return bool(result)

    @classmethod
    async def text(cls, sql, args=None, rows=None, batch=False):
        """ A coroutine that execute sql text """

        is_fetch = True
        if 'SELECT' in sql or 'select' in sql:
            result = await cls._fetch(sql, args=args, rows=rows)
        else:
            is_fetch = False
            result = await cls._execute(sql, args=args, batch=batch)

        return utils.TrodDict(is_fetch=is_fetch, data=result)
