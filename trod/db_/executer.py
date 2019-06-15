import sys
import logging

import aiomysql

from trod.errors import DuplicateBindError, NoBindError
from trod import utils


class Executer:

    clint = None

    __slots__ = ('clint',)

    def __init__(self):
        if self.clint is None:
            raise NoBindError(
                'Executer no binding db or closed'
            )

    def __repr__(self):
        return "<Class '{}' bind to '{}'>".format(
            self.__class__.__name__, self.clint.connmeta.db
        )

    __str__ = __repr__

    @property
    def connect_status(self):
        """ Current connection pool status """

        return self.clint.state

    @property
    def connect_info(self):
        """ Db info """

        return self.clint.connmeta

    @classmethod
    async def bind_db(cls, connector):
        """ A coroutine that bind db for `RequestClient` """

        if cls.clint is not None:
            raise DuplicateBindError('Duplicate database binding')

        cls.clint = connector
        return True

    @classmethod
    async def unbind(cls):
        """ A coroutine that call `executer.close()` to unbind db"""

        if cls.clint is not None:
            cls.clint = await cls.clint.close()
            return True

        logging.warning('No binding db connection or closed')
        return False

    @classmethod
    def is_usable(cls):
        """ return a bool is bind a db """

        return bool(cls.clint)

    @property
    def autocommit(self):
        """ Whether to automatically submit a transaction """

        return self.clint.connmeta.autocommit

    @utils.troddict_formatter(is_async=True)
    async def _fetch(self, sql, args=None, rows=None):

        if args:
            args = utils.tuple_formater(args)

        async with self.clint.get() as conn:
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

    async def _execute(self, sql, args=None, batch=False):
        sql = sql.strip()
        if args:
            args = utils.tuple_formater(args)

        async with self.clint.get() as conn:
            if not self.autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if batch is True:
                        await cur.executemany(sql, args or ())
                    else:
                        await cur.execute(sql, args or ())
                    affected, last_id = cur.rowcount, cur.lastrowid
                if not self.autocommit:
                    await conn.commit()
            except BaseException:
                if not self.autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
            return utils.TrodDict(last_id=last_id, affected=affected)

    async def fetch(self, sql, args=None, rows=None):
        """ A coroutine that proxy fetch sql request """

        return await self._fetch(
            sql, args=args, rows=rows
        )

    async def execute(self, sql, values=None, is_batch=False):
        """ A coroutine that proxy execute sql request """

        return await self._execute(
            sql, args=values, batch=is_batch
        )

    async def exist(self, exist_sql):
        """ A coroutine that return a bool is table is exist """

        result = await self._fetch(exist_sql)
        return bool(result)

    async def text(self, sql, args=None, rows=None, batch=False):
        """ A coroutine that execute sql text """

        is_fetch = True
        if 'SELECT' in sql or 'select' in sql:
            result = await self._fetch(sql, args=args, rows=rows)
        else:
            is_fetch = False
            result = await self._execute(sql, args=args, batch=batch)
        return utils.TrodDict(is_fetch=is_fetch, data=result)
