import sys
import warnings

import aiomysql

from trod import errors, utils
from .connector import Connector


class Executer:
    """ MySQL SQL executer """

    __slots__ = ('connector',)

    @classmethod
    async def init(cls, *args, **kwargs):
        """ A coroutine that to bind connector """

        return cls(await Connector.from_url(*args, **kwargs))

    def __init__(self, connector):
        if not isinstance(connector, Connector):
            raise RuntimeError()
        self.connector = connector

    def __repr__(self):
        return "<{} by {}>".format(
            self.__class__.__name__, self.connector
        )

    __str__ = __repr__

    @property
    def connstate(self):

        return self.connector.state

    @property
    def connmeta(self):

        return self.connector.connmeta

    @property
    def autocommit(self):
        """ Whether to automatically submit a transaction """

        return self.connector.connmeta.autocommit

    async def unbind(self):
        """ A coroutine that call `clint.close()` to unbind connector"""

        if self.connector:
            self.connector = await self.connector.close()
            return True

        warnings.warn('No binding db connection or closed', errors.ProgrammingWarning)
        return False

    @utils.troddict_formatter(is_async=True)
    async def _fetch(self, sql, args=None, rows=None):

        if args:
            args = utils.tuple_formatter(args)

        async with self.connector.get() as conn:
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
            args = utils.tuple_formatter(args)

        async with self.connector.get() as conn:
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
