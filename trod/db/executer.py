import sys

import aiomysql

from trod.db.connector import Connector
from trod.errors import DuplicateBindError, NoBindError
from trod.extra.logger import Logger
from trod.utils import async_dict_formatter, Dict, tuple_formater


class Executer:
    """ Maintain an instance of the connector,
        and get the connection execution SQL from this.
    """

    def __init__(self, conn_pool):
        """
        arg: conn_pool is a `Connector` instance
        """

        if not isinstance(conn_pool, Connector):
            raise ValueError('Init connection pool must be `Connector` type')
        self.conn_pool = conn_pool

    @property
    def autocommit(self):
        """ Whether to automatically submit a transaction """

        return self.conn_pool.db.extra.autocommit

    @async_dict_formatter
    async def fetch(self, sql, args=None, rows=None):
        if args:
            args = tuple_formater(args)

        async with self.conn_pool.get() as conn:
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

    async def execute(self, sql, args=None, batch=False):
        sql = sql.strip()
        if args:
            args = tuple_formater(args)

        async with self.conn_pool.get() as conn:
            if not self.autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if batch is True:
                        await cur.executemany(sql, args or ())
                    else:
                        await cur.execute(sql, args or ())
                    affected = cur.rowcount
                    last_id = cur.lastrowid
                if not self.autocommit:
                    await conn.commit()
            except BaseException:
                if not self.autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
            return Dict(last_id=last_id, affected=affected)

    async def close(self):
        """ A coroutine that close self.conn_pool. """

        await self.conn_pool.close()

    def connect_status(self):
        """ Current connection pool status """

        return self.conn_pool.status

    def connect_info(self):
        """ Db info """

        return self.conn_pool.db


class RequestClient:
    """
        Responsible for initiating read and write requests from the model layer
    """

    executer = None

    def __init__(self):
        if self.executer is None:
            raise NoBindError(
                'RequestClient no binding db or closed, maybe call Trod().bind()'
            )

    def __repr__(self):
        _bind = self.get_conn_info()
        _db = _bind.db.db if _bind else None
        return "<class '{}' bind to '{}'>".format(
            self.__class__.__name__, _db
        )

    __str__ = __repr__

    @classmethod
    async def bind_db(cls, **kwargs):
        """ A coroutine that bind db for `RequestClient` """

        useful_kwargs = kwargs.copy()
        for arg, value in kwargs.items():
            if value is None:
                useful_kwargs.pop(arg)
        if cls.executer is not None:
            raise DuplicateBindError('Duplicate database binding')
        connect_pool = await Connector.create(**useful_kwargs)
        cls.executer = Executer(connect_pool)
        return True

    @classmethod
    async def bind_db_by_conn(cls, connector):
        """ A coroutine that bind db for `RequestClient` by connector """

        if cls.executer is not None:
            raise DuplicateBindError('Duplicate database binding')
        cls.executer = Executer(connector)
        return True

    @classmethod
    async def unbind(cls):
        """ A coroutine that call `executer.close()` to unbind db"""

        if cls.executer is not None:
            await cls.executer.close()
            cls.executer = None
        else:
            Logger.warning('No binding db connection or closed')
        return True

    @classmethod
    def is_usable(cls):
        """ return a bool is bind a db """

        return bool(cls.executer)

    @classmethod
    def get_conn_status(cls):
        """ db Connection pool status """

        if cls.executer:
            return cls.executer.connect_status()
        return {}

    @classmethod
    def get_conn_info(cls):
        """ db self info """

        if cls.executer:
            return cls.executer.connect_info()
        return {}

    async def execute(self, excu_sql, values=None, is_batch=False):
        """ A coroutine that proxy execute sql request """

        return await self.executer.execute(
            excu_sql, args=values, batch=is_batch
        )

    async def exist(self, exist_sql):
        """ A coroutine that return a bool is table is exist """

        result = await self.executer.fetch(exist_sql)
        return bool(result)

    async def fetch(self, query, args=None, rows=None):
        """ A coroutine that proxy fetch sql request """

        return await self.executer.fetch(
            query, args=args, rows=rows
        )

    async def text(self, sql, args=None, rows=None, batch=False):
        """ A coroutine that execute sql text """

        is_fetch = True
        if 'SELECT' in sql or 'select' in sql:
            result = await self.executer.fetch(sql, args=args, rows=rows)
        else:
            is_fetch = False
            result = await self.executer.execute(sql, args=args, batch=batch)
        return Dict(is_fetch=is_fetch, data=result)
