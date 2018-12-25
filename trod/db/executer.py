import sys

import aiomysql

from trod.db.connector import Connector
from trod.extra.logger import Logger
from trod.utils import async_dict_formatter, Dict


class Executer:
    """ 执行 sql """

    def __init__(self, conn_pool):
        if not isinstance(conn_pool, Connector):
            raise ValueError('Init connection pool must be `Connector` type')
        self.conn_pool = conn_pool

    @async_dict_formatter
    async def fetch(self, sql, args=None, rows=None):
        async with self.conn_pool.get() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args or ())
                    if rows and rows == 1:
                        result = await cur.fetchone()
                    elif rows:
                        result = await cur.fetchmany(rows)
                    else:
                        result = await cur.fetchall()
                    return result
                except BaseException:
                    exc_type, exc_value, _ = sys.exc_info()
                    raise exc_type(exc_value)

    @async_dict_formatter
    async def execute(self, sql, args=None, batch=False, autocommit=True):
        async with self.conn_pool.get() as conn:
            if not autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    executer = cur.execute if not batch else cur.executemany
                    if args:
                        await executer(sql, args)
                    else:
                        await executer(sql)
                    affected = cur.rowcount
                    last_id = cur.lastrowid
                if not autocommit:
                    await conn.commit()
            except BaseException:
                if not autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                raise exc_type(exc_value)
            return {'last_id': last_id, 'affected': affected}

    async def close(self):
        await self.conn_pool.close()

    def connect_status(self):
        return self.conn_pool.status

    def connect_info(self):
        return self.conn_pool.me


class RequestClient:

    executer = None

    def __init__(self):
        if self.executer is None:
            raise RuntimeError(
                'RequestClient no binding db or closed, maybe call Trod().bind()'
            )

    @classmethod
    async def bind_db(cls, **kwargs):
        for arg, value in kwargs.items():
            if value is None:
                kwargs.pop(arg)
        if cls.executer is not None:
            raise RuntimeError('Duplicate database binding')
        connect_pool = await Connector.create(**kwargs)
        cls.executer = Executer(connect_pool)
        return True

    @classmethod
    async def bind_db_by_conn(cls, connector):
        if cls.executer is not None:
            raise RuntimeError('Duplicate database binding')
        cls.executer = Executer(connector)
        return True

    @classmethod
    async def close(cls):
        if cls.executer is not None:
            await cls.executer.close()
        else:
            Logger.warning('No binding db connection or closed')
        return True

    @classmethod
    def is_usable(cls):
        return bool(cls.executer)

    @classmethod
    def get_conn_status(cls):
        if cls.executer:
            return cls.executer.connect_status()
        return {}

    @classmethod
    def get_conn_info(cls):
        if cls.executer:
            return cls.executer.connect_info()
        return {}

    async def execute(self, excu_sql, values=None, batch=False):
        return await self.executer.execute(
            excu_sql, args=values, batch=batch
        )

    async def exist(self, exist_sql):
        result = await self.executer.fetch(exist_sql)
        return bool(result)

    async def fetch(self, query, args=None, rows=None):
        return await self.executer.fetch(
            query, args=args, rows=rows
        )

    async def text(self, sql, args=None, rows=None):
        is_fetch = True
        if sql.find('SELECT"') or sql.find('select'):
            result = await self.executer.fetch(sql, args=args, rows=rows)
        else:
            is_fetch = False
            result = await self.executer.execute(sql, args=args, rows=rows)
        return Dict(is_fetch=is_fetch, data=result)
