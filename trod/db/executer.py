import sys

import aiomysql

from trod.db.connector import Connector
from trod.utils import async_dict_formatter


class Executer:
    """ 执行 sql """

    def __init__(self, conn_pool):
        if not isinstance(conn_pool, Connector):
            raise TypeError('Init conn_pool must be `Connector` type')
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


class Transitioner:

    executer = None

    def __init__(self, query, args=None, rows=None):
        self.query = query
        self.args = args
        self.rows = rows

    @classmethod
    async def bind_db(cls, **kwargs):
        if cls.executer is not None:
            raise RuntimeError('Duplicate binding')
        connect_pool = await Connector.create(**kwargs)
        cls.executer = Executer(connect_pool)

    @classmethod
    async def bind_db_by_conn(cls, connector):
        if cls.executer is not None:
            raise RuntimeError('Duplicate binding')
        cls.executer = Executer(connector)

    @classmethod
    async def close(cls):
        if cls.executer is not None:
            await cls.executer.close()

    @classmethod
    async def execute(cls, excu_sql, values=None, batch=False):
        return await cls.executer.execute(
            excu_sql, args=values, batch=batch
        )

    @classmethod
    async def exist(cls, exist_sql):
        result = await cls.executer.fetch(exist_sql)
        return bool(result)

    @classmethod
    async def text(cls, sql, args=None, rows=None):
        return await cls.executer.fetch(sql, args=args, rows=rows)

    async def fetch(self):
        return await self.executer.fetch(
            self.query, args=self.args, rows=self.rows
        )
