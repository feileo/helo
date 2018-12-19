# -*- coding=utf8 -*-
"""
"""
import sys

import aiomysql

from trod.connector import Connector
from trod.executer import Executer
from trod.utils import async_dict_formatter


class Session:
    __affected__ = None

    def __init__(self, connector):
        if not isinstance(connector, Connector):
            raise TypeError("Parameter connector must be 'Connector' type")
        self.connector = connector

    async def close(self):
        await self.connector.close()

    @async_dict_formatter
    async def select(self, sql, args=None, rows=None):
        async with self.connector.get() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args or ())
                    if rows:
                        result = await cur.fetchmany(rows)
                    else:
                        result = await cur.fetchall()
                    return result
                except BaseException:
                    exc_type, exc_value, _ = sys.exc_info()
                    raise exc_type(exc_value)

    async def insert(self, sql, args=None, autocommit=True):
        async with self.connector.get() as conn:
            if not autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if args:
                        await cur.execute(sql, args)
                    else:
                        await cur.execute(sql)
                    self.__affected__ = cur.rowcount
                    last_id = cur.lastrowid
                if not autocommit:
                    await conn.commit()
            except BaseException:
                if not autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                raise exc_type(exc_value)
            return last_id

    submit = update = delete = insert
