import sys

import aiomysql

from trod.db_.connector import Connector
from trod import utils


class Result:

    def __init__(self, exec_ret):
        self.ret = exec_ret
        self.fetch = None
        self.use_troddict = None


class FetchRet(Result):

    def __init__(self, exec_ret):
        self.fetch = True
        super().__init__(exec_ret)

    def tdicts(self):
        self.use_troddict = True


class ExecRet(Result):
    def __init__(self, exec_ret):
        self.fetch = False
        super().__init__(exec_ret)


class Executer:
    """ MySQL SQL executer """

    connector = None

    @classmethod
    def init(cls, connector):
        if not isinstance(connector, Connector):
            raise RuntimeError()
        cls.connector = connector
        cls.autocommit = cls.connector.connmeta.autocommit

    @classmethod
    async def _fetch(cls, sql, args=None, rows=None):

        if args:
            args = utils.tuple_formatter(args)

        async with cls.connector.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql.strip(), args or ())
                    if rows and rows == 1:
                        result = await cur.fetchone()
                    elif rows:
                        result = await cur.fetchmany(rows)
                    else:
                        result = await cur.fetchall()
                    return FetchRet(result)
                except BaseException:
                    exc_type, exc_value, _ = sys.exc_info()
                    error = exc_type(exc_value)
                    raise error

    @classmethod
    async def _execute(cls, sql, args=None, batch=False):
        sql = sql.strip()
        if args:
            args = utils.tuple_formatter(args)

        async with cls.connector.acquire() as conn:
            if not cls.autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if batch is True:
                        await cur.executemany(sql, args or ())
                    else:
                        await cur.execute(sql, args or ())
                    affected, last_id = cur.rowcount, cur.lastrowid
                if not cls.autocommit:
                    await conn.commit()
            except BaseException:
                if not cls.autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
            return ExecRet(utils.TrodDict(last_id=last_id, affected=affected))

    @classmethod
    async def fetch(cls, sql, args=None, rows=None):
        """ A coroutine that proxy fetch sql request """

        return await cls._fetch(sql, args=args, rows=rows)

    @classmethod
    async def execute(cls, sql, values=None, is_batch=False):
        """ A coroutine that proxy execute sql request """

        return await cls._execute(sql, args=values, batch=is_batch)

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
