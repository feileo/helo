import sys

import aiomysql

from trod import utils


class TrodDictCursor(aiomysql.DictCursor):
    dict_type = utils.TrodDict


async def _fetch(pool, sql, args=None, rows=None):

    if args:
        args = utils.tuple_formatter(args)

    async with pool.acquire() as connect:
        async with connect.cursor(aiomysql.DictCursor) as cur:
            try:
                await cur.execute(sql.strip(), args or ())
                if rows and rows == 1:
                    result = await cur.fetchone()
                elif rows:
                    result = await cur.fetchmany(rows)
                else:
                    result = await cur.fetchall()
            except BaseException:
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
    return result


async def _execute(pool, sql, args=None, batch=False):
    sql = sql.strip()
    if args:
        args = utils.tuple_formatter(args)

    async with pool.acquire() as connect:
        if not pool.connectmeta.autocommit:
            await connect.begin()
        try:
            async with connect.cursor(TrodDictCursor) as cur:
                if batch is True:
                    await cur.executemany(sql, args or ())
                else:
                    await cur.execute(sql, args or ())
                affected, last_id = cur.rowcount, cur.lastrowid
            if not pool.connectmet.autocommit:
                await connect.commit()
        except BaseException:
            if not pool.connectmeta.autocommit:
                await connect.rollback()
            exc_type, exc_value, _ = sys.exc_info()
            error = exc_type(exc_value)
            raise error

    return affected, last_id


async def fetch(sql, pool, args=None, rows=None):
    """ A coroutine that proxy fetch sql request """

    return await _fetch(pool, sql, args=args, rows=rows)


async def execute(pool, sql, values=None, is_batch=False):
    """ A coroutine that proxy execute sql request """

    return await _execute(pool, sql, args=values, batch=is_batch)
