import sys

import aiomysql

from trod import utils


class TdictCursor(aiomysql.DictCursor):
    dict_type = utils.Tdict


async def _fetch(pool, sql, args=None, rows=None, db=None):

    if args:
        args = utils.tuple_formatter(args)

    async with pool.acquire() as connect:

        if db:
            await connect.select_db(db)
        elif not pool.connmeta.db:
            raise RuntimeError()  # TODO

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


async def _execute(pool, sql, args=None, batch=False, db=None):
    sql = sql.strip()
    if args:
        args = utils.tuple_formatter(args)

    async with pool.acquire() as connect:

        if db:
            await connect.select_db(db)
        elif not pool.connmeta.db:
            raise RuntimeError()  # TODO

        if not pool.connectmeta.autocommit:
            await connect.begin()
        try:
            async with connect.cursor(TdictCursor) as cur:
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


async def fetch(sql, pool, args=None, rows=None, db=None):
    """ A coroutine that proxy fetch sql request """

    return await _fetch(pool, sql, args=args, rows=rows, db=db)


async def execute(pool, sql, values=None, is_batch=False, db=None):
    """ A coroutine that proxy execute sql request """

    return await _execute(pool, sql, args=values, batch=is_batch, db=db)
