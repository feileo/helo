#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 表操作
import asyncio
import aiomysql
import sys
from .dbconpool import DBconpool
from .error import ArgTypeError


class Table(DBconpool):
    __affected__ = None

    @classmethod
    async def select(cls, sql, args=None, rows=None):
        print(sql, args)
        async with cls.db_con_pool.get() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql, args or ())
                    if rows:
                        rs = await cur.fetchmany(rows)
                    else:
                        rs = await cur.fetchall()
                    return rs
                except:
                    exc_type, exc_value, _ = sys.exc_info()
                    raise exc_type(exc_value)

    @classmethod
    async def submit(cls, sql, args=None, autocommit=True):
        print(sql, args)
        async with cls.db_con_pool.get() as conn:
            if not autocommit:
                await conn.begin()
            try:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if args:
                        await cur.execute(sql, args)
                    else:
                        await cur.execute(sql)
                    cls.__affected__ = cur.rowcount
                    last_id = cur.lastrowid
                if not autocommit:
                    await conn.commit()
            except BaseException as e:
                if not autocommit:
                    await conn.rollback()
                exc_type, exc_value, _ = sys.exc_info()
                raise exc_type(exc_value)
            return last_id

    @classmethod
    async def create_table(cls):
        await cls.submit(cls.__create_sql__)

    @classmethod
    def show_create_table(cls):
        show_str = cls.__create_sql__.replace(',', ',\n')
        print(show_str)

    @classmethod
    async def drop_table(cls):
        drop_sql = cls.__drop_sql__.format(cls.get_db_name(), cls.__table__)
        await cls.submit(drop_sql)

    @classmethod
    async def status(cls):
        execute_sql = "SELECT table_name FROM information_schema.TABLES WHERE table_name = '{}' AND table_schema='{}';".format(
            cls.__table__, cls.get_db_name())
        res = await cls.select(execute_sql)
        if len(res) > 0:
            return True
        else:
            return False

    @classmethod
    async def select_all(cls):
        sql = 'SELECT {},{} FROM {} ORDER BY {};'
        execute_sql = sql.format(cls.__primary_key__, ','.join(cls.__fields__), cls.__table__, cls.__primary_key__)
        res = await cls.select(execute_sql)
        object = []
        for n in res:
            object.append(n)
        return object

    @classmethod
    async def select_eq_filter(cls, **kwargs):
        keys = []
        values = []
        for key, value in kwargs.items():
            keys.append(key)
            values.append(value)
        where_clause = ' AND '.join(['='.join([n, '%s']) for n in keys])
        execute_sql = cls.__select__.format(cls.__primary_key__, ','.join(cls.__fields__),
                                            cls.__table__,
                                            ('WHERE {}'.format(where_clause)) if where_clause else '',
                                            cls.__primary_key__)
        res = await cls.select(execute_sql, values)
        object = []
        for n in res:
            object.append(n)
        return object

    @classmethod
    async def select_like_filter(cls, **kwargs):
        keys = []
        values = []
        for key, value in kwargs.items():
            keys.append(key)
            values.append(value.join(['%'] * 2))
        where_clause = ' AND '.join([' LIKE '.join([n, '%s']) for n in keys])
        execute_sql = cls.__select__.format(cls.__primary_key__, ','.join(cls.__fields__),
                                            cls.__table__,
                                            ('WHERE {}'.format(where_clause)) if where_clause else '',
                                            cls.__primary_key__)
        res = await cls.select(execute_sql, values)
        object = []
        for n in res:
            object.append(n)
        return object

    @classmethod
    async def select_custom_filter(cls, **kwargs):
        keys = []
        patterns = []
        for key, pattern in kwargs.items():
            keys.append(key)
            patterns.append(pattern)
        where_clause = ' AND '.join([pa.format(ke) for ke, pa in zip(keys, patterns)])
        execute_sql = cls.__select__.format(cls.__primary_key__, ','.join(cls.__fields__),
                                            cls.__table__,
                                            ('WHERE {}'.format(where_clause)) if where_clause else '',
                                            cls.__primary_key__)
        print(execute_sql)
        res = await cls.select(execute_sql)
        object = []
        for n in res:
            object.append(n)
        return object

    @classmethod
    async def select_custom_where(cls, where_clause):
        execute_sql = cls.__select__.format(cls.__primary_key__, ','.join(cls.__fields__),
                                            cls.__table__,
                                            where_clause,
                                            cls.__primary_key__)
        res = await cls.select(execute_sql)
        object = []
        for n in res:
            object.append(n)
        return object

    @classmethod
    def _remove_sql(cls, uid, where):
        if uid is None:
            where_data = where
        else:
            where_data = {cls.__primary_key__: uid}
        sql = 'DELETE FROM {} WHERE {};'
        where_fields = where_data.keys()
        where_values = [where_data[n] for n in where_fields]
        where_clause = ' AND '.join(['='.join([n, '%s']) for n in where_fields])
        execute_sql = sql.format(cls.__table__, where_clause)
        return execute_sql, where_values

    @classmethod
    async def remove(cls, uid=None, where={}):
        sql, values = cls._remove_sql(uid, where)
        await cls.submit(sql, values)

    @classmethod
    async def remove_by_ids(cls, uid=[]):
        if len(uid) == 0:
            return
        sql = 'DELETE FROM {} WHERE {} IN ({});'
        in_claues = ','.join(['%s'] * len(uid))
        execute_sql = sql.format(cls.__table__, cls.__primary_key__, in_claues)
        await cls.submit(execute_sql, uid)

    # 暂时不包含pk,so 必须 AI
    @classmethod
    def _batch_insert_sql(cls, data):
        if isinstance(data, list):
            insert_data = data
        else:
            insert_data = [data]
        sql = 'INSERT INTO {} ({}) VALUES {};'
        fields = []
        value_list = []
        for n in insert_data[0].keys():
            fields.append(n)
        for each_data in insert_data:
            tmp = [each_data[n] for n in fields]
            value_list.extend(tmp)
        symbols_num = len(fields)
        single_symbol = '({0})'.format(','.join(['%s'] * symbols_num))
        execute_sql = sql.format(cls.__table__, ','.join(fields), ','.join([single_symbol] * len(insert_data)))
        return execute_sql, value_list

    @classmethod
    def _insert_sql(cls, data):
        sql = 'INSERT INTO {} ({}) VALUES ({});'
        fields = []
        values = []
        for n in data.keys():
            fields.append(n)
            values.append(data[n])
        execute_sql = sql.format(cls.__table__, ','.join(fields), ','.join(['%s'] * len(fields)))
        return execute_sql, values

    @classmethod
    async def insert(cls, data):
        if not isinstance(data, dict):
            raise ArgTypeError('type error')
        sql, values = cls._insert_sql(data)
        return await cls.submit(sql, values)

    @classmethod
    async def batch_insert(cls, data):
        sql, values = cls._batch_insert_sql(data)
        await cls.submit(sql, values)

    @classmethod
    async def conditional_insert(cls, data, where):
        fields = []
        values = []
        for n in data.keys():
            fields.append(n)
            values.append(data[n])
        obj = cls.select_eq_filter(**where)
        if obj:
            await cls.update(what=data, where=where)
        else:
            await cls.insert(data=data)
