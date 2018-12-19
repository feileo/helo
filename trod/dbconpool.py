#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 数据库连接池

import aiomysql

from component import EventLogger
from trod.connector import Connector


class DBconpool():
    """
    创建一个db连接池。
    使用连接池的好处是不必频繁地打开和关闭数据库连接，能复用就尽量复用。
    连接池由db_con_pool存储，缺省情况下将编码设置为utf8，自动提交事务：
    """
    db_con_pool = None
    __con_db_name = None

    @classmethod
    async def create_dbcon_pool(cls, loop, url):
        cls.__con_db_name = 'trod'  # kwargs['db']
        EventLogger.info('create database connection pool', task='building')
        cls.db_con_pool = await Connector.from_url(url, loop=loop)

    @classmethod
    async def close_pool(cls):
        await cls.db_con_pool.close()

    @classmethod
    def get_db_name(cls):
        return cls.__con_db_name
