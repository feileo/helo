#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from component import EventLogger
import asyncio
import aiomysql

class DBconpool(object):
	db_con_pool = None
	__con_db_name = None
	
	"""
	创建一个db连接池。
	使用连接池的好处是不必频繁地打开和关闭数据库连接，能复用就尽量复用。
	连接池由db_con_pool存储，缺省情况下将编码设置为utf8，自动提交事务：
	"""

	@classmethod
	async def create_dbcon_pool(cls,loop, **kwargs):
		cls.__con_db_name = kwargs['db']
		EventLogger.info('create database connection pool',task='building')
		cls.db_con_pool = await aiomysql.create_pool(
	        host=kwargs.get('host', 'localhost'),
	        port=kwargs.get('port', 3306),
	        user=kwargs['user'],
	        password=kwargs['password'],
	        db=kwargs['db'],
	        charset=kwargs.get('charset', 'utf8'),
	        autocommit=kwargs['autocommit'],
	        maxsize=kwargs['maxsize'],
	        minsize=kwargs['minsize'],
	        loop=loop
	    )

	@classmethod
	async def close_pool(cls):
	    EventLogger.info('close database connection pool')
	    if cls.db_con_pool is not None:
	        cls.db_con_pool.close()
	        await cls.db_con_pool.wait_closed()

	@classmethod
	def get_db_name(cls):
		return cls.__con_db_name
