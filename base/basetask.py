#!/test_objsr/bin/env python3
# -*- coding:utf-8 -*-
# 
from orm import DBconpool
# import asyncio

class BaseTask(object):
	conn_path = None

	def __init__(self):
		if self.conn_path is None:
			raise
	@classmethod
	async def start(cls,loop):
		await DBconpool.create_dbcon_pool(loop=loop, **cls.conn_path)

	@classmethod
	async def end(cls):
		await DBconpool.close_pool()

	# @classmethod
	# async def get_loop(cls):
	# 	return cls.loop



