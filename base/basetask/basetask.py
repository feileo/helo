#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from orm import DBconpool,space
from orm import NoDbConnPathError
from orm import MODEL_LIST
from logger import EventLogger

class BaseTask(object):
	conn_path = None
	tasks_list = []

	@classmethod
	async def start(cls,loop):
		if cls.conn_path is None:
			raise NoDbConnPathError('No \'conn_path\'')
		await DBconpool.create_dbcon_pool(loop=loop, **cls.conn_path)
		cls.log()
		EventLogger.log('started')

	@classmethod
	async def end(cls):
		await DBconpool.close_pool()
		EventLogger.log('finished')

	@classmethod
	def log(cls):
		for each_model in MODEL_LIST:
	   		EventLogger.log('{} linking model \'{}\' to conn_path \'{}\''.format(space,each_model,
	   			DBconpool.get_db_name()),task='building')

	# @classmethod
	# def task_run(cls):
	# 	raise NotImplementedError



