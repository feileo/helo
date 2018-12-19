#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from trod import DBconpool, space
from trod import NoDbConnPathError
from trod import MODEL_LIST
from component import EventLogger


class BaseTask(object):
    conn_path = None
    tasks_list = []

    @classmethod
    async def start(cls, loop):
        if cls.conn_path is None:
            raise NoDbConnPathError('No \'conn_path\'')
        await DBconpool.create_dbcon_pool(loop, cls.conn_path)
        cls.log()
        EventLogger.info('started')

    @classmethod
    async def end(cls):
        await DBconpool.close_pool()
        EventLogger.info('finished')

    @classmethod
    def log(cls):
        for each_model in MODEL_LIST:
            EventLogger.info('{} linking model \'{}\' to conn_path \'{}\''.format(space, each_model,
                                                                                  DBconpool.get_db_name()), task='building')

    # @classmethod
    # def task_run(cls):
    # 	raise NotImplementedError
