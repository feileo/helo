# # -*- coding:utf-8 -*-
# from .dbconpool import DBconpool
# from .model import Model, MODEL_LIST
# from .datafield import IntegerField, StringField, DatetimeField, FloatField, DecimalField, TimestampField
# from .indexfield import UniqueKey, Key
# from component import EventLogger
# from .const import toDict, space
# from .dbconpool import DBconpool
# from .error import NoDbConnPathError


# async def create_all(module):
#     for key, value in vars(module).items():
#         if hasattr(value, '__base__') and value.__base__ is Model:
#             if not await value.status():
#                 await value.create_table()
#                 EventLogger.info('created <Table \'{}\'> in <db: \'{}\'>'.format(key, DBconpool.get_db_name()))
#             # else:
#             # 	EventLogger.warning('<Table \'{}>\' already exists"'.format(key))


# async def drop_all(module):
#     for key, value in vars(module).items():
#         if hasattr(value, '__base__') and value.__base__ is Model:
#             if await value.status():
#                 await value.drop_table()
#                 EventLogger.info('dropped <Table \'{}\'> from <db: \'{}\'>'.format(key, DBconpool.get_db_name()))
from trod.model import Model
from trod.db import Transitioner


class Trod:

    Model = Model

    @classmethod
    async def bind(cls, url,
                   minsize=None, maxsize=None,
                   timeout=None, pool_recycle=None,
                   echo=None, loop=None, **kwargs):

        await Transitioner.bind_db(url=url)

    @classmethod
    async def unbind(cls):
        await Transitioner.close()
