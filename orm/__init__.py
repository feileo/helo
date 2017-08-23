#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from  .dbconpool import  DBconpool
from .model import Model
from .datafield import IntegerField,StringField,DatetimeField,FloatField,DecimalField,TimestampField
from .indexfield import UniqueKey,Key
from logger import EventLogger

async def create_all(module):
    for key, value in vars(module).items():
        if hasattr(value, '__base__') and value.__base__ is Model:
        	if not await value.status():
        		await value.create_table()
        		EventLogger.log('created <Table \'{}\'>'.format(key))
        	# else:
        	# 	EventLogger.warning('<Table \'{}>\' already exists"'.format(key))


async def drop_all(module):
    for key, value in vars(module).items():
    	if hasattr(value, '__base__') and value.__base__ is Model:
            if await value.status():
                await value.drop_table()
                EventLogger.log('dropped <Table \'{}\'>'.format(key))

        		