#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 系统配置文件

from orm.const import toDict

# 临时文件目录配置
TEMP_FILES_DIR = '../Tempfiles'

# 数据库连接池配置
DB_SETING = {
    'test1': {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'db': 'test1',
        'autocommit' : True,
        'maxsize': 10,
        'minsize': 1
    },
    'test2':{
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'db': 'test2',
        'autocommit' : True,
        'maxsize': 10,
        'minsize': 1
    }
}

DB_PATH = toDict(DB_SETING)