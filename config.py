#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 数据库连接池设置
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

class Dict(dict):
    def __init__(self, names=(), values=(), **kwargs):
        super(Dict, self).__init__(**kwargs)
        for key, value in zip(names, values):
            self[key] = value     
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"Dict object has not attribute {}".format(key))
    def __setattr__(self, key, value):
        self[key] = value

def toDict(db_seting):
    rdb_seting = Dict()
    for key, value in db_seting.items():
        rdb_seting[key] = toDict(value) if isinstance(value, dict) else value
    return rdb_seting


DB_PATH = toDict(DB_SETING)