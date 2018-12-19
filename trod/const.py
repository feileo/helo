# -*- coding:utf-8 -*-

from enum import Enum, unique


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


space = '   |=> '


SCHEMES = ['mysql']


@unique
class Schemes(Enum):
    """ Schemes """

    MYSQL = 1

    @classmethod
    def all(cls):
        """ all scheme name list """
        return [scheme.lower() for scheme in [cls.MYSQL.name]]
