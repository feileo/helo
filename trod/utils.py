# -*- coding=utf8 -*-
"""
# Description:
"""
import asyncio

from functools import wraps


class Dict(dict):
    """ 更便捷的 dict  """

    def __init__(self, names=(), values=(), **kwargs):
        super(Dict, self).__init__(**kwargs)
        for key, value in zip(names, values):
            self[key] = value

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(
                "Dict object has not attribute {}".format(key)
            )

    def __setattr__(self, key, value):
        self[key] = value


def _to_format_dict(ori_dict):
    r_dict = Dict()
    for key, value in ori_dict.items():
        r_dict[key] = _to_format_dict(value) if isinstance(value, dict) else value
    return r_dict


def _do_format(result):
    if result is None:
        return result
    elif isinstance(result, dict):
        return _to_format_dict(result)
    elif isinstance(result, (list, tuple)):
        fmt_result = []
        for item in result:
            fmt_result.append(_do_format(item))
        return fmt_result
    else:
        raise ValueError('Invalid data type to convert Dict')


def dict_formatter(func):
    """ 把返回的 dict 对象转成 Dict
        如果是列表，转换其元素
    """
    @wraps(func)
    def convert(*args, **kwargs):
        result = func(*args, **kwargs)
        return _do_format(result)
    return convert


def async_dict_formatter(func):
    """ 协程版 dict_formatter
    """
    @wraps(func)
    async def convert(*args, **kwargs):
        result = await func(*args, **kwargs)
        return _do_format(result)
    return convert


def singleton(cls):
    """ 单例装饰器 """
    instances = {}

    @wraps(cls)
    def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return getinstance


def to_list(args):
    if not args:
        return []
    elif isinstance(args, str):
        return [args]
    elif isinstance(args, (list, tuple)):
        result = []
        for arg in args:
            result.append(to_list(arg))
        return result
    raise TypeError(
        'Rags must be list or str, now get {}'.format(type(args))
    )
