import inspect
from functools import wraps


class Dict(dict):
    """ Is a class that makes it easier to access the elements of the dict

        For example:
            dict_ = Dict(key=1)
            you can:
            dict_.k
    """

    def __init__(self, names=(), values=(), **kwargs):
        super().__init__(**kwargs)
        for key, value in zip(names, values):
            self[key] = value

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(
                f"Dict object has not attribute {key}"
            )

    def __setattr__(self, key, value):
        self[key] = value


def asyncinit(obj):
    """
        A class decorator that add async `__init__` functionality.
    """

    if not inspect.isclass(obj):
        raise ValueError("decorated object must be a class")

    if obj.__new__ is object.__new__:
        cls_new = _new
    else:
        cls_new = _force_async(obj.__new__)

    @wraps(obj.__new__)
    async def new(cls, *args, **kwargs):
        self = await cls_new(cls, *args, **kwargs)

        cls_init = _force_async(self.__init__)
        await cls_init(*args, **kwargs)

        return self

    obj.__new__ = new

    return obj


async def _new(cls, *_args, **_kwargs):
    return object.__new__(cls)


def _force_async(f_n):
    if inspect.iscoroutinefunction(f_n):
        return f_n

    async def wrapped(*args, **kwargs):
        return f_n(*args, **kwargs)

    return wrapped


def singleton(cls):
    """ A singleton decorator of asyncinit class """

    instances = {}

    @wraps(cls)
    async def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = await cls(*args, **kw)
        return instances[cls]
    return getinstance


def dict_formatter(func):
    """ A function decorator that convert the returned dict object to Dict
        If it is a list, recursively convert its elements
    """

    @wraps(func)
    def convert(*args, **kwargs):
        result = func(*args, **kwargs)
        return _do_format(result)
    return convert


def async_dict_formatter(func):
    """ A coroutine decorator of dict_formatter """

    @wraps(func)
    async def convert(*args, **kwargs):
        result = await func(*args, **kwargs)
        return _do_format(result)
    return convert


def to_list(*args):
    """ Args to list """

    result = []
    if not args:
        return result
    for arg in args:
        if isinstance(arg, (list, tuple)):
            result.append(arg)
        elif not arg:
            result.append(arg)
        else:
            result.append([arg])
    return result


def tuple_formater(args):
    """ Arg to tuple """

    res_args = None
    if not args:
        return res_args
    if isinstance(args, list):
        res_args = tuple(args)
    elif isinstance(args, str):
        res_args = tuple([args])
    return res_args


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
        raise ValueError(f'Invalid data type {result} to convert Dict')
