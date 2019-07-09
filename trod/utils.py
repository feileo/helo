import inspect
from functools import wraps
from collections.abc import Iterable

__all__ = (
    'TrodDict',
    'troddict_formatter',
    'singleton',
    'asyncinit',
)


class TrodDict(dict):
    """ Is a class that makes it easier to access the elements of the dict

        EX::
            dict_ = TrodDict(key=1)
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
                f"TrodDict object has not attribute {key}."
            )

    def __setattr__(self, key, value):
        self[key] = value

    def __str__(self):
        pass

    __repr__ = __str__

    def __iadd__(self, rhs):
        self.update(rhs)
        return self

    def __add__(self, rhs):
        td = TrodDict(self)
        td.update(rhs)
        return td

    def from_object(self, obj):
        if not isinstance(obj, type):
            raise ValueError(f'Invalid obj type: {obj}')

        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)


def troddict_formatter(is_async=False):
    """ A function decorator that convert the returned dict object to TrodDict
        If it is a list, recursively convert its elements
    """
    def decorator(func):
        if not is_async:
            @wraps(func)
            def convert(*args, **kwargs):
                result = func(*args, **kwargs)
                return format_troddict(result)
        else:
            @wraps(func)
            async def convert(*args, **kwargs):
                result = await func(*args, **kwargs)
                return format_troddict(result)
        return convert
    return decorator


def singleton(is_async=False):
    """ A singleton decorator of asyncinit class """

    def decorator(cls):

        instances = {}

        if is_async:
            async def getinstance(*args, **kw):
                if cls not in instances:
                    instances[cls] = await cls(*args, **kw)
                return instances[cls]
        else:
            def getinstance(*args, **kw):
                if cls not in instances:
                    instances[cls] = cls(*args, **kw)
                return instances[cls]
        return getinstance

    return decorator


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


def format_troddict(target):
    if target is None:
        return target
    if isinstance(target, dict):
        return _do_troddict_format(target)
    if isinstance(target, Iterable):
        fmt_result = []
        for item in target:
            if isinstance(item, (str, int, bool)):
                raise ValueError(f"Invalid data type '{target}' to convert `TrodDict`")
            fmt_result.append(format_troddict(item))
        return fmt_result
    raise ValueError(f"Invalid data type '{target}' to convert `TrodDict`")


async def _new(cls, *_args, **_kwargs):
    return object.__new__(cls)


def _force_async(f_n):
    if inspect.iscoroutinefunction(f_n):
        return f_n

    async def wrapped(*args, **kwargs):
        return f_n(*args, **kwargs)

    return wrapped


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


def tuple_formatter(args):
    """ Arg to tuple """

    res_args = None
    if not args:
        return res_args
    if isinstance(args, list):
        res_args = tuple(args)
    elif isinstance(args, str):
        res_args = tuple([args])
    return res_args


def _do_troddict_format(ori_dict):
    tdict = TrodDict()
    for key, value in ori_dict.items():
        tdict[key] = _do_troddict_format(value) if isinstance(value, dict) else value
    return tdict
