from collections.abc import Iterable
from functools import wraps, reduce
from inspect import iscoroutinefunction, isclass, signature

__all__ = (
    'tdict',
    'tdictformatter',
    'singleton',
    'asyncinit',
    'argschecker',
    'and_',
    'or_',
    'FreeObject',
)


class tdict(dict):  # pylint: disable=invalid-name
    """ Is a class that makes a dictionary behave like an object,
        with attribute-style access.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        keys = kwargs.pop("keys", None)
        values = kwargs.pop("values", None)

        if keys and values:
            for key, value in zip(keys, values):
                self[key] = value

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(
                f"tdict object has not attribute {key}."
            )

    def __setattr__(self, key, value) -> None:
        self[key] = value

    def __iadd__(self, other):
        self.update(other)
        return self

    def __add__(self, other):
        td = self.copy()
        td.update(other)
        return td

    def copy(self):
        return tdict(**super().copy())


def tdictformatter(func):
    """ A function decorator that convert the returned dict object to tdict
        If it is a list, recursively convert its elements
    """
    if iscoroutinefunction(func):
        @wraps(func)
        async def convert(*args, **kwargs):
            result = await func(*args, **kwargs)
            return formattdict(result)
    else:
        @wraps(func)
        def convert(*args, **kwargs):
            result = func(*args, **kwargs)
            return formattdict(result)

    return convert


class FreeObject:

    def __init__(self, **kwargs):
        for name in kwargs:
            setattr(self, name, kwargs[name])

    def __bool__(self):
        return bool(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __contains__(self, name):
        return name in self.__dict__

    def __getitem__(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            raise KeyError(name)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def __delitem__(self, name):
        delattr(self, name)

    def __iter__(self):
        return iter(self.__dict__)

    def __iadd__(self, other):
        self.__dict__.update(other)

    def __add__(self, other):
        return self.as_new(**other)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return f'FreeObject({self.__dict__!r})'

    def as_new(self, **values):
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        if values:
            c.__dict__.update(values)
        return c


def singleton(cls):
    """ A singleton decorator of asyncinit class """

    instances = {}

    if iscoroutinefunction(cls):
        @wraps(cls)
        async def getinstance(*args, **kw):
            if cls not in instances:
                instances[cls] = await cls(*args, **kw)
            return instances[cls]
    else:
        @wraps(cls)
        def getinstance(*args, **kw):
            if cls not in instances:
                instances[cls] = cls(*args, **kw)
            return instances[cls]

    return getinstance


def asyncinit(obj):
    """A class decorator that add async `__init__` functionality."""

    if not isclass(obj):
        raise ValueError("Decorated object must be a class")

    async def nnew(cls, *_args, **_kwargs):
        return object.__new__(cls)

    def force_async(f):
        if iscoroutinefunction(f):
            return f

        async def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    if obj.__new__ is object.__new__:
        cls_new = nnew
    else:
        cls_new = force_async(obj.__new__)

    @wraps(obj.__new__)
    async def new(cls, *args, **kwargs):
        self = await cls_new(cls, *args, **kwargs)

        cls_init = force_async(self.__init__)
        await cls_init(*args, **kwargs)

        return self

    obj.__new__ = new

    return obj


def argschecker(*cargs, **ckwargs):

    def decorator(func):
        # If in optimized mode, disable type checking
        if not __debug__:
            return func

        nullable = ckwargs.pop('nullable', True)

        # Map function argument names to supplied types
        sig = signature(func)
        bound_args = sig.bind_partial(*cargs, **ckwargs).arguments

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_values = sig.bind(*args, **kwargs)
            # Enforce type assertions across supplied arguments
            for name, value in bound_values.arguments.items():
                if name in bound_args:
                    if not isinstance(value, bound_args[name]):
                        raise TypeError(
                            f"Argument {name} must be {bound_args[name]}, "
                            f"now got {type(value)}"
                        )
                    if not nullable and not value:
                        raise ValueError(f"Arguments {name} cannot be empty")
            return func(*args, **kwargs)
        return wrapper

    return decorator


def and_(*exprs):
    return reduce(lambda a, b: a & b, exprs)


def or_(*exprs):
    return reduce(lambda a, b: a | b, exprs)


def formattdict(original):

    def do_format(ori_dict):
        td = tdict()
        for key, value in ori_dict.items():
            td[key] = do_format(value) if isinstance(value, dict) else value
        return td

    if original is None:
        return original

    if isinstance(original, dict):
        return do_format(original)

    if isinstance(original, Iterable):
        fmted = []
        for item in original:
            if not isinstance(item, (list, tuple, dict)):
                raise TypeError(
                    f"Invalid data type '{original}' to convert `tdict`"
                )
            fmted.append(formattdict(item))
        return fmted
    raise TypeError(f"Non-iterable object can not '{original}' to convert `tdict`")
