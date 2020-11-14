from typing import Callable, Optional

from helo import _sql
from helo.types import core


class _Func(_sql.ClauseElement):

    __slots__ = ('_func', '_element')

    def __init__(
        self,
        func: str,
        element: Optional[_sql.ClauseElement]
    ) -> None:
        self._func = func.upper()
        self._element = element

    def __getattr__(self, func: str) -> Callable:

        def decorator(*args, **kwargs):
            return _Func(func, *args, **kwargs)

        return decorator

    def as_(self, alias: str) -> core.Alias:
        return core.Alias(self, alias)

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(self._func)
        with ctx(parens=True):
            ctx.sql(self._element)
        return ctx


Func = _Func("", None)
