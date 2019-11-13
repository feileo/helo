"""
    trod
    ~~~~
"""

from types import ModuleType
from typing import Any, Optional, Type, Union, List, Tuple

from . import (
    types,
    util,
    err,
    db as _db_impl,
    _helper,
)

from .model import Model, ROWTYPE, JOINTYPE

__version__ = '0.0.15'
__all__ = (
    'types',
    'util',
    'err',
    'Model',
    'Trod',
    'ROWTYPE',
    'JOINTYPE',
)


class Trod:

    def __init__(self, url_key=None) -> None:
        if url_key is not None:
            self.set_url_key(url_key)

    async def bind(self, url: Optional[str] = None, **kwargs: Any) -> bool:

        return await _db_impl.binding(url, **kwargs)

    async def unbind(self) -> bool:

        return await _db_impl.unbinding()

    Binder = _db_impl.Binder

    def set_url_key(self, key: Optional[str]) -> None:
        return _db_impl.DefaultURL.set_key(key)

    @property
    def is_bound(self) -> bool:
        return _db_impl.is_bound()

    async def create_tables(
        self, models: List[Type[Model]], **options: Any
    ) -> bool:

        for m in models:
            await m.create(**options)

        return True

    async def create_all(self, module: ModuleType, **options: Any) -> bool:

        if not isinstance(module, ModuleType):
            raise TypeError()

        return await self.create_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, Model)],
            **options
        )

    async def drop_tables(self, models: List[Type[Model]]) -> bool:

        for m in models:
            await m.drop()

        return True

    async def drop_all(self, module: ModuleType) -> bool:

        if not isinstance(module, ModuleType):
            raise TypeError()

        return await self.drop_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, Model)],
        )

    async def raw(
        self, sql: Union[str, _helper.Query], **kwargs: Any
    ) -> Union[
        None, _db_impl.FetchResult, util.tdict, Tuple[Any, ...], _db_impl.ExecResult
    ]:
        """ A coroutine that used to directly execute SQL statements """

        query = sql
        if not isinstance(query, _helper.Query):
            query = _helper.Query(query, kwargs.pop('params', None))
        return await _db_impl.execute(query, **kwargs)
