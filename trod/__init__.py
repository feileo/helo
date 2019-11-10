"""
    trod
    ~~~~
"""
from types import ModuleType
from typing import Any, Optional, Type, Union, List

from . import (
    db,
    types,
    model,
    err,
    util,
    _helper,
)

__version__ = '0.0.15'
__all__ = (
    'db',
    'types',
    'util',
    'err',
    'Trod',
)


class Trod:

    Model = model.Model

    async def bind(self, url: Optional[str] = None, **kwargs: Any) -> bool:

        return await db.binding(url, **kwargs)

    async def unbind(self) -> bool:

        return await db.unbinding()

    async def create_tables(
        self, models: List[Type[model.Model]], **options: Any
    ) -> bool:

        for m in models:
            await m.create(**options)

        return True

    async def create_all(self, module: ModuleType, **options: Any) -> bool:

        if not isinstance(module, ModuleType):
            raise TypeError()

        return await self.create_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, self.Model)],
            **options
        )

    async def drop_tables(self, models: List[Type[model.Model]]) -> bool:

        for m in models:
            await m.drop()

        return True

    async def drop_all(self, module: ModuleType) -> bool:

        if not isinstance(module, ModuleType):
            raise TypeError()

        return await self.drop_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, self.Model)],
        )

    async def text(self, sql: Union[str, _helper.Query], **kwargs: Any) -> Any:
        """ A coroutine that used to directly execute SQL statements """
        query = sql
        if not isinstance(query, _helper.Query):
            query = _helper.Query(query, kwargs.pop('params', None))
        return await db.execute(query, **kwargs)
