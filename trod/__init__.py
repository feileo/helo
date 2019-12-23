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
    db,
    _helper,
)

from .model import Model, JOINTYPE

__version__ = '0.0.3'
__all__ = (
    'types',
    'util',
    'err',
    'Model',
    'Trod',
    'JOINTYPE',
)


class Trod:
    """You can specify the default environment variable
    key name. But it can only work in the ``Trod.Binder``.
    """

    def __init__(self, url_key=None) -> None:

        if url_key is not None:
            self.set_url_key(url_key)

    async def bind(self, url: Optional[str] = None, **kwargs: Any) -> bool:
        """A coroutine that binding a database.
        Kwargs: see ``db.Pool``
        """

        return await db.binding(url, **kwargs)

    async def unbind(self) -> bool:
        """A coroutine that to unbind the database"""

        return await db.unbinding()

    # By default, the database url is looked up from the environment
    # variable and automatically to binding and unbinding.
    Binder = db.Binder

    def set_url_key(self, key: Optional[str]) -> None:
        """Set environment variable key name"""

        return db.DefaultURL.set_key(key)

    @property
    def isbound(self) -> bool:
        return db.isbound()

    @property
    def state(self) -> Optional[util.tdict]:
        return db.state()

    async def create_tables(
        self, models: List[Type[Model]], **options: Any
    ) -> bool:
        """Create table from Model list"""

        for m in models:
            await m.create(**options)
        return True

    async def create_all(self, module: ModuleType, **options: Any) -> bool:
        """Create all table from a model module"""

        if not isinstance(module, ModuleType):
            raise TypeError(f"{module!r} is not a module")

        return await self.create_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, Model) and m is not Model
             ],
            **options
        )

    async def drop_tables(self, models: List[Type[Model]]) -> bool:
        """Drop table from Model list"""

        for m in models:
            await m.drop()
        return True

    async def drop_all(self, module: ModuleType) -> bool:
        """Drop all table from a model module"""

        if not isinstance(module, ModuleType):
            raise TypeError(f"{module!r} is not a module")

        return await self.drop_tables(
            [m for _, m in vars(module).items()
             if isinstance(m, type) and issubclass(m, Model) and m is not Model
             ]
        )

    async def raw(
        self, sql: Union[str, _helper.Query], **kwargs: Any
    ) -> Union[
        None, util.tdict, Tuple[Any, ...], db.FetchResult, db.ExecResult
    ]:
        """A coroutine that used to directly execute SQL query statements"""

        query = sql
        if not isinstance(query, _helper.Query):
            query = _helper.Query(query, kwargs.pop('params', None))
        return await db.execute(query, **kwargs)
