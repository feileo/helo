"""
    helo.g
    ~~~~~~
"""

from types import ModuleType
from typing import Any, Optional, Type, Union, List, Tuple

from . import db, model, util, _builder


@util.singleton
class G:
    """
    You can specify the default environment variable key name.
    But it can only work in the ``G.Binder``.
    """

    def __init__(
        self,
        env_key: str = '',
        model_class: Optional[Type[model.Model]] = None,
    ) -> None:
        if env_key:
            self.set_env_key(env_key)
        self._mc = model_class or model.Model  # type: Type[model.Model]

    @property
    def Model(self) -> Type[model.Model]:  # pylint: disable=invalid-name
        return self._mc

    @property
    def isbound(self) -> bool:
        return db.isbound()

    @property
    def state(self) -> Optional[util.adict]:
        return db.state()

    async def bind(self, url: Optional[str] = None, **kwargs: Any) -> bool:
        """
        A coroutine that binding a database.

        Kwargs: see ``db.Pool``
        """

        return await db.binding(url, **kwargs)

    async def unbind(self) -> bool:
        """A coroutine that to unbind the database"""

        return await db.unbinding()

    # By default, the database url is looked up from the environment
    # variable and automatically to binding and unbinding.
    Binder = db.Binder

    def set_env_key(self, key: str) -> None:
        """Set environment variable key name"""

        return db.EnvKey.set(key)

    async def create_tables(
        self, models: List[Type[model.Model]], **options: Any
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
             if isinstance(m, type) and issubclass(m, self.Model) and m is not self.Model
             ],
            **options
        )

    async def drop_tables(self, models: List[Type[model.Model]]) -> bool:
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
             if isinstance(m, type) and issubclass(m, self.Model) and m is not self.Model
             ]
        )

    async def raw(
        self, sql: Union[str, _builder.Query], **kwargs: Any
    ) -> Union[
        None, util.adict, Tuple[Any, ...], db.FetchResult, db.ExecResult
    ]:
        """A coroutine that used to directly execute SQL query statements"""

        query = sql
        if not isinstance(query, _builder.Query):
            query = _builder.Query(query, kwargs.pop('params', None))
        return await db.execute(query, **kwargs)
