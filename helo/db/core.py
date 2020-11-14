from contextvars import ContextVar
from typing import Optional, Any, List, Union, Callable, AsyncGenerator
from functools import wraps

from . import logging, interface
from .url import URL
from .result import ExeResult
from .. import err
from .._sql import Query
from ..util import import_object, adict


def __ensure__(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(self, *arg, **kwargs):
        if not self.is_connected:
            raise err.UnconnectedError()
        return func(self, *arg, **kwargs)
    return wrapper


logger = logging.create_logger()


class Database:

    _SUPPORTED_BACKENDS = {
        "mysql": "helo.db.backend.mysql.Backend",
    }

    def __init__(self, url_str: str, **options: Any) -> None:
        if not url_str:
            raise ValueError("Database URL cannot be an empty")

        self.url = URL(url_str)
        self.scheme = self.url.scheme
        self.options = options
        self.echo = options.pop("debug", False)

        backend_name = self._SUPPORTED_BACKENDS.get(self.url.scheme)
        if not backend_name:
            raise err.UnSupportedError(f"Helo not supported {self.url.scheme} now")
        if not self.url.db:
            raise ValueError("Must be specified the database name in url")

        self._backend = import_object(backend_name)(self.url, **options)
        self._connctx = ContextVar("connctx")  # type: ContextVar
        self._is_connected = False

    @property
    def is_connected(self):
        return self._is_connected

    async def connect(self) -> None:
        if self.is_connected:
            raise err.DuplicateConnect(
                f"Database already connected to {self._backend}"
            )

        await self._backend.connect()
        self._is_connected = True
        logger.info("Database is connected to %s", self._backend)

    @__ensure__
    async def close(self) -> None:
        await self._backend.close()
        self._is_connected = False
        logger.info("Database connected to %s is closed", self._backend)

    @__ensure__
    async def execute(
        self, query: Query, **kwargs: Any
    ) -> Union[None, adict, List[adict], ExeResult]:
        if not isinstance(query, Query):
            raise TypeError(
                "Invalid type for 'query', "
                f"expected 'Query', got {type(query)}"
            )

        if self.echo:
            logger.info(query)

        async with self.connection() as conn:
            if query.r:
                return await conn.fetch(
                    sql=query.sql,
                    params=query.params,
                    **kwargs
                )
            return await conn.execute(
                sql=query.sql,
                params=query.params,
                **kwargs
            )

    @__ensure__
    async def iterate(self, query: Query) -> AsyncGenerator[adict, None]:
        if not isinstance(query, Query):
            raise TypeError(
                "Invalid type for 'query', "
                f"expected 'Query', got {type(query)}"
            )

        if query.r:
            raise ValueError()

        if self.echo:
            logger.info(query)

        async with self.connection() as conn:
            async for row in conn.iterate(query.sql, query.params):
                yield row

    @__ensure__
    def connection(self) -> interface.Connection:
        try:
            return self._connctx.get()
        except LookupError:
            current = self._backend.connection()
            self._connctx.set(current)
            return current

    @__ensure__
    def transaction(self) -> interface.Transaction:
        return self.connection().transaction()
