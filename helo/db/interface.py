from __future__ import annotations

from typing import Tuple, List, Any, Callable, Optional, Union

from .result import ExeResult
from ..util import adict


class Backend:

    async def connect(self) -> None:
        raise NotImplementedError()

    async def close(self) -> None:
        raise NotImplementedError()

    def connection(self) -> Connection:
        raise NotImplementedError()


class Connection:

    async def __aenter__(self) -> Connection:
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        raise NotImplementedError()

    async def acquire(self) -> None:
        raise NotImplementedError()

    async def release(self) -> None:
        raise NotImplementedError()

    async def fetch(
        self,
        sql: str,
        params: Optional[Tuple[Any, ...]] = None,
        rows: Optional[int] = None,
    ) -> Union[None, adict, List[adict]]:
        raise NotImplementedError()

    async def execute(
        self,
        sql: str,
        params: Optional[Tuple[Any, ...]] = None,
        many: bool = False,
    ) -> ExeResult:
        raise NotImplementedError()

    def transaction(self) -> Transaction:
        raise NotImplementedError()


class Transaction:

    async def __aenter__(self) -> Transaction:
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        raise NotImplementedError()

    def __call__(self, func: Callable) -> Callable:
        raise NotImplementedError()

    async def begin(self) -> None:
        raise NotImplementedError()

    async def commit(self) -> None:
        raise NotImplementedError()

    async def rollback(self) -> None:
        raise NotImplementedError()
