import asyncio
import atexit
import threading
import warnings

from trod import errors
from trod.db_.connpool import Pool
from trod.db_.executer import fetch, execute
from trod.model_ import loader


__all__ = (
    'Connector',
    'Doer',
)


class Connector:

    __slots__ = ()

    _pool = None

    selected = None
    dblock = threading.Lock()

    @classmethod
    async def create(cls, *args, **kwargs):

        # TODO test
        if cls._pool is not None:
            raise RuntimeError()

        if args or kwargs.get('url'):
            cls._pool = await Pool.from_url(*args, **kwargs)
        else:
            cls._pool = await Pool(*args, **kwargs)

    @classmethod
    async def close(cls):

        if cls._pool:
            cls._pool = await cls._pool.close()
            return True

        warnings.warn('No binding db connector or closed', errors.ProgrammingWarning)
        return False

    @classmethod
    def select_db(cls, db=None):
        if db is None:  # pylint: disable=
            if not cls._pool:
                raise RuntimeError()
            return cls.get_connmeta().db
        elif not db or not isinstance(db, str):
            raise ValueError()

        with cls.dblock:
            cls.selected = db

        return cls.selected

    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            raise errors.NoConnectorError(
                "Connector has not been created, maybe you should call \
                `trod.bind()` before."
            )
        return cls._pool

    @classmethod
    def get_state(cls):
        if cls._pool:
            return cls._pool.state
        return None

    @classmethod
    def get_connmeta(cls):
        if cls._pool:
            return cls._pool.connmeta
        return None


atexit.register(
    lambda: asyncio.get_event_loop().run_until_complete(Connector.close())
)


async def text(sql, *args, **kwargs):  # TODO
    pool = Connector.get_pool()

    if 'SELECT' in sql or 'select' in sql:
        result = await fetch(pool, sql, *args, **kwargs)
    else:
        result = await execute(pool, sql, *args, **kwargs)
    return result


def current():
    c_db = None
    if Connector.selected:
        c_db = Connector.selected
    else:
        connmeta = Connector.get_connmeta()
        if connmeta:
            c_db = connmeta.db
    return c_db


class Doer:

    __slots__ = ('_model', '_sql', '_args')
    __fetch__ = False

    def __init__(self, model, sql=None, args=None):
        self._model = model
        self._sql = sql or []
        self._args = args

    def __str__(self):
        args = f' % {self._args}' if self._args else ''
        return f"Doer by {Connector.get_pool()}\n For SQL({self.sql}{args})"

    __repr__ = __str__

    @property
    def sql(self):
        if isinstance(self._sql, (list, tuple)):
            if self._sql:
                self._sql.append(';')
                self._sql = ' '.join(self._sql)
        return self._sql

    async def do(self):

        pool = Connector.get_pool()
        db = Connector.selected

        if self.__fetch__:
            fetch_results = await fetch(
                pool, self.sql, args=self._args, db=db
            )
            tdicts = getattr(self, 'tdicts', True)
            return loader.load(self._model, fetch_results, tdicts)

        exec_results = await execute(
            pool, self.sql, values=self._args,
            is_batch=getattr(self, '_batch', False), db=db
        )
        return loader.ExecResults(*exec_results)
