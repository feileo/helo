"""
    trod.db._impl
    ~~~~~~~~~~~~~

    Implements the connection pool and executer of db module.
"""
from __future__ import annotations

import asyncio
import os
import sys
import urllib.parse as urlparse
from functools import wraps
from inspect import iscoroutinefunction
from typing import Optional, Any, Union, Callable, Dict

import aiomysql

from .. import util, err
from .._helper import Query


SUPPORTED_SCHEMES = ('mysql',)
URL_KEY = 'TROD_DB_URL'


def __ensure__(bound: bool) -> Callable:
    """A decorator to ensure that the executor has been activated or dead."""

    def decorator(func):

        def checker():
            if Executer.active():
                if not bound:
                    cm = Executer.pool.connmeta
                    raise err.DuplicateBinding(host=cm.host, port=cm.port)
            elif bound:
                raise err.UnboundError()

        if iscoroutinefunction(func):
            @wraps(func)
            async def async_wraper(*args, **kwargs):
                checker()
                return await func(*args, **kwargs)

            return async_wraper

        @wraps(func)
        def wrapper(*args, **kwargs):
            checker()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def get_db_url() -> Optional[str]:
    return os.environ.get(URL_KEY)


class BindContext:

    def __init__(self, **bindings: Any) -> None:
        self.initcmd = bindings.pop('init', None)
        self.clearcmd = bindings.pop('clear', None)
        self.bindings = bindings

    async def __aenter__(self) -> None:
        await binding(get_db_url(), **self.bindings)

        if self.initcmd:
            if callable(self.initcmd) and iscoroutinefunction(self.initcmd):
                await self.initcmd()
            else:
                raise ValueError('init cmd must be coroutine function')

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.clearcmd:
            if callable(self.clearcmd) and iscoroutinefunction(self.clearcmd):
                await self.clearcmd()
            else:
                raise ValueError('clear cmd must be coroutine function')

        await unbinding()


@__ensure__(False)
async def binding(
    url: Optional[str] = None, **kwargs: Any
) -> bool:
    """A coroutine that binding a database(create a connection pool).

    The pool is a singleton, repeated create will cause errors.
    Returns true after successful create

    :param url: db url
    more parameters, see ``Pool` and ``Pool.from_url``
    """

    if url:
        pool = await Pool.from_url(url, **kwargs)
    else:
        pool = await Pool(**kwargs)  # type: ignore

    Executer.activate(pool)

    return True


@__ensure__(True)
async def execute(
    query: Query, **kwargs: Any
) -> Union[None, FetchResult, util.tdict, tuple, ExecResult]:
    """A coroutine that execute sql and return the results of its execution

    :param sql ``trod._helper.Query`` : sql query object
    """
    if not query:
        raise ValueError("No query to execute")

    return await Executer.do(query, **kwargs)


@__ensure__(True)
async def select_db(db: str) -> None:
    """Set current db"""

    async with Executer.pool.acquire() as conn:  # type: ignore
        conn._db = db  # pylint: disable=protected-access
        await conn.select_db(db)


@__ensure__(True)
async def unbinding() -> bool:
    """A coroutine that unbinding a database(close the connection pool)."""

    return await Executer.death()


def is_bound() -> bool:
    return bool(Executer.pool)


def get_state() -> Optional[util.tdict]:
    """Return the current state of the connection pool"""

    return Executer.poolstate()


@util.asyncinit
class Pool:
    """Create a MySQL connection pool based on `aiomysql.create_pool`.

    :param int minsize: Minimum sizes of the pool
    :param int maxsize: Maximum sizes of the pool
    :param bool echo: Executed log SQL queryes
    :param int pool_recycle: Connection reset period, default -1,
        indicating that the connectionion will be reclaimed after a given time,
        be careful not to exceed MySQL default time of 8 hours
    :param loop: Is an optional event loop instance,
        asyncio.get_event_loop() is used if loop is not specified.
    :param conn_kwargs: See `_CONN_KWARGS`.
    """
    _CONN_KWARGS = util.tdict(
        host="localhost",        # Host where the database server is located
        user=None,               # Username to log in as
        password="",             # Password to use
        db=None,                 # Database to use, None to not use a particular one
        port=3306,               # MySQL port to use
        charset='utf8',              # Charset you want to use
        unix_socket=None,        # You can use a unix socket rather than TCP/IP
        read_default_file=None,  # Specifies my.cnf file to read these parameters
        use_unicode=None,        # Whether or not to default to unicode strings
        init_command=None,       # Initial SQL statement to run when connectionion is established
        connect_timeout=20,      # Timeout before throwing an exception when connectioning
        autocommit=False,        # Autocommit mode. None means use server default
        local_infile=False,      # bool to enable the use of LOAD DATA LOCAL cmd
        ssl=None,                # Optional SSL Context to force SSL
        auth_plugin='',          # String to manually specify the authentication plugin to use
        program_name='',         # Program name string to provide
        server_public_key=None,  # SHA256 authentication plugin public key value
    )
    _POOL_KWARGS = ('minsize', 'maxsize', 'echo', 'pool_recycle', 'loop')

    __slots__ = ('_pool', '_connmeta')

    async def __init__(  # type: ignore
            self,
            minsize: int = 1,
            maxsize: int = 15,
            echo: bool = False,
            pool_recycle: int = -1,
            loop: Optional[asyncio.AbstractEventLoop] = None,
            **conn_kwargs: Any
    ) -> None:

        conn_kwargs = self._check_conn_kwargs(conn_kwargs)
        self._pool = await aiomysql.create_pool(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **conn_kwargs
        )
        self._connmeta = conn_kwargs

    @classmethod
    async def from_url(cls, url: str, **kwargs: Any) -> Pool:
        """Provide a factory method `from_url` to create a connection pool.

        :params url: mysql url to connect
        :params kwargs: see ``__init__`` for information

        :returns: ``Pool`` instance
        """
        if not url:
            raise ValueError('Database url cannot be empty')

        params = UrlParser(url).parse()
        params.update(kwargs)

        return await cls(**params)  # type: ignore

    def __repr__(self) -> str:
        return "<Pool[{1}:{2}] for {3}:{4}/{5}>".format(
            self.minsize, self.maxsize,
            self.connmeta["host"], self.connmeta["port"], self.connmeta["db"]
        )

    __str__ = __repr__

    def _check_conn_kwargs(self, conn_kwargs: Any) -> dict:

        ret_kwargs = {}
        for arg in self._CONN_KWARGS:
            ret_kwargs[arg] = conn_kwargs.pop(arg, None) or self._CONN_KWARGS[arg]
        for exarg in conn_kwargs:
            raise TypeError(
                f'{self.__class__.__name__} got an'
                f'unexpected keyword argument "{exarg}"'
            )
        return ret_kwargs

    @property
    def echo(self) -> bool:
        """Pool echo mode"""

        return self._pool.echo

    @property
    def minsize(self) -> int:
        """Pool minsize"""

        return self._pool.minsize

    @property
    def maxsize(self) -> int:
        """Pool pool maxsize"""

        return self._pool.maxsize

    @property
    def size(self) -> int:
        """The current size of the pool,
           including the used and idle connectionions
        """
        return self._pool.size

    @property
    def freesize(self) -> int:
        """Pool free size"""

        return self._pool.freesize

    @property
    def connmeta(self) -> util.tdict:
        """Pool connection meta"""

        return util.formattdict(self._connmeta)  # type: ignore

    def acquire(self) -> aiomysql.Connection:
        """Acquice a connectionion from the pool"""

        return self._pool.acquire()

    def release(self, connection: aiomysql.Connection) -> Any:
        """Reverts connectionion conn to free pool for future recycling"""

        return self._pool.release(connection)

    async def clear(self) -> None:
        """A coroutine that closes all free connectionions in the pool.
           At next connectionion acquiring at least minsize of them will be recreated
        """

        await self._pool.clear()

    async def close(self) -> None:
        """A coroutine that close pool.

        Mark all pool connectionions to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connectionions.
        """

        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()

    async def terminate(self) -> None:
        """A coroutine that terminate pool.

        Close pool with instantly closing all acquired connectionions also.
        """

        await self._pool.terminate()


class TdictCursor(aiomysql.DictCursor):

    dict_type = util.tdict


class Executer:
    """Executor of MySQL query."""

    __slots__ = ()

    pool = None  # type: Optional[Pool]

    @classmethod
    def activate(cls, connpool: Pool) -> None:
        cls.pool = connpool

    @classmethod
    async def death(cls) -> bool:
        if cls.pool is None:
            return False

        await cls.pool.close()
        cls.pool = None
        return True

    @classmethod
    def active(cls) -> bool:
        return bool(cls.pool)

    @classmethod
    async def do(
        cls, query: Query, **kwargs: Any
    ) -> Union[None, FetchResult, util.tdict, tuple, ExecResult]:
        if query.r:
            return await cls._fetch(
                query.sql, params=query.params, **kwargs,
            )
        return await cls._execute(
            query.sql, params=query.params, **kwargs
        )

    @classmethod
    def poolstate(cls) -> Optional[util.tdict]:
        if cls.pool is None:
            return None
        return util.tdict(
            minsize=cls.pool.minsize,
            maxsize=cls.pool.maxsize,
            size=cls.pool.size,
            freesize=cls.pool.freesize,
        )

    @classmethod
    async def _fetch(
            cls, sql: str,
            params: Optional[Union[tuple, list]] = None,
            rows: Optional[int] = None,
            db: Optional[str] = None,
            tdict: bool = True
    ) -> Union[None, FetchResult, util.tdict, tuple]:

        async with cls.pool.acquire() as connection:  # type: ignore

            if db:
                await connection.select_db(db)

            cursorclasses = [TdictCursor] if tdict is True else []
            async with connection.cursor(*cursorclasses) as cur:

                try:
                    await cur.execute(sql, params or ())
                    if not rows:
                        result = await cur.fetchall()
                    elif rows and rows == 1:
                        result = await cur.fetchone()
                    else:
                        result = await cur.fetchmany(rows)

                    if rows != 1 and not isinstance(result, list):
                        result = list(result)
                except Exception:
                    exc_type, exc_value, _traceback = sys.exc_info()
                    error = exc_type(exc_value)  # type: ignore
                    raise error

        return FetchResult(result) if isinstance(result, list) else result

    @classmethod
    async def _execute(
            cls, sql: str,
            params: Optional[Union[tuple, list]] = None,
            many: bool = False,
            db: Optional[str] = None
    ) -> ExecResult:

        async with cls.pool.acquire() as connection:  # type: ignore

            if db:
                await connection.select_db(db)

            autocommit = connection.get_autocommit()
            if not autocommit:
                await connection.begin()
            try:
                async with connection.cursor() as cur:
                    if many is True:
                        await cur.executemany(sql, params or ())
                    else:
                        await cur.execute(sql, params or ())
                    affected, last_id = cur.rowcount, cur.lastrowid
                if not autocommit:
                    await connection.commit()
            except Exception:
                if not autocommit:
                    await connection.rollback()
                exc_type, exc_value, _traceback = sys.exc_info()
                error = exc_type(exc_value)  # type: ignore
                raise error

        return ExecResult(affected, last_id)


class FetchResult(list):

    @property
    def count(self):
        return len(self)


class ExecResult:
    def __init__(self, affected, last_id):
        self.affected = affected
        self.last_id = last_id

    def __repr__(self) -> str:
        return "ExecResult(affected: {}, last_id: {})".format(
            self.affected, self.last_id
        )

    def __str__(self) -> str:
        return "({}, {})".format(self.affected, self.last_id)


class UrlParser:
    """Database url parser"""

    __slots__ = ('url', )

    def __init__(self, url: str) -> None:
        self.url = url

    @util.tdictformatter
    def parse(self) -> Dict[str, Any]:
        """ do parse database url """

        if not self._is_illegal_url():
            raise ValueError(f'Invalid db url {self.url}')

        self._register()
        url = urlparse.urlparse(self.url)

        if url.scheme not in SUPPORTED_SCHEMES:
            raise err.UnsupportedError(
                f'Unsupported scheme {url.scheme}'
            )

        path, query = url.path[1:], url.query
        if '?' in path and not url.query:
            path, query = path.split('?', 2)

        hostname = url.hostname or ''
        if '%2f' in hostname.lower():
            hostname = url.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')

        parsed = {
            'db': urlparse.unquote(path or ''),
            'user': urlparse.unquote(url.username or ''),
            'password': urlparse.unquote(url.password or ''),
            'host': hostname,
            'port': url.port or '',
        }

        options = {}  # type: Dict[str, Any]
        for key, values in urlparse.parse_qs(query).items():
            if url.scheme == 'mysql' and key == 'ssl-ca':
                options['ssl'] = {'ca': values[-1]}
                continue

            options[key] = values[-1]

        if options:
            parsed.update(options)

        return parsed

    def _register(self) -> None:
        """Register database schemes in URLs"""

        urlparse.uses_netloc.extend(SUPPORTED_SCHEMES)

    def _is_illegal_url(self) -> bool:
        """A bool of is illegal url"""

        url = urlparse.urlparse(self.url)
        if all([url.scheme, url.netloc]):
            return True
        return False
