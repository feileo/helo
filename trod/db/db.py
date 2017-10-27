import sys
import urllib.parse as urlparse
from collections import namedtuple
from functools import wraps

import aiomysql

from .. import utils, errors


SCHEMES = ('mysql',)
ARG = namedtuple('Arg', ['dft', 'help'])


@utils.singleton
@utils.asyncinit
class Pool:
    """ Create a MySQL connectionion pool based on `aiomysql.create_pool`.

        :param int minsize: Minimum sizes of the pool
        :param int maxsize: Maximum sizes of the pool
        :param bool echo: Executed log SQL queryes
        :param int pool_recycle: Connection reset period, default -1,
            indicating that the connectionion will be reclaimed after a given time,
            be careful not to exceed MySQL default time of 8 hours
        :param loop: Is an optional event loop instance,
            asyncio.get_event_loop() is used if loop is not specified.
        :param conn_kwargs: See `_CONN_KWARGS`.

        :Returns : `Pool` instance
    """
    _CONN_KWARGS = utils.Tdict(
        host=ARG(dft="localhost", help='Host where the database server is located'),
        user=ARG(dft=None, help='Username to log in as'),
        password=ARG(dft="", help='Password to use'),
        db=ARG(dft=None, help='Database to use, None to not use a particular one'),
        port=ARG(dft=3306, help='MySQL port to use'),
        charset=ARG(dft='', help='Charset you want to use'),
        unix_socket=ARG(dft=None, help='You can use a unix socket rather than TCP/IP'),
        read_default_file=ARG(dft=None, help='Specifies my.cnf file to read these parameters'),
        use_unicode=ARG(dft=None, help='Whether or not to default to unicode strings'),
        init_command=ARG(dft=None, help='Initial SQL statement to run when connectionion is established'),
        connect_timeout=ARG(dft=20, help='Timeout before throwing an exception when connectioning'),
        autocommit=ARG(dft=False, help='Autocommit mode. None means use server default'),
        echo=ARG(dft=False, help='Echo mode'),
        loop=ARG(dft=None, help='Asyncio loop'),
        local_infile=ARG(dft=False, help='bool to enable the use of LOAD DATA LOCAL cmd'),
        ssl=ARG(dft=None, help='Optional SSL Context to force SSL'),
        auth_plugin=ARG(dft='', help='String to manually specify the authentication plugin to use'),
        program_name=ARG(dft='', help='Program name string to provide'),
        server_public_key=ARG(dft=None, help='SHA256 authentication plugin public key value'),
    )
    _POOL_KWARGS = ('minsize', 'maxsize', 'echo', 'pool_recycle', 'loop')

    __slots__ = ('_pool', '_connmeta')

    class TdictCursor(aiomysql.DictCursor):

        dict_type = utils.Tdict

    async def __init__(self, minsize=1, maxsize=15, echo=False,
                       pool_recycle=-1, loop=None, **conn_kwargs):

        conn_kwargs = utils.formattdict(self._check_conn_kwargs(conn_kwargs))
        conn_kwargs['cursorclass'] = self.TdictCursor
        self._pool = await aiomysql.create_pool(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **conn_kwargs
        )
        self._connmeta = conn_kwargs
        super().__init__(**conn_kwargs)

    @classmethod
    async def from_url(cls, url, minsize=1, maxsize=15, echo=False,
                       pool_recycle=-1, loop=None, **conn_kwargs):
        """ Provide a factory method `from_url` to create a connectionion pool.

        :params see `__init__` for information

        :Returns : `Pool` instance
        """
        if not url:
            raise ValueError('Db url cannot be empty')

        db_meta = UrlParser(url).parse()
        db_meta.update(conn_kwargs)
        for arg in cls._POOL_KWARGS:
            db_meta.pop(arg, None)

        return await cls(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **db_meta
        )

    def __repr__(self):
        return "<Pool[{1}:{2}] for {3}:{4}/{5}>".format(
            self.minsize, self.maxsize,
            self.connmeta.host, self.connmeta.port, self.connmeta.db
        )

    __str__ = __repr__

    def _check_conn_kwargs(self, conn_kwargs):

        ret_kwargs = {}
        for arg in self._CONN_KWARGS:
            ret_kwargs[arg] = conn_kwargs.pop(arg, None) or self._CONN_KWARGS[arg].dft
        for exarg in conn_kwargs:
            raise TypeError(
                f'{self.__class__.__name__} got an unexpected keyword argument {exarg}'
            )
        return ret_kwargs

    @property
    def echo(self):
        """ echo mode"""

        return self._pool.echo

    @property
    def minsize(self):
        """ pool minsize """

        return self._pool.minsize

    @property
    def maxsize(self):
        """ pool maxsize"""

        return self._pool.maxsize

    @property
    def size(self):
        """ The current size of the pool,
            including the used and idle connectionions
        """
        return self._pool.size

    @property
    def freesize(self):
        """ Pool free size """

        return self._pool.freesize

    @property
    def connmeta(self):
        return self._connmeta

    def acquire(self):
        """ Acquice a connectionion """

        return self._pool.acquire()

    def release(self, connection):
        """ Reverts connectionion conn to free pool for future recycling. """

        return self._pool.release(connection)

    async def clear(self):
        """ A coroutine that closes all free connectionions in the pool.
            At next connectionion acquiring at least minsize of them will be recreated
        """

        await self._pool.clear()

    async def close(self):
        """ A coroutine that close pool.

        Mark all pool connectionions to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connectionions.
        """

        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()

    async def terminate(self):
        """ A coroutine that terminate pool.

        Close pool with instantly closing all acquired connectionions also.
        """

        await self._pool.terminate()


class Executer:

    __slots__ = ()

    pool = None

    @classmethod
    def activate(cls, connpool):
        cls.pool = connpool

        import asyncio
        import atexit

        atexit.register(
            lambda: asyncio.get_event_loop().run_until_complete(cls.death())
        )

    @classmethod
    async def death(cls):
        if cls.pool is None:
            return False

        await cls.pool.close()
        cls.pool = None
        return True

    @classmethod
    def active(cls):
        return bool(cls.pool)

    @classmethod
    async def fetch(cls, sql, params=None, rows=None, db=None):

        if params:
            params = utils.tuple_formatter(params)

        async with cls.pool.acquire() as connection:

            if db:
                await connection.select_db(db)

            async with connection.cursor() as cur:
                try:
                    await cur.execute(sql.strip(), params or ())
                    if rows and rows == 1:
                        result = await cur.fetchone()
                    elif rows:
                        result = await cur.fetchmany(rows)
                    else:
                        result = await cur.fetchall()
                except Exception:
                    exc_type, exc_value, _ = sys.exc_info()
                    error = exc_type(exc_value)
                    raise error
        return result

    @classmethod
    async def execute(cls, sql, params=None, many=False, db=None):
        sql = sql.strip()
        if params:
            params = utils.tuple_formatter(params)

        async with cls.pool.acquire() as connection:
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
                exc_type, exc_value, _ = sys.exc_info()
                error = exc_type(exc_value)
                raise error
        return affected, last_id


def __ensure__(needbind):

    def decorator(func):

        @wraps(func)
        def checker(func, *args, **kwargs):
            if Executer.active():
                if not needbind:
                    cm = Executer.pool.connmeta
                    raise errors.DuplicateBinding(host=cm.host, port=cm.port)
            else:
                if needbind:
                    raise errors.UnboundError()
            return func(*args, **kwargs)
        return checker
    return decorator


class UrlParser:
    """ Database url parser """

    __slots__ = ('url', )

    def __init__(self, url):
        self.url = url

    @utils.tdictformatter()
    def parse(self):
        """ do parse database url """

        if not self._is_illegal_url():
            raise ValueError(f'Invalid db url {self.url}')
        self._register()

        url = urlparse.urlparse(self.url)

        if url.scheme not in SCHEMES:
            raise ValueError(f'Unsupported scheme {url.scheme}')

        path, query = url.path[1:], url.query
        if '?' in path and not url.query:
            path, query = path.split('?', 2)

        query = urlparse.parse_qs(query)

        hostname = url.hostname or ''
        if '%2f' in hostname.lower():
            hostname = url.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')

        db_meta = {
            'db': urlparse.unquote(path or ''),
            'user': urlparse.unquote(url.username or ''),
            'password': urlparse.unquote(url.password or ''),
            'host': hostname,
            'port': url.port or '',
        }

        options = {}
        for key, values in query.items():
            if url.scheme == 'mysql' and key == 'ssl-ca':
                options['ssl'] = {'ca': values[-1]}
                continue

            options[key] = values[-1]

        if options:
            db_meta.update(options)

        return db_meta

    def _register(self):
        """ Register database schemes in URLs """

        urlparse.uses_netloc.extend(SCHEMES)

    def _is_illegal_url(self):
        """ A bool of is illegal url """

        url = urlparse.urlparse(self.url)
        if all([url.scheme, url.netloc]):
            return True
        return False
