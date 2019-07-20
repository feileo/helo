import urllib.parse as urlparse
from enum import Enum, unique

import aiomysql

from trod.errors import InvaildDBUrlError
from trod.extra.logger import Logger
from trod.utils import tdictformatter, singleton, asyncinit


@unique
class Schemes(Enum):
    """Currently supported Schemes """

    MYSQL = 1

    @classmethod
    def all(cls):
        """ all scheme name list """
        return [scheme.lower() for scheme in [cls.MYSQL.name]]


class DefaultConnConfig:
    """ Connection default config """

    MINSIZE = 1
    MAXSIZE = 15
    POOL_RECYCLE = -1
    ECHO = False
    TIMEOUT = 5

    _CONFIG = {
        'scheme': '',
        'user': '',
        'password': '',
        'host': 'localhost',
        'port': 3306,
        'db': '',
        'extra': {
            'unix_socket': None,
            'charset': 'utf8',
            'sql_mode': None,
            'use_unicode': None,
            'connect_timeout': TIMEOUT,
            'autocommit': True,
            'ssl': None,
        }
    }

    @property
    def config(self):
        """ Config struct template """

        return self._CONFIG


class Connector:
    """ Provide a factory method to create a database connection pool.

        Ex:
            connector = await Connector.create(url)

        Get a connection from the connection pool:
            connection = await connector.get()

        Close the pool:
            await connector.close()

        And etc.
    """

    @classmethod
    async def create(cls, url='',
                     minsize=DefaultConnConfig.MINSIZE,
                     maxsize=DefaultConnConfig.MAXSIZE,
                     timeout=DefaultConnConfig.TIMEOUT,
                     pool_recycle=DefaultConnConfig.POOL_RECYCLE,
                     echo=DefaultConnConfig.ECHO,
                     loop=None, **kwargs):
        """ A coroutine that create a connection pool object

        Args:
            url: db url.
            minsize: minimum sizes of the pool
            maxsize: maximum sizes of the pool.
            timeout: timeout of connection.
                     abandoning the connection from the pool after not getting the connection
            pool_recycle: connection reset period, default -1,
                          indicating that the connection will be reclaimed after a given time,
                          be careful not to exceed MySQL default time of 8 hours
            echo: executed log SQL queryes
            loop: is an optional event loop instance,
                  asyncio.get_event_loop() is used if loop is not specified.
            kwargs: and etc.

        returns : a `Connector` instance
        """

        if not url:
            return None
        result = ParseUrl(url, Schemes.all()).parse()
        extra = result.pop('extra')
        kwargs.update(extra)
        if timeout != DefaultConnConfig.TIMEOUT:
            kwargs.update({'connect_timeout': timeout})

        connector = await cls._create_pool(
            minsize=minsize, maxsize=maxsize,
            pool_recycle=pool_recycle,
            echo=echo, loop=loop, dbpath=result,
            extra=kwargs
        )
        return cls(connector=connector)

    @classmethod
    async def _create_pool(cls, minsize, maxsize, pool_recycle,
                           echo, loop, dbpath, extra):
        connector = None
        if dbpath.scheme and dbpath.scheme == Schemes.MYSQL.name.lower():
            connector = await _MySQLConnector(
                minsize, maxsize, pool_recycle,
                echo, loop, dbpath, extra
            )
        return connector

    def __init__(self, connector):
        if not connector:
            raise RuntimeError(
                'Invalid connector parameter type {}'.format(type(connector))
            )
        self.connector = connector

    def __repr__(self):
        return "<class '{} for {}:{}'>".format(
            self.__class__.__name__, self.db.db.host, self.db.db.port
        )

    __str__ = __repr__

    @property
    @tdictformatter()
    def status(self):
        """ connection pool status """

        return {
            'minsize': self.connector.minsize,
            'maxsize': self.connector.maxsize,
            'echo': self.connector.echo,
            'size': self.connector.size,
            'freesize': self.connector.freesize
        }

    @property
    @tdictformatter()
    def db(self):
        """ Db info """

        return self.connector.conn()

    def get(self):
        """ Get a connection """

        return self.connector.acquire()

    def release(self, connect):
        """ Reverts connection conn to free pool for future recycling. """

        return self.connector.release(connect)

    async def clear(self):
        """ A coroutine that closes all free connections in the pool.
            At next connection acquiring at least minsize of them will be recreated
        """
        await self.connector.clear()
        return True

    async def close(self):
        """ A coroutine that close pool. """

        await self.connector.close_pool()
        del self.connector
        return True


@singleton
@asyncinit
class _MySQLConnector:
    """
    Create a connection by aiomysql.create_pool().
    """
    is_depr = False

    async def __init__(self, minsize, maxsize, pool_recycle,
                       echo, loop, dbpath={}, extra={}):

        self._db = dbpath
        self._minsize = minsize
        self._maxsize = maxsize
        self._pool_recycle = pool_recycle
        self._echo = echo
        self._loop = loop
        self._extra = extra
        self._config = self._db.copy()
        self._config.update(self._extra)
        self._pool = await self._create_pool()

    async def _create_pool(self):
        db_conn_pool = None
        if self._db is None:
            return db_conn_pool
        self._config.pop('scheme')
        db_conn_pool = await aiomysql.create_pool(
            minsize=self._minsize,
            maxsize=self._maxsize,
            echo=self._echo,
            loop=self._loop,
            pool_recycle=self._pool_recycle,
            **self._config
        )
        Logger.info('Create database connection pool success')
        return db_conn_pool

    def conn(self):
        """ Db info """

        return {'db': self._db, 'extra': self._extra}

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
            including the used and idle connections
        """
        return self._pool.size

    @property
    def freesize(self):
        """ Pool free size """

        return self._pool.freesize

    def acquire(self):
        """ Get a connection """

        return self._pool.acquire()

    def release(self, connect):
        """ Release free connection """

        return self._pool.release(connect)

    async def clear(self):
        """ A coroutine that close all free connection  """

        await self._pool.clear()

    async def close_pool(self):
        """ A coroutine that close pool """

        self.is_depr = True
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
        del self
        Logger.info('Database connection pool closed')


class ParseUrl:
    """ database url parser """

    def __init__(self, url, schemes):
        self.url = url
        self.schemes = schemes

    def _register(self):
        """ Register database schemes in URLs """

        for scheme in self.schemes:
            urlparse.uses_netloc.append(scheme)

    def is_illegal_url(self):
        """ A bool of is illegal url """

        url = urlparse.urlparse(self.url)
        if all([url.scheme, url.netloc]):
            return True
        return False

    @tdictformatter()
    def parse(self):
        """ do parse database url """

        if not self.is_illegal_url():
            raise InvaildDBUrlError('Invalid dburl')
        self._register()

        config = DefaultConnConfig().config

        url = urlparse.urlparse(self.url)

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

        config.update({
            'scheme': url.scheme,
            'db': urlparse.unquote(path or ''),
            'user': urlparse.unquote(url.username or ''),
            'password': urlparse.unquote(url.password or ''),
            'host': hostname,
            'port': url.port or '',
        })

        options = {}
        for key, values in query.items():
            if url.scheme == 'mysql' and key == 'ssl-ca':
                options['ssl'] = {'ca': values[-1]}
                continue

            options[key] = values[-1]
        if options:
            config['extra'].update(options)

        return config
