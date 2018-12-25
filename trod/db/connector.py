import urllib.parse as urlparse
from enum import Enum, unique

import aiomysql
from asyncinit import asyncinit

from trod.extra.logger import Logger
from trod.utils import dict_formatter, singleton


@unique
class Schemes(Enum):
    """ Schemes """

    MYSQL = 1

    @classmethod
    def all(cls):
        """ all scheme name list """
        return [scheme.lower() for scheme in [cls.MYSQL.name]]


class DefaultConnConfig:
    """ connection default config """

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
            'autocommit': False,
            'ssl': None,
        }
    }

    @property
    def config(self):
        """ config struct template """
        return self._CONFIG


class Connector:
    """ 数据库连接池:
        2. connector = await Connector.from_url(url)
    """

    @classmethod
    async def create(cls, url='',
                     minsize=DefaultConnConfig.MINSIZE,
                     maxsize=DefaultConnConfig.MAXSIZE,
                     timeout=DefaultConnConfig.TIMEOUT,
                     pool_recycle=DefaultConnConfig.POOL_RECYCLE,
                     echo=DefaultConnConfig.ECHO,
                     loop=None, **kwargs):
        """ 创建连接池 """
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
            raise RuntimeError(f'Connector pool is {type(connector)}')
        self.connector = connector

    def __repr__(self):
        return "<class '{} for {}:{}'>".format(
            self.__class__.__name__, self.me.host, self.me.port
        )

    def __str__(self):
        return "<class '{} for {}:{}'>".format(
            self.__class__.__name__, self.me.host, self.me.port
        )

    @property
    @dict_formatter
    def status(self):
        """ 连接池状态 """
        return {
            'minsize': self.connector.minsize,
            'maxsize': self.connector.maxsize,
            'echo': self.connector.echo,
            'size': self.connector.size,
            'freesize': self.connector.freesize
        }

    @property
    @dict_formatter
    def me(self):
        """ 连接池元信息 """
        return self.connector.conn()

    def get(self):
        """ 从池中获取连接 """
        return self.connector.acquire()

    def release(self, connect):
        """ 释放空闲连接 """
        return self.connector.release(connect)

    async def clear(self):
        """ 关闭空闲连接 """
        await self.connector.clear()

    async def close(self):
        """ 关闭连接池 """
        await self.connector.close_pool()


@singleton
@asyncinit
class _MySQLConnector:
    """
    使用 aiomysql 创建一个连接池。
    """

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
        """ 连接相关元信息 """
        return {'db': self._db, 'extra': self._extra}

    @property
    def echo(self):
        """返回 echo 模式状态"""
        return self._pool.echo

    @property
    def minsize(self):
        """ pool 的最小大小"""
        return self._pool.minsize

    @property
    def maxsize(self):
        """ pool 的最大大小"""
        return self._pool.maxsize

    @property
    def size(self):
        """ pool 的当前大小，包括使用的和空闲的连接 """
        return self._pool.size

    @property
    def freesize(self):
        """pool 的空闲大小 """
        return self._pool.freesize

    def acquire(self):
        """ 从池中获取空闲连接 """
        return self._pool.acquire()

    def release(self, connect):
        """ 释放空闲连接 """
        return self._pool.release(connect)

    async def clear(self):
        """ 关闭空闲连接 """
        await self._pool.clear()

    async def close_pool(self):
        """ 关闭连接池 """
        Logger.info('Database connection pool closed')
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()


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
        """ 是否合法 DB URL """
        url = urlparse.urlparse(self.url)
        if all([url.scheme, url.netloc]):
            return True
        return False

    @dict_formatter
    def parse(self):
        """ 解析 DATABASE URL """

        if not self.is_illegal_url():
            raise Exception('illegal dburl')
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
