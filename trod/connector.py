# -*- coding=utf8 -*-
"""
# Description:
"""

import asyncio
import urllib.parse as urlparse

import aiomysql
from asyncinit import asyncinit

from trod.const import Schemes
from trod.utils import dict_formatter, singleton

from component import EventLogger


@asyncinit
class Connector:
    """ 数据库连接池，可用过以下两种方式创建:
        1. connector = await Connector(...)
        2. connector = await Connector.from_url(url)
    """

    @classmethod
    async def from_url(cls, url='', loop=None, echo=False):
        """ 从 URL 走配置 """
        if not url:
            return None
        parser = ParseUrl(url, Schemes.all())
        config = parser.parse()
        connector = await cls._create_pool(config=config, loop=loop, echo=echo)
        return await cls(connector=connector)

    async def __init__(self, scheme=None, user=None, password=None,
                       host=None, port=None, database=None, query=None,
                       loop=None, echo=False, connector=None):
        if not connector:
            config = self._init_config(
                scheme, user, password, host, port, database, query
            )
            connector = await self._create_pool(config=config, loop=loop, echo=echo)
        self.connector = connector

    def __repr__(self):
        return "<class '{} for {}:{}'>".format(
            self.__class__.__name__, self.meta.host, self.meta.port
        )

    def __str__(self):
        return "<class '{} for {}:{}'>".format(
            self.__class__.__name__, self.meta.host, self.meta.port
        )

    @dict_formatter
    def _init_config(self, scheme, user, password, host, port, database,
                     query):
        if scheme is None:
            raise Exception('missing scheme error')
        if query is not None:
            assert isinstance(query, dict), 'illegal query type'
        config = DefaultConfig().config
        config.update({
            'scheme': scheme,
            'user': user,
            'password': password,
            'host': host if host else 'localhost',
            'port': int(port) if port is not None else 3306,
            'db': database,
            'query': query
        })
        return config

    @classmethod
    async def _create_pool(cls, config, loop, echo):
        connector = None
        if config.scheme and config.scheme == Schemes.MYSQL.name.lower():
            connector = await MySQLConnector(
                loop=loop, echo=echo, meta=config
            )
        return connector

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
    def meta(self):
        """ 连接池元信息 """
        return self.connector.meta()

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
class MySQLConnector:
    """
    创建一个db连接池。
    使用连接池的好处是不必频繁地打开和关闭数据库连接，能复用就尽量复用。
    """

    async def __init__(self, loop=None, echo=False, meta=None):
        self._meta = meta
        self._echo = echo
        self._loop = loop
        self._pool = await self._create_pool()

    async def _create_pool(self):
        db_conn_pool = None
        if self._meta is None:
            return db_conn_pool
        db_conn_pool = await aiomysql.create_pool(
            loop=self._loop,
            echo=self._echo,
            host=self._meta.host,
            port=self._meta.port,
            user=self._meta.user,
            password=self._meta.password,
            db=self._meta.db,
            charset=self._meta.query.get('charset', DefaultConfig.CHARSET),
            maxsize=self._meta.query.get('maxsize', DefaultConfig.MAXSIZE),
            minsize=self._meta.query.get('minsize', DefaultConfig.MINSIZE),
            autocommit=self._meta.query.get('autocommit', DefaultConfig.AUTOCOMMIT)
        )
        return db_conn_pool
        # EventLogger.info('create database connection pool', task='building')

    def meta(self):
        """ 连接相关元信息 """
        return self._meta

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
        # EventLogger.info('close database connection pool')
        return self._pool.release(connect)

    async def clear(self):
        """ 关闭空闲连接 """
        # EventLogger.info('close database connection pool')
        await self._pool.clear()

    async def close_pool(self):
        """ 关闭连接池 """
        EventLogger.info('close database connection pool')
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()


@singleton
class DefaultConfig:
    """ connection default config """

    CHARSET = 'utf-8'
    MINSIZE = 1
    MAXSIZE = 15
    AUTOCOMMIT = True

    _CONFIG = {
        'scheme': '',
        'user': '',
        'password': '',
        'host': 'localhost',
        'port': 3306,
        'db': '',
        'query': {}
    }

    @property
    @dict_formatter
    def config(self):
        """ config struct template """
        return self._CONFIG


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
            raise Exception('illegal url')
        self._register()

        config = DefaultConfig().config

        url = urlparse.urlparse(self.url)

        # Split query strings from path.
        path, query = url.path[1:], url.query
        if '?' in path and not url.query:
            path, query = path.split('?', 2)

        query = urlparse.parse_qs(query)

        # Handle postgres percent-encoded paths.
        hostname = url.hostname or ''
        if '%2f' in hostname.lower():
            # Switch to url.netloc to avoid lower cased paths
            hostname = url.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')

        # Update with environment configuration.
        config.update({
            'scheme': url.scheme,
            'db': urlparse.unquote(path or ''),
            'user': urlparse.unquote(url.username or ''),
            'password': urlparse.unquote(url.password or ''),
            'host': hostname,
            'port': url.port or '',
        })

        # Pass the query string into OPTIONS.
        options = {}
        for key, values in query.items():
            if url.scheme == 'mysql' and key == 'ssl-ca':
                options['ssl'] = {'ca': values[-1]}
                continue

            options[key] = values[-1]
        if options:
            config['query'] = options

        return config
