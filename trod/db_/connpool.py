from collections import namedtuple

import aiomysql

from trod import utils
from trod.db_ import schemes


Arg = namedtuple('Arg', ['dft', 'help'])


@utils.singleton
@utils.asyncinit
class Pool:
    """ Create a MySQL connection pool based on `aiomysql.create_pool`.

        :param int minsize: Minimum sizes of the pool
        :param int maxsize: Maximum sizes of the pool
        :param bool echo: Executed log SQL queryes
        :param int pool_recycle: Connection reset period, default -1,
            indicating that the connection will be reclaimed after a given time,
            be careful not to exceed MySQL default time of 8 hours
        :param loop: Is an optional event loop instance,
            asyncio.get_event_loop() is used if loop is not specified.
        :param conn_kwargs: See `_CONN_KWARGS`.

        :Returns : `Pool` instance
    """
    _CONN_KWARGS = utils.Tdict(
        host=Arg(dft="localhost", help='Host where the database server is located'),
        user=Arg(dft=None, help='Username to log in as'),
        password=Arg(dft="", help='Password to use'),
        db=Arg(dft=None, help='Database to use, None to not use a particular one'),
        port=Arg(dft=3306, help='MySQL port to use'),
        unix_socket=Arg(dft=None, help='You can use a unix socket rather than TCP/IP'),
        charset=Arg(dft='', help='Charset you want to use'),
        sql_mode=Arg(dft=None, help='Default SQL_MODE to use'),
        read_default_file=Arg(dft=None, help='Specifies my.cnf file to read these parameters'),
        use_unicode=Arg(dft=None, help='Whether or not to default to unicode strings'),
        cursorclass=Arg(dft=aiomysql.cursors.DictCursor, help='Custom cursor class to use'),
        init_command=Arg(dft=None, help='Initial SQL statement to run when connection is established'),
        connect_timeout=Arg(dft=15, help='Timeout before throwing an exception when connecting'),
        autocommit=Arg(dft=False, help='Autocommit mode. None means use server default'),
        echo=Arg(dft=False, help='Echo mode'),
        loop=Arg(dft=None, help='Asyncio loop'),
        local_infile=Arg(dft=False, help='bool to enable the use of LOAD DATA LOCAL cmd'),
        ssl=Arg(dft=None, help='Optional SSL Context to force SSL'),
        auth_plugin=Arg(dft='', help='String to manually specify the authentication plugin to use'),
        program_name=Arg(dft='', help='Program name string to provide'),
        server_public_key=Arg(dft=None, help='SHA256 authentication plugin public key value'),
    )
    _POOL_KWARGS = ('minsize', 'maxsize', 'echo', 'pool_recycle', 'loop')

    __slots__ = ('_pool', '_connmeta')

    async def __init__(self, minsize=1, maxsize=15, echo=False,
                       pool_recycle=-1, loop=None, **conn_kwargs):

        conn_kwargs = utils.format_troddict(self._check_conn_kwargs(conn_kwargs))
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
        """ Provide a factory method `from_url` to create a connection pool.

        :params see `__init__` for information

        :Returns : `Pool` instance
        """
        if not url:
            raise ValueError('Db url cannot be empty')

        db_meta = schemes.UrlParser(url).parse()
        db_meta.update(conn_kwargs)
        for arg in cls._POOL_KWARGS:
            db_meta.pop(arg, None)

        return await cls(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **db_meta
        )

    def __repr__(self):
        return "<{0}[{1}:{2}] for {3}:{4}/{5}>".format(
            self.__class__.__name__, self.state.minsize, self.state.maxsize,
            self.connmeta.host, self.connmeta.port, self.connmeta.db
        )

    __str__ = __repr__

    @property
    def connmeta(self):
        return self._connmeta

    @property
    def state(self):
        """ Connection pool state """

        return utils.Tdict(
            minsize=self._pool.minsize,
            maxsize=self._pool.maxsize,
            size=self._pool.size,
            freesize=self._pool.freesize
        )

    def _check_conn_kwargs(self, conn_kwargs):

        ret_kwargs = {}
        for arg in self._CONN_KWARGS:
            ret_kwargs[arg] = conn_kwargs.pop(arg, None) or self._CONN_KWARGS[arg].dft
        for exarg in conn_kwargs:
            raise TypeError(
                f'{self.__class__.__name__} got an unexpected keyword argument {exarg}'
            )
        return ret_kwargs

    def acquire(self):
        """ Acquice a connection """

        return self._pool.acquire()

    def release(self, connect):
        """ Reverts connection conn to free pool for future recycling. """

        return self._pool.release(connect)

    async def clear(self):
        """ A coroutine that closes all free connections in the pool.
            At next connection acquiring at least minsize of them will be recreated
        """

        await self._pool.clear()

    async def close(self):
        """ A coroutine that close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """

        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()

    async def terminate(self):
        """ A coroutine that terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        await self._pool.terminate()
