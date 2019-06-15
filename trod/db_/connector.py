from collections import namedtuple

import aiomysql

from trod import utils
from trod.db_ import utils as db_utils


Arg = namedtuple('Arg', ['default', 'help'])


class DataBase:

    __slots__ = ('connmeta',)

    def __init__(self, **kwargs):
        self.connmeta = kwargs


@utils.singleton
@utils.asyncinit
class Connector(DataBase):
    """ Provide a factory method to create a database connection pool.

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

        Ex:
            connector = await Connector.create(url)

        Get a connection from the connection pool:
            connection = await connector.get()

        Close the pool:
            await connector.close()

        And etc.
    """
    _CONN_KWARGS = utils.TrodDict(
        host=Arg(default="localhost", help=''),
        user=Arg(default=None, help_=''),
        password=Arg(default="", help_=''),
        db=Arg(default=None, help_=''),
        port=Arg(default=3306, help_=''),
        unix_socket=Arg(default=None, help_=''),
        charset=Arg(default='', help_=''),
        sql_mode=Arg(default=None, help_=''),
        read_default_file=Arg(default=None, help_=''),
        conv=Arg(default=aiomysql.connection.decoders, help_=''),
        use_unicode=Arg(default=None, help_=''),
        client_flag=Arg(default=0, help_=''),
        cursorclass=Arg(default=aiomysql.cursors.DictCursor, help_=''),
        init_command=Arg(default=None, help_=''),
        connect_timeout=Arg(default=None, help_=''),
        read_default_group=Arg(default=None, help_=''),
        no_delay=Arg(default=None, help_=''),
        autocommit=Arg(default=False, help_=''),
        echo=Arg(default=False, help_=''),
        loop=Arg(default=None, help_=''),
        local_infile=Arg(default=False, help_=''),
        ssl=Arg(default=None, help_=''),
        auth_plugin=Arg(default='', help_=''),
        program_name=Arg(default='', help_=''),
        server_public_key=Arg(default=None, help_=''),
    )
    _POOL_KWARGS = ('minsize', 'maxsize', 'echo', 'pool_recycle', 'loop')

    __slots__ = ('pool', '_conn_kwargs')

    async def __init__(self, minsize=1, maxsize=10, echo=False,
                       pool_recycle=-1, loop=None, **conn_kwargs):

        conn_kwargs = self._check_conn_kwargs(conn_kwargs)
        self.pool = await aiomysql.create_pool(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **conn_kwargs
        )
        super().__init__(**conn_kwargs)

    @classmethod
    async def from_url(cls, url, minsize=1, maxsize=10, echo=False,
                       pool_recycle=-1, loop=None, **conn_kwargs):
        """ A coroutine that create a connection pool object
        """
        if not url:
            raise ValueError('Db url cannot be empty')

        db_meta = db_utils.UrlParser(url).parse()
        db_meta.update(conn_kwargs)
        for arg in cls._POOL_KWARGS:
            db_meta.pop(arg, None)

        return await cls(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop,
            **db_meta
        )

    def __repr__(self):
        return "<Class '{0}'[{1}:{2}] for {3}:{4}/{5}>".format(
            self.__class__.__name__, self.state.minsize, self.state.maxsize,
            self._conn_kwargs.host, self._conn_kwargs.port, self._conn_kwargs.db
        )

    __str__ = __repr__

    @property
    def state(self):
        """ connection pool state """

        return utils.TrodDict(
            minsize=self.pool.minsize,
            maxsize=self.pool.maxsize,
            size=self.pool.size,
            freesize=self.pool.freesize
        )

    @utils.troddict_formatter()
    def _check_conn_kwargs(self, conn_kwargs):

        ret_kwargs = {}
        for arg in self._CONN_KWARGS:
            ret_kwargs[arg] = conn_kwargs.pop(arg, None) or self._CONN_KWARGS[arg].default
        for exarg in conn_kwargs:
            raise TypeError(
                f'{self.__class__.__name__} got an unexpected keyword argument {exarg}'
            )
        return ret_kwargs

    def get(self):
        """ Get a connection """

        return self.pool.acquire()

    def release(self, connect):
        """ Reverts connection conn to free pool for future recycling. """

        return self.pool.release(connect)

    async def clear(self):
        """ A coroutine that closes all free connections in the pool.
            At next connection acquiring at least minsize of them will be recreated
        """
        await self.pool.clear()

    async def close(self):
        """ A coroutine that close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """

        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()

    async def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        await self.pool.terminate()
