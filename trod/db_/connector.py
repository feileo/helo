import logging
from collections import namedtuple

import aiomysql

from trod import utils


Arg = namedtuple('Arg', ['default', 'help'])


@utils.asyncinit
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

    __slots__ = ('pool', 'meta', 'state')

    async def __init__(self, minsize=1, maxsize=10, echo=False, pool_recycle=-1, loop=None, **conn_kwargs):

        conn_kwargs = self._check_conn_kwargs(conn_kwargs)
        self.pool = await aiomysql.create_pool(
            minsize=minsize,
            maxsize=maxsize,
            echo=echo,
            loop=loop,
            pool_recycle=pool_recycle,
            **conn_kwargs
        )
        logging.info('Create database connection pool success')

    def _check_conn_kwargs(self, conn_kwargs):
        ret_kwargs = {}
        for arg in self._CONN_KWARGS:
            ret_kwargs[arg] = conn_kwargs.pop(arg, None) or self._CONN_KWARGS[arg].default
        for exarg in conn_kwargs:
            raise TypeError(
                f'{self.__class__.__name__} got an unexpected keyword argument {exarg}'
            )
        return ret_kwargs

    @classmethod
    async def from_url(cls, url, minsize=1, maxsize=10, echo=False, pool_recycle=-1, loop=None, **conn_kwgs):
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

        return cls(
            minsize=minsize, maxsize=maxsize, echo=echo,
            pool_recycle=pool_recycle, loop=loop, **conn_kwgs
        )

    def __repr__(self):
        return "<class '{0}'[{1}:{2}] for {3}:{4}/{5}>".format(
            self.__class__.__name__, self.state.minsize, self.state.maxsize, self.meta['host'],
            self.meta['port'], self.meta['db']
        )

    __str__ = __repr__

    @property
    @utils.troddict_formatter()
    def state(self):
        """ connection pool state """

        return {
            'minsize': self.pool.minsize,
            'maxsize': self.pool.maxsize,
            'size': self.pool.size,
            'freesize': self.pool.freesize
        }

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
        """ A coroutine that close pool. """

        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()

        logging.info('Database connection pool closed')

    async def terminate(self):

        await self.pool.terminate()
