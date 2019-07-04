import warnings
from trod.db_.connector import Connector
from trod.db_.executer import Executer
from trod import errors

__all__ = ('Connector', 'Executer', 'Doer')


async def bind(*args, **kwargs):
    if Executer.connector is not None:
        raise RuntimeError()

    if args or kwargs.get('url'):
        connector = await Connector.from_url(*args, **kwargs)
    else:
        connector = await Connector(*args, **kwargs)
    init(connector)


def init(connector):
    Executer.init(connector)


async def finished():

    if Executer.connector:
        Executer.connector = await Executer.connector.close()
        return True

    warnings.warn('No binding db connector or closed', errors.ProgrammingWarning)
    return False


def state():
    if Executer.connector:
        return Executer.connector.state
    return None


class Doer:

    __slots__ = ('_sql', '_args')

    def __init__(self, sql=None, args=None):
        self._sql = sql or []
        self._args = args

    def __str__(self):
        args = f' % {self._args}' if self._args else ''
        return f"Doer({Executer.connector}) for SQL({self.sql}{args})"

    __repr__ = __str__

    @property
    def sql(self):
        if isinstance(self._sql, (list, tuple)):
            self._sql.append(';')
            self._sql = ' '.join(self._sql)
        return self._sql

    async def do(self):
        if Executer.connector is None:
            raise errors.NoExecuterError()

        if getattr(self, '_select', False):
            return await Executer.fetch(self.sql, args=self._args)
        return await Executer.execute(
            self.sql, values=self._args, is_batch=getattr(self, '_batch', False)
        )
