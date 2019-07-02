from trod.db_.connector import Connector
from trod.db_.executer import Executer
from trod import errors

__all__ = ('Connector', 'Executer')


class SQL:

    executer = None

    __slots__ = ('_sql', '_args')

    def __init__(self):
        if self.executer is None:
            raise errors.NoExecuterError()  # TODO

        self._sql = []
        self._args = None

    @classmethod
    def init(cls, *args, **kwargs):
        cls.executer = Executer.init(*args, **kwargs)
        return cls.executer

    def __str__(self):
        args = f' % {self._args}' if self._args else ''
        return f"SQL({self.sql}){args}"

    __repr__ = __str__

    @property
    def sql(self):
        return ' '.join(self._sql)

    def do(self):
        raise NotImplementedError
