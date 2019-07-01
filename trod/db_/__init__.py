from trod.db_.connector import DataBase, Connector
from trod.db_.executer import Executer

__all__ = ('DataBase', 'Connector', 'Executer')


class SQL:

    __slots__ = ('_db', '_sql', '_args')

    def __init__(self, db):
        self._db = db
        self._sql = []
        self._args = None

    def __str__(self):
        args = f' % {self._args}' if self._args else ''
        return f"SQL({self.sql}){args}"

    __repr__ = __str__

    @property
    def sql(self):
        return ' '.join(self._sql)

    def do(self):
        raise NotImplementedError
