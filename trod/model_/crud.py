from trod import db_ as db


class SQL(db.SQL):

    __slots__ = ()

    def do(self):
        if isinstance(self, Select):
            return self._db.fetch(self.sql, args=self._args)
        is_batch = getattr(self, '_batch', False)
        return self._db.execute(self.sql, values=self._args, is_batch=is_batch)


class Select(SQL):

    __slots__ = ('_table', '_fields', '_where', '_group_by', '_order_by', '_rows')

    def __init__(self, db, table, *fields):
        self._table = table
        self._fields = fields
        self._where = None
        self._group_by = None
        self._order_by = None
        self._rows = None
        super().__init__(db)

    def where(self, **query):
        pass

    def group_by(self, *fields):
        pass

    def order_by(self, field, desc=False):
        pass

    def rows(self, limit=500, offset=0):
        pass

    def first(self):
        pass

    def all(self):
        return self.do()

    def scalar(self):
        pass


class Insert(SQL):

    __slots__ = ('_table', '_rows', '_batch')

    def __init__(self, db, table, *rows):
        self._table = table
        self._rows = rows
        self._batch = False
        super().__init__(db)


class Update(SQL):

    __slots__ = ('_table', '_values', '_where')

    def __init__(self, db, table, *values):
        self._table = table
        self._values = values
        self._where = None
        super().__init__(db)

    def where(self, **query):
        pass


class Delete(SQL):

    __slots__ = ('_table', '_where')

    def __init__(self, db, table):
        self._table = table
        self._where = None
        super().__init__(db)

    def where(self, **query):
        pass

class Replace(Insert):
    pass

