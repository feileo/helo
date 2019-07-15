from functools import reduce

from trod import db_ as db


class Select(db.Doer):

    __slots__ = (
        '_select', '_fields', '_where', '_group_by', '_order_by',
        '_limit', '_func', '_having', '_distinct', '_use_td'
    )

    def __init__(self, model, fields, distinct=False, table=None):
        self._model = model
        self._fields = fields
        self._where = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._limit = None
        self._distinct = " DISTINCT" if distinct else ""
        table = self._model.__table__.name or table

        fields = ', '.join(self._fields)
        self._select = f"SELECT{self._distinct} {fields} FROM `{table}`"
        super().__init__(model, sql=self._select)

    def where(self, *filters):
        if filters:
            _where = reduce(lambda f1, f2: f1 & f2, filters)
            self._where = f"WHERE {_where.sql}"

        if self._where:
            self._sql.append(self._where)

        return self

    def group_by(self, *fields):
        if fields:
            _group_by = ', '.join([f.sname for f in fields])
            self._group_by = f"GROUP BY {_group_by}"

        if self._group_by:
            self._sql.append(self._group_by)

        return self

    def having(self, *fields):
        pass

    def order_by(self, fields):
        fs = []
        for f in fields:
            if isinstance(f, str):
                fs.append(f)
            else:
                fs.append(f.sname)
        fs = ', '.join(fs)
        self._order_by = f"ORDER BY {fs}"
        self._sql.append(self._order_by)
        return self

    def limit(self, limit=1000, offset=0):

        offset = f' OFFSET {offset}' if offset else ''
        self._limit = f"LIMIT {limit}{offset}"
        self._sql.append(self._limit)
        return self

    def do(self):
        raise AttributeError()

    async def all(self, tdicts=False):

        self._use_td = tdicts
        return await super().do()

    async def first(self, tdicts=False):
        self._use_td = tdicts
        self.limit(1)
        return await super().do()

    def scalar(self):
        pass


class Insert(db.Doer):

    __slots__ = ('_table', '_rows')

    _c = "INSERT INTO"

    def __init__(self, table, rows):
        self._table = table
        self._rows = rows

        insert = f"{self._c} `{self._table}` ({self._rows.fields}) VALUES ({self._rows.values});"
        super().__init__(None, sql=insert, args={})

    def select(self, *fields):
        fields = [f.sname for f in fields]
        return Select(None, fields, table=self._table)


class Replace(Insert):

    __slots__ = ('_replace', '_table', '_rows')

    _c = "REPLACE INTO"


class Update(db.Doer):

    __slots__ = ('_table', '_values', '_where')

    def __init__(self, table, values):
        self._table = table
        self._values = values
        self._where = None
        super().__init__(None)

    def where(self, *filters):
        pass


class Delete(db.Doer):

    __slots__ = ('_table', '_where',)

    def __init__(self, table):
        self._table = table
        self._where = None
        super().__init__(None)

    def where(self, *filters):
        pass

    def limit(self):
        pass
