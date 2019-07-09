from functools import reduce

from trod import db_ as db


class Select(db.Doer):

    __slots__ = (
        '_select', '_model', '_fields', '_where', '_group_by', '_order_by',
        '_rows', '_func', '_having', '_distinct',
    )

    def __init__(self, model, *fields, distinct=False):
        self._model = model
        self._fields = fields
        self._where = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._rows = None
        self._distinct = " DISTINCT" if distinct else ""

        # TODO func
        # SELECT
        # orderNumber,
        # COUNT(orderNumber) AS items
        # FROM
        # orderdetails
        # GROUP BY orderNumber

        fields = ', '.join(self._fields)
        self._select = f"SELECT{self._distinct} {fields} FROM `{self._table}`"
        super().__init__(sql=self._select)

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

    async def all(self):
        return await self.rows()

    async def rows(self, limit=1000, offset=0):
        self._rows = f"LIMIT {limit} OFFSET {offset}"
        self._sql.append(self._rows)
        return await self.do()

    async def first(self):
        self._rows = "LIMIT 1"
        self._sql.append(self._rows)
        return await self.do()

    def scalar(self):
        pass


class Insert(db.Doer):

    __slots__ = ('_insert', '_model', '_rows', '_batch')

    def __init__(self, model, rows):
        self._model = model
        self._rows = rows
        self._batch = False

        # fields = ', '.join(f.join('``') for f in self._fields)
        # self._insert = f"INSERT INTO `{self._table}` () VALUES ();"
        # super().__init__(sql=self._insert, args={})

        super().__init__()

    def select(self):
        pass


class Update(db.Doer):

    __slots__ = ('_model', '_values', '_where')

    def __init__(self, model, values):
        self._model = model
        self._values = values
        self._where = None
        super().__init__()

    def where(self, *filters):
        pass


class Delete(db.Doer):

    __slots__ = ('_model', '_where')

    def __init__(self, model):
        self._model = model
        self._where = None
        super().__init__()

    def where(self, *filters):
        pass

    def limit(self):
        pass


class Replace(Insert):
    pass
