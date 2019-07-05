from functools import reduce

from trod import db_ as db


class Select(db.Doer):

    __slots__ = (
        '_select', '_table', '_fields', '_where', '_group_by', '_order_by',
        '_rows', '_func'
    )

    def __init__(self, table, *fields):
        self._table = table
        self._fields = fields
        self._where = None
        self._group_by = None
        self._order_by = None
        self._rows = None

        # TODO func
        # SELECT
        # orderNumber,
        # COUNT(orderNumber) AS items
        # FROM
        # orderdetails
        # GROUP BY orderNumber

        fields = ', '.join(self._fields)
        self._select = f"SELECT {fields} FROM `{self._table}`"
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

    def order_by(self, field, desc=False):
        desc = 'DESC' if desc else 'ASC'
        self._order_by = f"ORDER BY {field.name} {desc}"
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

    __slots__ = ('_insert', '_table', '_rows', '_batch')

    def __init__(self, table, rows):
        self._table = table
        self._rows = rows
        self._batch = False

        # fields = ', '.join(f.join('``') for f in self._fields)
        # self._insert = f"INSERT INTO `{self._table}` () VALUES ();"
        # super().__init__(sql=self._insert, args={})

        super().__init__()

    def select(self):
        pass


class Update(db.Doer):

    __slots__ = ('_table', '_values', '_where')

    def __init__(self, table, values):
        self._table = table
        self._values = values
        self._where = None
        super().__init__()

    def where(self, *filters):
        pass


class Delete(db.Doer):

    __slots__ = ('_table', '_where')

    def __init__(self, table):
        self._table = table
        self._where = None
        super().__init__()

    def where(self, *filters):
        pass


class Replace(Insert):
    pass
