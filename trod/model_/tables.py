import warnings
from functools import reduce
import operator

from .. import db, errors, utils, types


NL = types.__real__.NodeList
SQL = types.SQL


class Query:

    __slots__ = ('_sql', '_values',)

    def __init__(self, sql=None, values=None):
        if sql and isinstance(sql, str):
            sql = [sql]
        self._sql = sql or []
        self._values = values or []

    def __str__(self):
        return f"Query{self.query}"

    __repr__ = __str__

    @property
    def sql(self):
        if isinstance(self._sql, list):
            self._sql = " ".join(self._sql)
        return self._sql

    @property
    def values(self):
        return tuple(self._values)

    @property
    def query(self):
        return self.sql, self.values

    def add_query(self, query):
        self._sql.append(query)
        return self

    def add_value(self, value, nesting=False):
        if isinstance(value, types.SEQUENCE):
            if not nesting:
                self._values.extend(value)
                return self
        self._values.append(value)
        return self

    def complete(self):
        return self


class Table:

    __slots__ = (
        "db", "name", "fields", "pk", "indexes", "auto_increment",
        "engine", "charset", "comment",
    )

    AIPK = 'id'
    DEFAULT = utils.Tdict(
        __db__=None,
        __table__=None,
        __indexes__=None,
        __auto_increment__=1,
        __engine__='InnoDB',
        __charset__='utf8',
        __comment__='',
    )

    def __init__(self, database, name, fields, pk=None, indexes=None,
                 engine=None, charset=None, comment=None):
        self.db = database
        self.name = name
        self.fields = fields
        self.pk = pk
        self.indexes = indexes or {}
        self.auto_increment = pk.ai or self.DEFAULT.__auto_increment__
        self.engine = engine or self.DEFAULT.__engine__
        self.charset = charset or self.DEFAULT.__charset__
        self.comment = comment or self.DEFAULT.__comment__
        super().__init__(None)

    @property
    def __sfn__(self):
        if self.db:
            return f"`{self.db}`.`{self.name}`"
        return f"`{self.name}`"


class Show:

    __slots__ = ("_t",)

    __fetch__ = True

    def __init__(self, table):
        self._t = table

    def __str__(self):
        return f"<Class {self.__class__.__name__}>"

    __repr__ = __str__

    async def create_syntax(self):
        return await db.exec("SHOW CREATE TABLE {self._t.__sfn__};")

    async def fields(self):
        return await db.exec("SHOW FULL COLUMNS FROM {self._t.__sfn__};")

    async def indexs(self):
        return await db.exec("SHOW INDEX FROM {self._table.sname};")


class Alter:

    def __init__(self, model, modifys=None, adds=None, drops=None):
        self._model = model
        self._modifys = modifys
        self._adds = adds
        self._drops = drops

        self._prepare()

    def _prepare(self):
        pass


class Select:

    __slots__ = (
        '_fields', '_where', '_group_by', '_order_by',
        '_limit', '_func', '_having', '_distinct', '_tdicts',
        '_args',
        '_query', '_model',
    )
    __fetch__ = True

    def __init__(self, model, fields, distinct=False, _table=None):

        distinct = " DISTINCT" if distinct else ""
        table_name = model.__table__.__sfn__
        fields = ', '.join(fields)
        self._model = model
        self._query = Query(f"SELECT{distinct} {fields} FROM {table_name}")

    @property
    def __sql__(self):
        return self._query.sql

    @property
    def __values__(self):
        return self._query.values

    def where(self, *filters):
        if filters:
            _where = reduce(operator.and_, filters)
            self._query.add_query(f"WHERE {_where.sql}")
            self._query.add_value(_where.values)
        return self

    def group_by(self, *fields):
        if fields:
            _group_by = ', '.join([f.__sfn__ for f in fields])
            self._query.add_query(f"GROUP BY {_group_by}")
        return self

    def having(self, *fields):
        pass

    def order_by(self, *fields):
        if not fields:
            raise ValueError()
        fs = ', '.join([f.__sfn__ for f in fields])
        self._query.add_query(f"ORDER BY {fs}")
        return self

    def limit(self, limit=1000, offset=0):
        offset = f' OFFSET {offset}' if offset else ''
        self._query.add_query(f"LIMIT {limit}{offset}")
        return self

    async def first(self, tdicts=False):
        self._tdicts = tdicts
        self.limit(1)
        return await db.exec(self.__sql__)

    async def rows(self, rows, start=0, tdicts=False):
        self._tdicts = tdicts
        self.limit(rows, start)
        return await db.exec(self.__sql__)

    async def all(self, tdicts=False):
        self._tdicts = tdicts
        return await db.exec(self.__sql__)

    async def scalar(self):
        pass

    async def count(self):
        pass

    async def exist(self):
        pass


class Insert():

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


class Update():

    __slots__ = ('_table', '_values', '_where')

    def __init__(self, table, values):
        self._table = table
        self._values = values
        self._where = None
        super().__init__(None)

    def where(self, *filters):
        pass


class Delete():

    __slots__ = ('_table', '_where',)

    def __init__(self, table):
        self._table = table
        self._where = None
        super().__init__(None)

    def where(self, *filters):
        pass

    def limit(self):
        pass


@utils.argschecker(table=Table, nullable=False)
async def create(table, **options):

    defs = NL([f.__def__ for _, f in table.fields.items()], glue=", ", parens=True)
    defs.append(SQL(f"PRIMARY KEY({table.pk.field.__sfn__})"))
    defs.append([i.__def__ for _, i in table.indexes.items()])
    defs = defs.complete()
    safe = "IF NOT EXISTS " if options.pop("safe", True) else ""
    temp = "CREATE TEMPORARY TABLE" if options.pop('temporary', False) else "CREATE TABLE"
    create_syntax = NL([
        SQL(f"{temp} {safe}{table.__sfn__}"),
        defs,
        SQL(f"ENGINE={table.engine} AUTO_INCREMENT={table.auto_increment}"),
        SQL(f"DEFAULT CHARSET={table.charset}  COMMENT='{table.comment}';")
    ]).complete()

    return await db.exec(create_syntax)


async def drop(table, safe=True, **_options):

    exist = "IF NOT EXISTS" if safe else ""
    return await db.exec(SQL(f"DROP TABLE{exist} {table.__sfn__};"))
