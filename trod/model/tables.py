from functools import reduce
from collections import OrderedDict
import operator

from .. import db, errors, utils, types


class Table:

    __slots__ = (
        "db", "name", "fields_dict", "primary",
        "indexes", "auto_increment", "engine", "charset",
        "comment",
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

    def __init__(self, database, name, fields, primary=None, indexes=None,
                 engine=None, charset=None, comment=None):
        self.db = database
        self.name = name
        self.fields_dict = fields
        self.primary = primary
        self.indexes = indexes
        self.auto_increment = primary.ai or self.DEFAULT.__auto_increment__
        self.engine = engine or self.DEFAULT.__engine__
        self.charset = charset or self.DEFAULT.__charset__
        self.comment = comment or self.DEFAULT.__comment__
        super().__init__(None)

    @property
    def __sfn__(self):
        if self.db:
            return f"`{self.db}`.`{self.name}`"
        return f"`{self.name}`"

    @property
    def sfn_fields(self):
        return [f.__sfn__ for f in self.fields_dict.values()]

    @property
    def sfn_indexes(self):
        return [i.__sfn__ for i in self.indexes]

    @property
    def def_fields(self):
        return [f.__def__ for f in self.fields_dict.values()]

    @property
    def def_indexes(self):
        return [i.__def__ for i in self.indexes]

    async def create(self, **options):

        defs = types.__impl__.NodesComper(self.def_fields, glue=", ", parens=True)
        defs.append(f"PRIMARY KEY({self.primary.field.__sfn__})")
        defs.append(self.def_indexes)
        defs = defs.complete()
        safe = "IF NOT EXISTS " if options.pop("safe", True) else ""
        temp = "CREATE TEMPORARY TABLE" if options.pop('temporary', False) else "CREATE TABLE"
        create_syntax = types.__impl__.NodesComper([
            f"{temp} {safe}{self.__sfn__}",
            defs,
            f"ENGINE={self.engine} AUTO_INCREMENT={self.auto_increment}",
            f"DEFAULT CHARSET={self.charset}  COMMENT='{self.comment}';"
        ]).complete()

        return await db.execute(create_syntax)

    async def drop(self, safe=True, **_options):

        exist = "IF NOT EXISTS" if safe else ""
        return await db.execute(f"DROP TABLE{exist} {self.__sfn__};")

    def show(self):
        return Show(self)

    def alter(self):
        return Alter(self)


class Query:

    __slots__ = ('_nodes', '_sql', '_values',)

    def __init__(self, node=None):
        self._nodes = OrderedDict(node or {})
        self._sql = None
        self._values = []

    def __str__(self):
        return f"Query{self.query}"

    __repr__ = __str__

    @property
    def sql(self):
        self._sql = " ".join(list(self._nodes.values()))
        return f"{self._sql};"

    @property
    def values(self):
        return tuple(self._values)

    @property
    def query(self):
        return self.sql, self.values

    def completing(self, key, query):
        self._nodes[key] = query
        return self

    def with_value(self, value, nesting=False):
        if value:
            if isinstance(value, types.SEQUENCE):
                if not nesting:
                    self._values.extend(value)
                    return self
            self._values.append(value)
        return self

    def has(self, key):
        return key in self._nodes

    def get(self, key):
        return self._nodes.get(key)


class QueryBase:

    __slots__ = ('_q',)

    def __init__(self, key=None, query=None):
        if key and query:
            self._q = Query({key, query})
        else:
            self._q = Query()

    def __repr__(self):
        return str(self._q)

    __str__ = __repr__

    @property
    def __sql__(self):
        return self._q.sql

    @property
    def __values__(self):
        return self._q.values


class WriteQuery(QueryBase):

    async def do(self):
        return await db.execute(self.__sql__, self.__values__)


class Select(QueryBase):

    __slots__ = ('_model',)
    __fetch__ = True

    _CLAUSE = utils.Tdict(
        select="select",
        where="where",
        group_by="group_by",
        having="having",
        order_by="order_by",
        limit="limit",
    )

    def __init__(self, model, columns, database=None, table=None, distinct=False):

        self._model = model

        if model:
            table_name = model.__table__.__sfn__ if model else table
        else:
            if not table:
                raise ValueError()
            table_name = f"{database}.{table}" if database else table
        columns = ", ".join([f.__sfn__ for f in columns])
        distinct = " DISTINCT" if distinct else ""
        super().__init__(self._CLAUSE.select, f"SELECT{distinct} {columns} FROM {table_name}")

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")
        expr = reduce(operator.and_, filters)
        self._q.completing(self._CLAUSE.where, f"WHERE {expr.sql}").with_value(expr.values)
        return self

    def group_by(self, *columns):
        if not columns:
            raise ValueError("Group by clause cannot be empty")
        _group_by = ', '.join([f.__sfn__ for f in columns])
        self._q.completing(self.group_by, f"GROUP BY {_group_by}")
        return self

    def having(self, *filters):
        if not filters:
            raise ValueError("Having clause cannot be empty")
        expr = reduce(operator.and_, filters)
        self._q.completing(self._CLAUSE.having, f"HAVING {expr.sql}").with_value(expr.values)
        return self

    def order_by(self, *columns):
        if not columns:
            raise ValueError("Order by clause cannot be empty")
        fs = ', '.join([f.__sfn__ for f in columns])
        self._q.completing(self._CLAUSE.order_by, f"ORDER BY {fs}")
        return self

    def limit(self, limit=1000, offset=0):
        offset = f" OFFSET {offset}" if offset else ""
        self._q.completing(self._CLAUSE.limit, f"LIMIT {limit}{offset}")
        return self

    async def first(self):
        self.limit(1)
        return await db.execute(self.__sql__, params=self.__values__, model=self._model)

    async def rows(self, rows, start=0):
        self.limit(rows, start)
        return await db.execute(self.__sql__, params=self.__values__, model=self._model)

    async def all(self):
        return await db.execute(self.__sql__, params=self.__values__, model=self._model)

    async def scalar(self):
        pass

    async def count(self):
        pass

    async def exist(self):
        pass


class Insert(WriteQuery):

    __slots__ = ('_table', '_rows')

    _CLAUSE = utils.Tdict(
        insert="insert",
        columns="columns",
        values="values",
    )

    def __init__(self, table, rows):
        self._table = table.__sfn__
        self._rows = rows
        super().__init__(self._CLAUSE.insert, f"INSERT INTO {self._table}")

        columns = tuple([r.__sfn__ for r in self._rows.columns])
        placeholders = ", ".join(["'%s'"] * self._rows.num)
        self._q.completing(
            self._CLAUSE.columns, str(columns)
        ).completing(
            self._CLAUSE.values, f"VALUES ({placeholders})"
        ).with_value(self._rows.values)

    def select(self, *columns):
        if self._q.get(self._CLAUSE.columns) or self._q.get(self._CLAUSE.values):
            raise errors.ProgrammingError()
        # TODO
        return Select(None, columns, table=self._table)


class Replace(WriteQuery):

    __slots__ = ('_table', '_rows')

    _CLAUSE = utils.Tdict(
        replace="replace",
        columns="columns",
        values="values",
    )

    def __init__(self, table, rows):
        self._table = table.__sfn__
        self._rows = rows
        super().__init__(self._CLAUSE.replace, f"REPLACE INTO {self._table}")

        columns = tuple([r.__sfn__ for r in self._rows.columns])
        placeholders = ", ".join(["'%s'"] * self._rows.num)
        self._q.completing(
            self._CLAUSE.columns, str(columns)
        ).completing(
            self._CLAUSE.values, f"VALUES ({placeholders})"
        ).with_value(self._rows.values)

    def select(self):
        pass


class Update(WriteQuery):

    __slots__ = ('_table',)

    _CLAUSE = utils.Tdict(
        update="update",
        columns="columns",
        where="where",
    )

    def __init__(self, table, update):
        self._table = table.__sfn__
        self._update = update
        super().__init__(self._CLAUSE.update, f"UPDATE {self._table} SET")

        columns, values = [], []
        for k, v in update.items():
            columns.append(f"{k}='%s'")
            values.append(v)
        columns = ', '.join(columns)
        self._q.completing(
            self._CLAUSE.columns, columns
        ).with_value(values)

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")
        expr = reduce(operator.and_, filters)
        self._q.completing(self._CLAUSE.where, f"WHERE {expr.sql}").with_value(expr.values)
        return self


class Delete(WriteQuery):

    __slots__ = ('_table', '_where')

    _CLAUSE = utils.Tdict(
        delete="delete",
        where="where",
        limit="limit",
    )

    def __init__(self, table):
        self._table = table.__sfn__
        super().__init__(self._CLAUSE.delete, f"DELETE FROM {self._table}")

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")
        expr = reduce(operator.and_, filters)
        self._q.completing(self._CLAUSE.where, f"WHERE {expr.sql}").with_value(expr.values)
        return self

    def limit(self, row_count):
        self._q.completing(self._CLAUSE.limit, f"LIMIT {row_count}")
        return self


class Show:

    __slots__ = ("_t",)

    __fetch__ = True

    def __init__(self, table):
        self._t = table

    def __str__(self):
        return f"<Class {self.__class__.__name__}>"

    __repr__ = __str__

    async def create_syntax(self):
        return await db.execute("SHOW CREATE TABLE {self._t.__sfn__};")

    async def columns(self):
        return await db.execute("SHOW FULL COLUMNS FROM {self._t.__sfn__};")

    async def indexes(self):
        return await db.execute("SHOW INDEX FROM {self._table.sname};")

    async def engine(self):
        pass


class Alter(WriteQuery):

    def __init__(self, table):
        self._t = table
        super().__init__()

    def add(self):
        pass

    def drop(self):
        pass

    def modify(self):
        pass

    def change(self):
        pass

    def after(self):
        pass

    def first(self):
        pass
