import inspect
import threading
import warnings
from functools import reduce

from trod import db, errors, utils

from ..types_ import Query


class BaseQuery(Query):
    pass


class Table:

    table_lock = threading.Lock()

    AIPK = 'id'
    DEFAULT = utils.Tdict(
        __db__=None,
        __table__=None,
        __auto_increment__=1,
        __engine__='InnoDB',
        __charset__='utf8',
        __comment__='',
    )

    def __init__(self, name, fields, indexs=None, pk=None,
                 engine=None, charset=None, comment=None):
        self.name = name
        self.fields = fields
        self.indexs = indexs or {}
        self.pk = pk
        self.auto_increment = pk.ai or self.DEFAULT.__auto_increment__
        self.engine = engine or self.DEFAULT.__engine__
        self.charset = charset or self.DEFAULT.__charset__
        self.comment = comment or self.DEFAULT.__comment__
        super().__init__(None)

    @property
    def sname(self):
        return f"`{self.name}`"

    def set_sql(self, sql):

        with self.table_lock:
            self._sql = sql

    async def create(self, safe=True, **options):
        is_temp = options.pop('temporary', False)
        ct = 'CREATE TEMPORARY TABLE' if is_temp else 'CREATE TABLE'
        fdefs = [f.sql for f in self.fields]
        fdefs.append(f"PRIMARY KEY({self.pk.field.sname})")
        for index in self.indexs:
            fdefs.append(index.sql)
        fdefs = ", ".join(fdefs)
        exist = "IF NOT EXISTS" if safe else ""
        syntax = f"{ct} {exist} {self.sname} ({fdefs}) ENGINE={self.engine}\
            AUTO_INCREMENT={self.auto_increment} DEFAULT CHARSET={self.charset}\
            COMMENT='{self.comment}';"
        self.create_syntax = syntax
        self.set_sql(self.create_syntax)
        return await db.exec(self._sql)

    async def drop(self, safe=True, **_options):
        exist = "IF NOT EXISTS" if safe else ""
        self._sql = f"DROP TABLE {exist} {self.sname};"
        return await self.do()


class Show():

    __slots__ = ("_table",)
    __fetch__ = True

    def __init__(self, table):
        self._table = table
        super().__init__(None)

    def __str__(self):
        return f"<Class {self.__class__.__name__}>"

    __repr__ = __str__

    async def tables(self):
        self._sql = "SHOW TABLES"
        return await self.do()

    async def status(self):
        self._sql = "SHOW TABLE STATUS"
        return await self.do()

    async def create_syntax(self):
        self._sql = "SHOW CREATE TABLE {self._table.sname};"
        return await self.do()

    async def cloums(self):
        self._sql = "SHOW FULL COLUMNS FROM {self._table.sname};"
        return await self.do()

    async def indexs(self):
        self._sql = "SHOW INDEX FROM {self._table.sname};"
        return await self.do()


class Alter():

    def __init__(self, model, modifys=None, adds=None, drops=None):
        super().__init__(None)
        self._model = model
        self._modifys = modifys
        self._adds = adds
        self._drops = drops

        self._prepare()

    def _prepare(self):
        pass


def _find_models(module, md):
    if not module:
        return []
    if not inspect.ismodule(module):
        raise ValueError()

    return [m for _, m in vars(module).items() if issubclass(m, md)]


async def create_tables(md, *models, module=None, **options):

    models = list(models)
    models.extend(_find_models(module, md))

    if not models:
        raise RuntimeError()

    for model in models:
        await model.create(**options)


async def drop_tables(*models, module=None):
    pass


class Select(BaseQuery):

    __slots__ = (
        '_fields', '_where', '_group_by', '_order_by',
        '_limit', '_func', '_having', '_distinct', '_tdicts',
        '_args'
    )
    __fetch__ = True

    def __init__(self, model, fields, distinct=False, table=None):
        self._fields = fields
        self._where = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._limit = None
        self._distinct = distinct
        self._args = []

        distinct = " DISTINCT" if self._distinct else ""
        table = model.__table__.sname
        fields = ', '.join(self._fields)
        insql = f"SELECT{distinct} {fields} FROM {table}"
        super().__init__(model, sql=insql)

    def where(self, *filters):
        # TODO
        #  子表达式 -> Expression(...,values=())
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

    async def first(self, tdicts=False):
        self._tdicts = tdicts
        self.limit(1)
        return await super().do()

    async def rows(self, rows, start=0, tdicts=False):
        self._tdicts = tdicts
        if self._limit is not None:
            warnings.warn("", errors.ProgrammingWarning)
        self.limit(rows, start)
        return await super().do()

    async def all(self, tdicts=False):
        self._tdicts = tdicts
        return await super().do()

    def scalar(self):
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
