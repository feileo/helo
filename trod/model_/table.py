import inspect
import threading

from trod import utils, db_ as db


class Table(db.Doer):

    table_lock = threading.Lock()

    AIPK = 'id'
    DEFAULT = utils.Tdict(
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
        c = 'CREATE TEMPORARY TABLE' if is_temp else 'CREATE TABLE'
        fdefs = [f.sql for f in self.fields]
        fdefs.append(f"PRIMARY KEY({self.pk.field.sname})")
        for index in self.indexs:
            fdefs.append(index.sql)
        fdefs = ", ".join(fdefs)
        exist = "IF NOT EXISTS" if safe else ""
        syntax = f"{c} {exist} {self.sname} ({fdefs}) ENGINE={self.engine}\
            AUTO_INCREMENT={self.auto_increment} DEFAULT CHARSET={self.charset}\
            COMMENT='{self.comment}';"
        self.create_syntax = syntax
        self.set_sql(self.create_syntax)
        return await self.do()

    async def drop(self, safe=True, **_options):
        exist = "IF NOT EXISTS" if safe else ""
        self._sql = f"DROP TABLE {exist} {self.sname};"
        return await self.do()

    async def exist(self):
        return await TableQuery(self.name).do()

    def show(self):
        return _Show(self)


class _Show:

    def __init__(self, table):
        self._table = table

    def __str__(self):
        return f"<Class {self.__class__.__name__}>"

    __repr__ = __str__

    async def tables(self):
        self._table.set_sql("SHOW TABLES")
        return await self._table.do()

    async def status(self):
        self._table.set_sql("SHOW TABLE STATUS")
        return await self._table.do()

    async def create_syntax(self):
        self._table.set_sql("SHOW CREATE TABLE {self._table.sname};")
        return await self._table.do()

    async def cloums(self):
        self._table.set_sql("SHOW FULL COLUMNS FROM {self._table.sname};")
        return await self._table.do()

    async def indexs(self):
        self._table.set_sql("SHOW INDEX FROM {self._table.sname};")
        return await self._table.do()


class Alter(db.Doer):

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


class TableQuery(db.Doer):

    def __init__(self, *tables, database=None):
        self._tables = tables
        self._db = database or db.current()
        self._select = True
        super().__init__(None)

    def do(self):
        tables = tuple([f"'{t}'" for t in self._tables])
        sql = f"SELECT table_name FROM information_schema.tables WHERE \
            table_schema = '{self._db}' AND table_name IN {tables}"
        self._sql = sql
        return await super().do()
