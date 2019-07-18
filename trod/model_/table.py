from trod import utils, db_ as db


class Table(db.Doer):

    AIPK = 'id'
    DEFAULT = utils.TrodDict(
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
        self._sql = sql

    async def create(self, strict=True):
        fdefs = [f.sql for f in self.fields]
        fdefs.append(f"PRIMARY KEY({self.pk.sname})")
        for index in self.indexs:
            fdefs.append(index.sql)
        fdefs = ", ".join(fdefs)
        strict = "" if strict else "IF NOT EXISTS"
        syntax = f"CREATE TABLE {strict} {self.sname} ({fdefs}) ENGINE={self.engine}\
            AUTO_INCREMENT={self.auto_increment} DEFAULT CHARSET={self.charset}\
            COMMENT='{self.comment}';"
        self.create_syntax = syntax
        self._sql = self.create_syntax
        return await self.do()

    async def drop(self):
        self._sql = f"DROP TABLE {self.sname};"
        return await self.do()

    async def exist(self):
        database = None
        connmeta = db.Connector.get_connmeta()
        if connmeta:
            database = connmeta.db
        if not database:
            database = db.Connector.selected
        if not database:
            raise RuntimeError()  # TODO
        self._sql = f"SELECT table_name FROM information_schema.tables WHERE \
            table_schema = '{database}' AND table_name = '{self.name}'"
        return await self.do()

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
        self._table.set_sql("SHOW CREATE TABLE `{self._table.sname}`;")
        return await self._table.do()

    async def cloums(self):
        self._table.set_sql("SHOW FULL COLUMNS FROM `{self._table.sname}`;")
        return await self._table.do()

    async def indexs(self):
        self._table.set_sql("SHOW INDEX FROM `{self._table.sname}`;")
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
