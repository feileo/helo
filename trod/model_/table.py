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
        super().__init__(model=None)

    @property
    def sname(self):
        return f"`{self.name}`"

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

    def show(self):
        return self.Show()

    def exist(self):
        pass

    class Show:

        def tables(self):
            self._sql = "SHOW TABLES"
            return await Table.do()

        def status(self):
            self._sql = "SHOW TABLE STATUS"
            return await Table.do()

        def create_syntax(self):
            self._sql = "SHOW CREATE TABLE `{Table.sname}`;"
            return await Table.do()

        def cloums(self):
            self._sql = "SHOW FULL COLUMNS FROM `{Table.sname}`;"
            return await Table.do()

        def indexs(self):
            self._sql = "SHOW INDEX FROM `{Table.sname}`;"
            return await Table.do()
