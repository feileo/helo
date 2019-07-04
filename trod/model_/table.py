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
        super().__init__()

    async def create(self):
        fdefs = [f.sql() for f in self.fields]
        fdefs.append(f"PRIMARY KEY(`{self.pk.name}`)")
        for index in self.indexs:
            fdefs.append(index.sql())
        fdefs = ', '.join(fdefs)
        syntax = f"CREATE TABLE `{self.name}` ({fdefs}) ENGINE={self.engine}\
            AUTO_INCREMENT={self.auto_increment} DEFAULT CHARSET={self.charset}\
            COMMENT='{self.comment}';"
        self.create_syntax = syntax
        self._sql = self.create_syntax
        return await self.do()

    async def drop(self):
        self._sql = f"DROP TABLE `{self.name}`;"
        return await self.do()

    def show(self):
        pass

    def exist(self):
        pass

    def add_index(self):
        pass
