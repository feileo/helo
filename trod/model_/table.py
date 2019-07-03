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
        self.indexs = indexs
        self.pk = pk
        self.auto_increment = pk.ai
        self.engine = engine or self.DEFAULT.__engine__
        self.charset = charset or self.DEFAULT.__charset__
        self.comment = comment or self.DEFAULT.__comment__
        super().__init__()

    def do(self):
        pass

    def create(self):
        pass

    def drop(self):
        pass

    def show(self):
        pass

    def exist(self):
        pass

    def add_index(self):
        pass
