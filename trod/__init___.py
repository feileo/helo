from trod import model_ as model, types
from trod import db_ as db

__version__ = '0.0.15'
__all__ = ('Trod', 'types')


class Trod:

    Model = model.Model
    Bind = db.Executer

    def bind(self):
        pass

    def text(self, sql, args=None, rows=None):
        pass

    def create_tables(self, *models):
        pass

    def create_all(self, module):
        pass

    def drop_tables(self, *models):
        pass

    def drop_all(self, module):
        pass
