from trod import model_ as _model, types_ as types
from trod import db_ as db
from trod import utils

__version__ = '0.0.15'

__all__ = (
    'Trod',
    'types',
    'db',
    'utils'
)


class Trod:

    Model = _model.Model

    async def bind(self, *args, **kwargs):
        await db.Connector.create(*args, **kwargs)

    async def unbind(self):
        await db.Connector.close()

    def select_db(self, database):
        db.Connector.select_db(database)

    async def table_exist(self, database, table):
        pass

    async def text(self, use_model=False):
        pass

    def alter(self, *args, **kwargs):
        return _model.table.Alter(*args, **kwargs)

    def create_tables(self, *models):
        pass

    def create_all(self, module):
        pass

    def drop_tables(self, *models):
        pass

    def drop_all(self, module):
        pass
