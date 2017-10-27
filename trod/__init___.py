from trod import model_ as _model, types_ as types
from trod import db
from trod import utils

from . import errors
__version__ = '0.0.15'

__all__ = (
    'Trod',
    'types',
    'db',
    'utils',
    'errors',
)


class Trod:

    Model = _model.Model

    async def bind(self, *args, **kwargs):

        return await db.Connector.create(*args, **kwargs)

    async def unbind(self):

        return await db.Connector.close()

    def select_db(self, database):

        db.Connector.select_db(database)

    async def create_tables(self, *models, **options):
        """
        safe
        temporary
        """

        return await _model.table.create_tables(self.Model, *models, **options)

    async def create_all(self, module):

        return await _model.table.create_tables(self.Model, module=module)

    async def drop_tables(self, *models):

        return await _model.table.drop_tables(self.Model, *models)

    async def drop_all(self, module):

        return await _model.table.drop_tables(self.Model, module=module)

    async def alter(self, *args, **kwargs):
        return await _model.table.Alter(*args, **kwargs).do()

    async def text(self, *args, **kwargs):
        """ A coroutine that used to directly execute SQL statements """

        return await db.text(*args, **kwargs)
