from trod import model_ as model, types_ as types
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

    Model = model.Model

    async def bind(self, *args, **kwargs):
        await db.Connector.create(*args, **kwargs)

    async def unbind(self):
        await db.Connector.close()

    def text(self, use_model=False):
        pass

    def create_tables(self, *models):
        pass

    def create_all(self, module):
        pass

    def drop_tables(self, *models):
        pass

    def drop_all(self, module):
        pass
