from trod import model_ as model, types
from trod import db_ as db

__version__ = '0.0.15'
__all__ = ('Trod', 'types')


class Trod:

    Model = model.Model

    db = None

    async def bind(self, *args, **kwargs):
        self.db = await db.SQL.init(*args, **kwargs)

    async def unbind(self):
        await self.db.unbind()

    async def text(self):
        pass

    def create_tables(self, *models):
        pass

    def create_all(self, module):
        pass

    def drop_tables(self, *models):
        pass

    def drop_all(self, module):
        pass
