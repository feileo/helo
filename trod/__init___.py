from trod import model_ as model, types
from trod import db_ as db

__version__ = '0.0.15'
__all__ = ('Trod', 'types', 'db')


class Trod:

    Model = model.Model

    async def bind(self, *args, **kwargs):
        await db.bind(*args, **kwargs)

    async def unbind(self):
        await db.finished()

    def init(self, connector):
        db.init(connector)

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
