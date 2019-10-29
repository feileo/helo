"""
    trod
    ~~~~
"""
from . import (
    db,
    types,
    model,
    g,
    err,
    util
)

__version__ = '0.0.15'
__all__ = (
    'db',
    'g',
    'types',
    'util',
    'err',
    'Model',
    'Trod',
)


class Trod:

    Model = model.Model

    async def bind(self, *args, **kwargs):

        return await db.binding(*args, **kwargs)

    async def unbind(self):

        return await db.unbinding()

    async def create_tables(self, *models, **options):

        for m in models:
            await m.create(**options)

        return True

    async def create_all(self, module, **options):

        if not util.ismodule(module):
            raise TypeError()

        return await self.create_tables(
            *[m for _, m in vars(module).items() if issubclass(m, self.Model)],
            **options
        )

    async def drop_tables(self, *models):

        for m in models:
            await m.drop()

        return True

    async def drop_all(self, module):

        if not util.ismodule(module):
            raise TypeError()

        return await self.drop_tables(
            *[m for _, m in vars(module).items() if issubclass(m, self.Model)],
        )

    async def text(self, *args, **kwargs):
        """ A coroutine that used to directly execute SQL statements """

        return await db.execute(*args, **kwargs)
