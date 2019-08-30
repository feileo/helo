from . import (
    types,
    db,
    utils,
    errors,
    model_,
)

__version__ = '0.0.15'

__all__ = (
    'types',
    'db',
    'utils',
    'errors',
    'Trod',
)


class Trod:

    Model = model_.TrodModel

    async def bind(self, *args, **kwargs):

        return await db.binding(*args, **kwargs)

    async def unbind(self):

        return await db.unbinding()

    async def create_tables(self, *models, **options):

        for model in models:
            await model.create(**options)

        return True

    async def create_all(self, module, **options):

        if not utils.inspect.ismodule(module):
            raise TypeError()

        return await self.create_tables(
            *[m for _, m in vars(module).items() if issubclass(m, self.Model)],
            **options
        )

    async def drop_tables(self, *models):

        for model in models:
            await model.drop()

        return True

    async def drop_all(self, module):

        if not utils.inspect.ismodule(module):
            raise TypeError()

        return await self.drop_tables(
            *[m for _, m in vars(module).items() if issubclass(m, self.Model)],
        )

    async def text(self, *args, **kwargs):
        """ A coroutine that used to directly execute SQL statements """

        return await db.execute(*args, **kwargs)

    # DCL

    # async def get_tables():
    #     return await db.exec(SQL("SHOW TABLES"))

    # async def get_tables_status():
    #     return await db.exec(SQL("SHOW TABLE STATUS"))
