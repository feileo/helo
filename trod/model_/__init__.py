from trod.model_.core import _Model

__all__ = ('Model')


class Model(_Model):

    @classmethod
    async def get(cls, pk):
        return await cls._get(pk)

    @classmethod
    async def mget(cls, pks, *fields):
        return await cls._get_many(pks, *fields)

    @classmethod
    async def select(cls, *fields):
        return await cls._select(*fields)

    @classmethod
    async def insert(cls, **values):
        return await cls._insert(**values)

    @classmethod
    async def minsert(cls, rows, fields=None):
        return await cls._insert_many(rows, fields=fields)

    @classmethod
    async def add(cls, model):
        return await cls._add(model)

    @classmethod
    async def madd(cls, models):
        return await cls._add_many(models)

    @classmethod
    async def update(cls, **values):
        return await cls._update(**values)

    @classmethod
    async def delete(cls):
        return await cls._delete()

    async def save(self):
        return await self._save()

    async def remove(self):
        return await self._remove()
