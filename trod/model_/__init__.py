from trod.model_.core import _Model

__all__ = ('Model')


class Model(_Model):

    @classmethod
    async def create(cls):
        return await cls._create_table()

    @classmethod
    async def drop(cls):
        return await cls._drop_table()

    @classmethod
    async def add(cls, model):
        return await cls._add(model)

    @classmethod
    async def madd(cls, models):
        return await cls._add_many(models)

    @classmethod
    async def get(cls, pk):
        return await cls._get(pk)

    @classmethod
    async def mget(cls, pks, *fields):
        return await cls._get_many(pks, *fields)

    @classmethod
    def select(cls, *fields):
        return cls._select(*fields)

    @classmethod
    def insert(cls, **values):
        return cls._insert(**values)

    @classmethod
    def minsert(cls, rows, fields=None):
        return cls._insert_many(rows, fields=fields)

    @classmethod
    def update(cls, **values):
        return cls._update(**values)

    @classmethod
    def delete(cls):
        return cls._delete()

    async def save(self):
        return await self._save()

    async def remove(self):
        return await self._remove()
