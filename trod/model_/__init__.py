from trod.model_.core import _Model

__all__ = ('Model',)


class Model(_Model):

    @classmethod
    async def create(cls):
        return await cls._create_table()

    @classmethod
    async def drop(cls):
        return await cls._drop_table()

    @classmethod
    async def show(cls):
        return await cls._show()

    @classmethod
    async def exist(cls):
        return await cls._exist()

    @classmethod
    async def get(cls, pk, tdicts=False):
        return await cls._get(pk, tdicts=tdicts)

    @classmethod
    async def mget(cls, pks, *fields, tdicts=False):
        return await cls._get_many(pks, *fields, tdicts=tdicts)

    @classmethod
    def add(cls, instance):
        return cls._add(instance)

    @classmethod
    def madd(cls, instances):
        return cls._add_many(instances)

    @classmethod
    def select(cls, *fields, distinct=False):
        return cls._select(*fields, distinct=distinct)

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

    @classmethod
    def replace(cls, **values):
        return cls._replace(**values)

    async def save(self):
        return await self._save()

    async def remove(self):
        return await self._remove()
