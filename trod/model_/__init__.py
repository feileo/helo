"""
    trod.model
    ~~~~~~~~~~
"""

from .core import _Model

__all__ = ('Model',)


class Model(_Model):

    @classmethod
    async def create(cls, **options):
        return await cls._create_table(**options)

    @classmethod
    async def drop(cls, **options):
        return await cls._drop_table(**options)

    @classmethod
    def show(cls):
        return cls._show()

    @classmethod
    async def get(cls, _id, tdicts=False):
        return await cls._get(_id, tdicts=tdicts)

    @classmethod
    async def mget(cls, ids, *fields, tdicts=False):
        return await cls._get_many(ids, *fields, tdicts=tdicts)

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
