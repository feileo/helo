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
    def alter(cls):
        return cls._alter()

    @classmethod
    async def get(cls, _id):
        return await cls._get(_id)

    @classmethod
    async def mget(cls, ids, *columns):
        return await cls._get_many(ids, *columns)

    @classmethod
    def add(cls, instance):
        return cls._add(instance)

    @classmethod
    def madd(cls, instances):
        return cls._add_many(instances)

    @classmethod
    def select(cls, *columns, distinct=False):
        return cls._select(*columns, distinct=distinct)

    @classmethod
    def insert(cls, **values):
        return cls._insert(**values)

    @classmethod
    def minsert(cls, rows, columns=None):
        return cls._insert_many(rows, columns=columns)

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
