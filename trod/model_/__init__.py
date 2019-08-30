"""
    trod.model
    ~~~~~~~~~~
"""

from . import core

__all__ = ('TrodModel',)


class TrodModel(core.Model):

    @classmethod
    async def create(cls, **options):
        return await core.Api.create_table(cls, **options)

    @classmethod
    async def drop(cls, **options):
        return await core.Api.drop_table(cls, **options)

    @classmethod
    def alter(cls):
        return core.Api.alter(cls)

    @classmethod
    def show(cls):
        return core.Api.show(cls)

    @classmethod
    async def get(cls, _id):
        return await core.Api.get(cls, _id)

    @classmethod
    async def mget(cls, ids, *columns):
        return await core.Api.get_many(cls, ids, *columns)

    @classmethod
    def add(cls, instance):
        return core.Api.add(cls, instance)

    @classmethod
    def madd(cls, instances):
        return core.Api.add_many(cls, instances)

    @classmethod
    def select(cls, *columns, distinct=False):
        return core.Api.select(cls, *columns, distinct=distinct)

    @classmethod
    def insert(cls, **values):
        return core.Api.insert(cls, **values)

    @classmethod
    def minsert(cls, rows, columns=None):
        return core.Api.insert_many(cls, rows, columns=columns)

    @classmethod
    def update(cls, **values):
        return core.Api.update(cls, **values)

    @classmethod
    def delete(cls):
        return core.Api.delete(cls)

    @classmethod
    def replace(cls, **values):
        return core.Api.replace(cls, **values)

    async def save(self):
        return await core.Api.save(self)

    async def remove(self):
        return await core.Api.remove(self)
