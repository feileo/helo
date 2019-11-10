from datetime import datetime
import pytest

from trod import Trod, err
from trod.types import (
    Auto, Char, VarChar, DateTime, Tinyint,
    Timestamp, ON_CREATE, ON_UPDATE
)
from trod.db import get_db_url

from . import models

db = Trod()


class User(db.Model):

    id = Auto()
    name = VarChar(length=45, comment='username')
    password = VarChar(length=100)
    create_at = Timestamp(default=ON_CREATE)
    update_at = Timestamp(default=ON_UPDATE)


class Role(db.Model):

    id = Auto()
    name = Char(length=100)
    operator = Char(length=50, default='')
    is_deleted = Tinyint(default=0)
    created_at = DateTime(default=datetime.now)

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)

    class Meta:
        table_name = 'role'


class Permission(db.Model):

    id = Auto()
    name = Char(length=100)
    created_at = DateTime(default=datetime.now)


@pytest.mark.asyncio
async def test_trod():
    assert await db.bind(get_db_url())
    assert await db.create_tables([User, Role, Permission])
    assert await db.create_all(models)

    ret = await db.text('SHOW TABLES;')
    assert ret.count == 7

    assert await db.drop_tables([User, Role, Permission])
    assert await db.drop_all(models)

    ret = await db.text('SHOW TABLES;')
    assert ret.count == 0

    assert await db.unbind()
    try:
        ret = await db.text('SHOW TABLES;')
        assert False
    except err.UnboundError:
        pass
