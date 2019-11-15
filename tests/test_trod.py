import asyncio
from datetime import datetime

import pytest

from trod import Trod, Model, err, util
from trod.db import DefaultURL
from trod.types import (
    Auto, Char, VarChar, DateTime, Tinyint,
    Timestamp, ON_CREATE, ON_UPDATE
)

from . import models


class User(Model):

    id = Auto()
    name = VarChar(length=45, comment='username')
    password = VarChar(length=100)
    create_at = Timestamp(default=ON_CREATE)
    update_at = Timestamp(default=ON_UPDATE)


class Role(Model):

    id = Auto()
    name = Char(length=100)
    operator = Char(length=50, default='')
    is_deleted = Tinyint(default=0)
    created_at = DateTime(default=datetime.now)

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)

    class Meta:
        table_name = 'role'


class Permission(Model):

    id = Auto()
    name = Char(length=100)
    created_at = DateTime(default=datetime.now)


@pytest.mark.asyncio
async def test_trod():

    db = Trod()

    async with db.Binder():

        assert db.is_bound is True
        assert await db.create_tables([User, Role, Permission])
        assert await db.create_all(models)

        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 7

        assert await db.drop_tables([User, Role, Permission])
        assert await db.drop_all(models)

        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 0

        try:
            await db.create_all([User])
            assert False
        except TypeError:
            pass
        try:
            await db.create_all([User])
            assert False
        except TypeError:
            pass

    assert db.is_bound is False
    try:
        ret = await db.raw('SHOW TABLES;')
        assert False
    except err.UnboundError:
        pass

    db = Trod('TEST_KEY')
    try:
        async with db.Binder():
            pass
        assert False
    except ValueError:
        pass
    try:
        await db.bind(password='1234')
        assert False
    except err.OperationalError:
        pass
    db.set_url_key(None)
    await db.bind(DefaultURL.get())
    assert db.is_bound is True
    await db.unbind()
    assert db.is_bound is False


@pytest.mark.asyncio
async def test_util():
    d = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
    td = util.tdict(
        __keys__=('k1', 'k2', 'k3'), __values__=('v1', 'v2', 'v3')
    )
    assert td == d
    td = util.tdict(
        __keys__=('k1', 'k2'), __values__=('v1', 'v2', 'v3')
    )
    assert td == {'k1': 'v1', 'k2': 'v2'}
    td.k3 = 'v3'
    assert td == d

    d = td + {'ak1': 'av1'}
    assert isinstance(td, util.tdict)
    assert d == util.tdict(ak1='av1', k1='v1', k2='v2', k3='v3')
    d = util.tdict(k1='v1')
    d += {'ak1': 'av1'}
    assert isinstance(d, util.tdict)
    assert d == util.tdict(ak1='av1', k1='v1')

    dd = d.copy()
    dd.k4 = 'v4'
    isinstance(dd, util.tdict)
    try:
        d = d.v4
        assert False
    except AttributeError:
        pass

    fo = util.FreeObject()
    assert bool(fo) is False
    fo = util.FreeObject(n1='v1', n2='v2', n3='v3')
    assert len(fo) == 3
    assert fo.n1 == 'v1'
    assert fo.n3 == 'v3'
    assert 'n2' in fo
    fo['n4'] = 'v4'
    assert fo['n4'] == 'v4'
    for n in fo:
        if n == 'n3':
            assert fo[n] == 'v3'
    try:
        fo = fo['nn']
        assert False
    except KeyError:
        pass
    fo = fo + util.FreeObject(n5='v5')
    assert isinstance(fo, util.FreeObject)
    fo += {'n6': 'v6'}
    assert fo.n6 == 'v6'
    fo = util.FreeObject(n1='v1', n2='v2')
    assert str(fo) == "{'n1': 'v1', 'n2': 'v2'}"
    assert repr(fo) == "FreeObject({'n1': 'v1', 'n2': 'v2'})"
    del fo['n1']
    assert str(fo) == "{'n2': 'v2'}"
    fonew = fo.as_new(n3='v3')
    assert fonew.n3 == 'v3'
    try:
        fo = fo.v3
        assert False
    except AttributeError:
        pass

    try:
        @util.asyncinit
        def num():
            return 1
        assert False
    except ValueError:
        pass

    @util.singleton
    class TS:
        def __init__(self):
            self.a = 'ok'

    ts = TS()
    assert ts.a == 'ok'
    ts.attr = 1
    ts1 = TS()
    assert ts1.attr == 1
    del ts1.attr
    try:
        assert ts.attr == 1
        assert False
    except AttributeError:
        pass

    @util.singleton_asyncinit
    class TSA:

        async def __init__(self, **kwargs):
            await asyncio.sleep(0.01)
            self.a = 'ok'
            for name in kwargs:
                setattr(self, name, kwargs[name])

    tsa = await TSA(b=1)
    assert tsa.a == 'ok'
    assert tsa.b == 1
    tsa.attr = 1
    assert tsa.attr == 1

    tsa1 = await TSA(c=2)
    try:
        assert tsa1.c == 2
        assert False
    except AttributeError:
        pass
    assert tsa1.attr == 1
    try:
        assert tsa.c == 2
        assert False
    except AttributeError:
        pass
    del tsa1.attr
    assert getattr(tsa, 'attr', None) is None
    tsa2 = await TSA()
    tsa2.t = 7
    assert tsa.t == 7
    assert tsa1.t == 7

    @util.tdictformatter
    def c1():
        return {'k1': 'v1', 'k2': 'v2'}

    @util.tdictformatter
    def c2():
        return [
            {'k1': 'v1', 'k2': 'v2'},
            {'k1': 'v1', 'k2': 'v2'},
        ]

    @util.tdictformatter
    def c3():
        return [
            ('k1', 'v1', 'k2', 'v2'),
            ('k1', 'v1', 'k2', 'v2'),
        ]

    @util.tdictformatter
    async def c4():
        await asyncio.sleep(0.1)
        return 1

    @util.tdictformatter
    async def c5():
        await asyncio.sleep(0.1)
        return None

    cv1 = c1()
    assert isinstance(cv1, util.tdict)
    assert cv1 == util.tdict(**cv1)
    cv2 = c2()
    assert isinstance(cv2, list)
    assert len(cv2) == 2
    assert cv2[0] == util.tdict(**cv1)
    assert cv2[1] == util.tdict(**cv1)
    try:
        c3()
        assert False
    except TypeError:
        pass

    try:
        await c4()
        assert False
    except TypeError:
        pass
    assert await c5() is None
