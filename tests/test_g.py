#  type: ignore
#  pylint: disable=too-many-statements,no-member,unused-variable
"""
Supplementary tests, g etc.
"""

import asyncio
import quart

import pytest

from helo import EnvKey, err, util, G, ENCODING

from . import case


app = quart.Quart(__name__)
db = G(app, True)


@app.route('/api/users')
async def users():
    await case.User.create()

    await case.User.minsert(
        [
            ('u1', 0, 37),
            ('u2', 1, 38),
            ('u3', 1, 39),
            ('u4', 0, 40),
        ],
        [case.User.name, case.User.gender, case.User.age]
    ).do()
    user_list = await case.User.select(
        case.User.id, case.User.name
    ).order_by(
        case.User.id.asc()
    ).all(False)

    await case.User.drop()
    return quart.jsonify(user_list)


@pytest.mark.asyncio
async def test_users():
    async with app.test_client() as c:
        resp = await c.get('api/users')
        redata = await resp.get_json()

        assert isinstance(redata, list)
        assert len(redata) == 4
        assert redata[1]['name'] == 'u2'
        assert redata[2]['name'] == 'u3'

    await db.unbind()


@pytest.mark.asyncio
async def test_helo():
    assert db.state is None

    async with db.binder():
        assert db.isbound is True
        assert db.state == {
            'minsize': 1, 'maxsize': 15,
            'size': 1, 'freesize': 1
        }
        assert await db.create_all(case)
        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 7

        assert await db.drop_all(case)
        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 0

        assert await db.create_tables([case.Author, case.Post, case.Column])
        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 3

        assert await db.drop_tables([case.Author, case.Post, case.Column])
        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 0

        try:
            await db.create_all([case.Author])
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        try:
            await db.drop_all([case.Author])
            assert False, "Should raise TypeError"
        except TypeError:
            pass

    assert db.isbound is False
    try:
        ret = await db.raw('SHOW TABLES;')
        assert False, "Should raise err.UnboundError"
    except err.UnboundError:
        pass
    try:
        await db.bind(password='1234')
        assert False, "Should raise err.OperationalError"
    except err.OperationalError:
        pass

    assert str(db) == repr(db) == '<helo.G object, debug: True>'
    db.set_env_key(None)
    await db.bind(EnvKey.get())
    assert db.isbound is True
    await db.unbind()
    assert db.isbound is False

    db.test = True
    testkey = 'TEST_KEY'
    db1 = G(env_key=testkey)
    assert EnvKey.USER == ''
    assert db1.test is True
    db1.set_env_key(testkey)
    assert EnvKey.USER == testkey
    try:
        async with db1.binder():
            pass
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    db1.set_env_key("")


@pytest.mark.asyncio
async def test_util():
    d = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
    td = util.adict(
        __keys__=('k1', 'k2', 'k3'), __values__=('v1', 'v2', 'v3')
    )
    assert td == d
    td = util.adict(
        __keys__=('k1', 'k2'), __values__=('v1', 'v2', 'v3')
    )
    assert td == {'k1': 'v1', 'k2': 'v2'}
    td.k3 = 'v3'
    assert td == d

    d = td + {'ak1': 'av1'}
    assert isinstance(td, util.adict)
    assert d == util.adict(ak1='av1', k1='v1', k2='v2', k3='v3')
    d = util.adict(k1='v1')
    d += {'ak1': 'av1'}
    assert isinstance(d, util.adict)
    assert d == util.adict(ak1='av1', k1='v1')

    dd = d.copy()
    dd.k4 = 'v4'
    isinstance(dd, util.adict)
    try:
        d = d.v4
        assert False, "Should raise AttributeError"
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
        assert False, "Should raise KeyError"
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
        assert False, "Should raise AttributeError"
    except AttributeError:
        pass

    try:
        @util.asyncinit
        def num():
            return 1
        assert False, "Should raise ValueError"
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
        assert False, "Should raise AttributeError"
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
        assert False, "Should raise AttributeError"
    except AttributeError:
        pass
    assert tsa1.attr == 1
    try:
        assert tsa.c == 2
        assert False, "Should raise AttributeError"
    except AttributeError:
        pass
    del tsa1.attr
    assert getattr(tsa, 'attr', None) is None
    tsa2 = await TSA()
    tsa2.t = 7
    assert tsa.t == 7
    assert tsa1.t == 7

    @util.adictformatter
    def c1():
        return {'k1': 'v1', 'k2': 'v2'}

    @util.adictformatter
    def c2():
        return [
            {'k1': 'v1', 'k2': 'v2'},
            {'k1': 'v1', 'k2': 'v2'},
        ]

    @util.adictformatter
    def c3():
        return [
            ('k1', 'v1', 'k2', 'v2'),
            ('k1', 'v1', 'k2', 'v2'),
        ]

    @util.adictformatter
    async def c4():
        await asyncio.sleep(0.1)
        return 1

    @util.adictformatter
    async def c5():
        await asyncio.sleep(0.1)
        return None

    cv1 = c1()
    assert isinstance(cv1, util.adict)
    assert cv1 == util.adict(**cv1)
    cv2 = c2()
    assert isinstance(cv2, list)
    assert len(cv2) == 2
    assert cv2[0] == util.adict(**cv1)
    assert cv2[1] == util.adict(**cv1)
    try:
        c3()
        assert False, "Should raise TypeError"
    except TypeError:
        pass

    try:
        await c4()
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    assert await c5() is None

    assert 'utf16' in ENCODING
    assert ENCODING.UTF8 == 'utf8'
    o = util.In(['e1', 'e2', 'e3'], 'E')
    assert str(o) == repr(o) == '<E object>'
    assert 'e1' in o
    assert len(o) == 3
