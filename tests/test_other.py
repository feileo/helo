"""
Supplementary tests
"""
import asyncio
from datetime import datetime

import pytest

from trod import Trod
from trod import err, util, _helper as h, JOINTYPE
from trod.db import DefaultURL
from trod.types import (
    Auto, Int, Char, VarChar, DateTime, Tinyint,
    Timestamp, ON_CREATE, ON_UPDATE, F, SQL,
)

from . import test_model as models

db = Trod()


class User(db.Model):

    id = Auto()
    name = VarChar(length=45, comment='username')
    password = VarChar(length=100)
    create_at = Timestamp(default=ON_CREATE)
    update_at = Timestamp(default=ON_UPDATE)


class Column(db.Model):

    id = Auto()
    name = Char(length=100)
    create_at = Timestamp(default=ON_CREATE)


class Post(db.Model):

    id = Int(primary_key=True, auto=True)
    name = VarChar(length=100)
    author = Int(default=0)
    column = Int(default=0)
    is_deleted = Tinyint(default=0)
    created = DateTime(default=datetime(2019, 10, 10))

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)


class TestImportantQueries:

    def as_query(self, node):
        return node.__query__()

    def test_select(self):
        query = User.select(
            User.id, User.name, F.COUNT(Post.id).as_('pct')
        ).join(
            Post, on=(User.id == Post.author)
        ).where(
            User.name == 'at7h',
            User.id > 3
        ).order_by(
            User.id.desc()
        ).limit(100).offset(1)
        assert self.as_query(query) == h.Query(
            'SELECT `t1`.`id`, `t1`.`name`, COUNT(`t2`.`id`) AS `pct` '
            'FROM `user` AS `t1` '
            'INNER JOIN `post` AS `t2` ON (`t1`.`id` = `t2`.`author`) '
            'WHERE ((`t1`.`name` = %s) AND (`t1`.`id` > %s)) '
            'ORDER BY `t1`.`id` DESC  LIMIT 100 OFFSET 1;',
            params=['at7h', 3]
        )

        query = User.select(
            User.id,
            User.name.as_('username'),
            User.password,
            F.COUNT(Post.id).as_('pct')
        ).join(
            Post, JOINTYPE.LEFT, on=(Post.author == User.id)
        ).group_by(
            User.id, User.name
        ).having(
            User.id.nin_([1, 2, 3])
        )
        assert self.as_query(query) == h.Query(
            'SELECT `t1`.`id`, `t1`.`name` AS `username`, `t1`.`password`,'
            ' COUNT(`t2`.`id`) AS `pct` '
            'FROM `user` AS `t1` '
            'LEFT JOIN `post` AS `t2` ON (`t1`.`id` = `t2`.`author`) '
            'GROUP BY `t1`.`id`, `t1`.`name` '
            'HAVING (`t1`.`id` NOT IN %s);',
            params=[(1, 2, 3)]
        )

        query = Post.select(F.COUNT(SQL('1'))).where(
            Post.created > datetime(2019, 10, 10),
            Post.is_deleted == 0
        ).limit(1)
        assert self.as_query(query) == h.Query(
            'SELECT COUNT(1) FROM `post` AS `t1` WHERE ((`t1`.`created` > %s)'
            ' AND (`t1`.`is_deleted` = %s)) LIMIT 1;',
            params=[datetime(2019, 10, 10, 0, 0), 0]
        )

        query = User.select().where(
            User.id.in_(
                Post.select(Post.author).where(
                    Post.author
                ).where(Post.id.between(10, 100))
            )
        ).order_by(
            User.id.desc()
        ).limit(100)
        assert self.as_query(query) == h.Query(
            'SELECT * FROM `user` AS `t1` WHERE (`t1`.`id` '
            'IN (SELECT `t2`.`author` FROM `post` AS `t2` WHERE '
            '(`t2`.`id` BETWEEN %s AND %s))) ORDER BY '
            '`t1`.`id` DESC  LIMIT 100;',
            params=[10, 100]
        )

    def test_insert(self):
        query = Column.insert(name='c1')
        assert self.as_query(query) == h.Query(
            'INSERT INTO `column` (`name`) VALUES (%s);',
            params=['c1']
        )
        query = Post.insert(name='p1')
        assert self.as_query(query) == h.Query(
            'INSERT INTO `post` (`name`, `author`, `column`, `is_deleted`, `created`)'
            ' VALUES (%s, %s, %s, %s, %s);',
            params=['p1', 0, 0, 0, datetime(2019, 10, 10)]
        )
        q1 = User.insert(name='u1', password='xxxx')
        q2 = User.insert({'name': 'u1', 'password': 'xxxx'})
        assert self.as_query(q1) == self.as_query(q2) == h.Query(
            'INSERT INTO `user` (`name`, `password`) VALUES (%s, %s);',
            params=['u1', 'xxxx']
        )

    def test_minsert(self):
        q1 = User.minsert([
            {'name': 'n1', 'password': 'p1'},
            {'name': 'n2', 'password': 'p2'},
            {'name': 'n3', 'password': 'p3'},
            {'name': 'n4', 'password': 'p4'},
        ])
        q2 = User.minsert(
            [('n1', 'p1'),
                ('n2', 'p2'),
                ('n3', 'p3'),
                ('n4', 'p4')],
            columns=[User.name, User.password]
        )
        assert self.as_query(q1) == self.as_query(q2) == h.Query(
            'INSERT INTO `user` (`name`, `password`) VALUES (%s, %s);',
            params=[('n1', 'p1'), ('n2', 'p2'), ('n3', 'p3'), ('n4', 'p4')]
        )

    def test_insert_from(self):
        select = models.Employee.select(
            models.Employee.id, models.Employee.name
        ).where(models.Employee.id > 10)
        q1 = User.insert_from(select, [User.id, User.name])
        q2 = User.insert_from(select, ['id', 'name'])
        assert self.as_query(q1) == self.as_query(q2) == h.Query(
            'INSERT INTO `user` (`id`, `name`) SELECT `t1`.`id`, '
            '`t1`.`name` FROM `employee` AS `t1` WHERE (`t1`.`id` > %s);',
            params=[10]
        )

    def test_replace(self):
        q1 = User.replace(name='at7h', password='7777')
        q2 = User.replace({'name': 'at7h', 'password': '7777'})
        assert self.as_query(q1) == self.as_query(q2) == h.Query(
            'REPLACE INTO `user` (`id`, `name`, `password`) '
            'VALUES (%s, %s, %s);',
            params=[None, 'at7h', '7777']
        )

    def test_mreplace(self):
        q1 = User.mreplace([
            {'name': 'n1', 'password': 'p1'},
            {'name': 'n2', 'password': 'p2'},
            {'name': 'n3', 'password': 'p3'},
            {'name': 'n4', 'password': 'p4'},
        ])
        q2 = User.mreplace(
            [('n1', 'p1'),
                ('n2', 'p2'),
                ('n3', 'p3'),
                ('n4', 'p4')],
            columns=[User.name, User.password]
        )
        assert self.as_query(q1) == self.as_query(q2) == h.Query(
            'REPLACE INTO `user` (`id`, `name`, `password`) '
            'VALUES (%s, %s, %s);',
            params=[
                (None, 'n1', 'p1'), (None, 'n2', 'p2'),
                (None, 'n3', 'p3'), (None, 'n4', 'p4')
            ])

    def test_update(self):
        query = Post.update(author=2, name='p1').where(
            (Post.author.in_(
                User.select(User.id).where(User.name.startswith('at'))
            ) | Post.column.nin_(
                Column.select(Column.id).where(Column.id >= 100)
            ))
        )
        assert self.as_query(query) == h.Query(
            'UPDATE `post` SET `author` = %s, `name` = %s WHERE '
            '((`author` IN (SELECT `t1`.`id` FROM `user` AS `t1` WHERE '
            '(`t1`.`name` LIKE %s))) OR (`t2`.`column` NOT IN '
            '(SELECT `t3`.`id` FROM `column` AS `t3` WHERE '
            '(`t3`.`id` >= %s))));',
            params=[2, 'p1', "at%", 100]
        )

    def test_update_from(self):
        query = User.update(
            name=Post.name, create_at=Post.created
        ).from_(
            Post
        ).where(
            (User.id == Post.author) | (Post.name == 'xxxx')
        )
        assert self.as_query(query) == h.Query(
            'UPDATE `user` SET `name` = `post`.`name`, '
            '`create_at` = `post`.`created` FROM `post` '
            'WHERE ((`user`.`id` = `post`.`author`) OR '
            '(`post`.`name` = %s));',
            params=['xxxx']
        )

    def test_delete(self):
        query = Post.delete().where(
            Post.author << (
                User.select(User.id).where(
                    User.name.like('at')
                )
            )
        )
        assert self.as_query(query) == h.Query(
            'DELETE FROM `post` WHERE (`author` IN '
            '(SELECT `t1`.`id` FROM `user` AS `t1` '
            'WHERE (`t1`.`name` LIKE %s)));',
            params=['at']
        )


@pytest.mark.asyncio
async def test_trod():

    assert db.state is None

    async with db.Binder():

        assert db.isbound is True
        assert db.state == {
            'minsize': 1, 'maxsize': 15,
            'size': 1, 'freesize': 1
        }
        assert await db.create_tables([User, Post, Column])
        assert await db.create_all(models)

        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 7

        assert await db.drop_tables([User, Post, Column])
        assert await db.drop_all(models)

        ret = await db.raw('SHOW TABLES;')
        assert ret.count == 0

        try:
            await db.create_all([User])
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        try:
            await db.drop_all([User])
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

    db.set_url_key(None)
    await db.bind(DefaultURL.get())
    assert db.isbound is True
    await db.unbind()
    assert db.isbound is False

    db.test = True
    testkey = 'TEST_KEY'
    db1 = Trod(testkey)
    assert db1.test is True
    db1.set_url_key(testkey)
    try:
        async with db1.Binder():
            pass
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    db1.set_url_key(None)


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
        assert False, "Should raise TypeError"
    except TypeError:
        pass

    try:
        await c4()
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    assert await c5() is None
