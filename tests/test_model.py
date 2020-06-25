#  type: ignore
#  pylint: disable=too-many-statements,too-many-branches,unused-variable
"""
Tests for model module
"""

import asyncio
import datetime

import pytest

from helo import (
    db, types as t, err, util, _builder,
    Model, JOINTYPE, ENCODING, ENGINE
)

from .case import People, Employee, User, Role, deltanow


class TestModel:

    def setup(self):
        async def init():
            await db.binding(db.EnvKey.get(), debug=True)
            await People.create()
            await Employee.create()
            await User.create()
            await Role.create()

        asyncio.get_event_loop().run_until_complete(init())

    def teardown(self):
        async def clear():
            await People.drop()
            await Employee.drop()
            await User.drop()
            await Role.drop()
            await db.unbinding()

        asyncio.get_event_loop().run_until_complete(clear())

    @pytest.mark.asyncio
    async def tests(self):
        await self.for_ddl()
        await self.for_save_and_remove()
        await self.for_get()
        await self.for_mget()
        await self.for_add()
        await self.for_madd()
        await self.for_set()
        await self.for_select()
        await self.for_insert()
        await self.for_minsert()
        await self.for_insert_from()
        await self.for_update()
        await self.for_update_from()
        await self.for_replace()
        await self.for_mreplace()
        await self.for_aiter()

    async def for_ddl(self):
        try:
            await Model.create()
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass
        try:
            await Model.drop()
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass

        csql = await People.show().create_syntax()
        assert (
            "CREATE TABLE `people` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            "  `name` varchar(45) DEFAULT NULL,\n"
            "  `gender` tinyint(1) unsigned DEFAULT NULL,\n"
            "  `age` tinyint(4) unsigned DEFAULT NULL,\n"
        ) in csql
        csql = await Employee.show().create_syntax()
        assert (
            "  `salary` float DEFAULT NULL,\n"
            "  `departmentid` int(11) DEFAULT NULL,\n"
            "  `phone` varchar(254) DEFAULT '',\n"
            "  `email` varchar(100) DEFAULT '',\n"
            "  PRIMARY KEY (`id`),\n"
            "  KEY `idx_age_salary` (`age`,`salary`)\n"
        ) in csql
        csql = await User.show().create_syntax()
        assert (
            "  `nickname` varchar(100) DEFAULT NULL,\n"
            "  `pwd` varchar(254) DEFAULT NULL,\n"
            "  `role` int(11) DEFAULT '0',\n"
            "  `loginat` datetime DEFAULT NULL,\n"
            "  PRIMARY KEY (`id`),\n"
            "  UNIQUE KEY `unidx_nickname` (`nickname`),\n"
            "  KEY `idx_name` (`name`)\n"
        ) in csql
        assert (await User.show().columns())[0]['Field'] == 'id'
        assert len(await User.show().indexes()) == 3
        assert (await User.show().indexes())[0]['Key_name'] == 'PRIMARY'
        assert (await User.show().indexes())[1]['Key_name'] == 'unidx_nickname'
        assert (await User.show().indexes())[1]['Column_name'] == 'nickname'
        assert str(User.show()) == '<Show object for <Table `helo`.`user_`>>'

    async def for_save_and_remove(self):
        user = User(name='at7h', gender=0, age=25)
        uid = await user.save()
        assert uid == 1
        assert user.id == 1
        assert user.password is None
        assert user.lastlogin is None

        user = User(
            name='mejor', gender=1, age=22,
            password='xxxx', nickname='huhu',
            lastlogin=datetime.datetime.now()
        )
        uid = await user.save()
        assert uid == 2
        assert user.id == 2
        assert user.age == 22
        assert user.nickname == 'huhu'
        assert user.password == 'xxxx'
        assert isinstance(user.lastlogin, datetime.datetime)

        user.age = 18
        user.gender = 0
        uid = await user.save()
        assert uid == 2
        user = await User.get(2)
        assert user.id == 2
        assert user.nickname == 'huhu'
        assert user.password == 'xxxx'
        assert user.age == 18
        assert user.gender == 0

        try:
            user = User(
                id=3, name='mejor', gender=1, age=22,
                password='xxxx', nickname='huhu',
                lastlogin=datetime.datetime.now()
            )
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass

        user = User(
            name='keyoxu', gender=1, age=28,
            password='mmmm', nickname='jiajia',
            lastlogin=datetime.datetime.now()
        )
        uid = await user.save()
        assert uid == 3
        ret = await user.remove()
        assert ret == 1
        user = User(name='n')
        try:
            await user.remove()
            assert False, "Should raise RuntimeError"
        except RuntimeError:
            pass

        employee = Employee(name='at7h', email='g@at7h.com')
        assert (await employee.save()) == 1
        try:
            await Employee(email='g..@a.c').save()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_get(self):
        user = User(
            name='keyoxu', gender=1, age=28,
            password='mmmm', nickname='jiajia',
            lastlogin=datetime.datetime.now()
        )
        uid = await user.save()
        assert uid == 4
        user = await User.get(uid)
        assert isinstance(user, User)
        assert user.name == 'keyoxu'
        assert user.gender == 1
        assert user.age == 28
        assert user.password == 'mmmm'
        assert user.nickname == 'jiajia'
        assert isinstance(user.lastlogin, datetime.datetime)
        assert isinstance(user.create_at, datetime.datetime)
        user = await User.get(User.id == uid)
        assert isinstance(user, User)
        assert user.name == user.name
        assert user.gender == user.gender
        assert user.age == user.age
        assert user.password == user.password
        assert user.nickname == user.nickname
        assert isinstance(user.lastlogin, datetime.datetime)
        assert isinstance(user.create_at, datetime.datetime)
        user = await User.get(User.name == user.name)
        assert user.name == 'keyoxu'
        try:
            user.email = ''
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass
        try:
            assert user.email
            assert False, "Should raise AttributeError"
        except AttributeError:
            pass

        try:
            assert user.email
            assert False, "Should raise AttributeError"
        except AttributeError:
            pass

        user = await User.get(10000)
        assert user is None
        assert await User.get(None) is None
        assert await User.get('') is None
        assert await User.get({}) is None
        assert await User.get(()) is None
        assert await User.get([]) is None

    async def for_mget(self):
        # no 3
        user_ids = [1, 2, 3, 4]
        users = await User.mget(user_ids)
        assert isinstance(users, db.FetchResult)
        assert repr(users) == (
            "[<User object at 1>, <User object at 2>, <User object at 4>]")
        assert users.count == 3
        assert isinstance(users[0], User)
        assert isinstance(users[2], User)
        assert users[0].id == 1
        assert users[0].name == 'at7h'
        assert users[0].age == 25
        assert isinstance(users[0].update_at, datetime.datetime)
        assert users[1].id == 2
        assert users[1].name == 'mejor'
        assert users[1].nickname == 'huhu'
        assert users[1].gender == 0
        assert users[1].age == 18
        assert isinstance(users[1].create_at, datetime.datetime)
        assert users[2].name == 'keyoxu'
        assert users[2].password == 'mmmm'
        assert users[2].nickname == 'jiajia'
        assert isinstance(users[2].lastlogin, datetime.datetime)

        users = await User.mget(
            user_ids,
            columns=[User.name, User.age, User.password],
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert isinstance(users[0], User)
        assert isinstance(users[2], User)
        assert users[0].id is None
        assert users[0].name == 'at7h'
        assert users[0].age == 25
        assert users[0].password is None
        assert users[0].lastlogin is None
        assert users[0].update_at is None
        assert users[1].id is None
        assert users[1].name == 'mejor'
        assert users[1].age == 18
        assert users[1].nickname is None
        assert users[1].lastlogin is None
        assert users[0].create_at is None
        assert users[2].id is None
        assert users[2].name == 'keyoxu'
        assert users[2].create_at is None
        assert users[2].age == 28

        users = await User.mget(
            User.id.in_(user_ids),
            columns=[User.name, User.age, User.password],
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert users[0].id is None
        assert users[0].lastlogin is None
        assert users[0].name == 'at7h'
        assert users[0].age == 25
        assert users[1].name == 'mejor'
        assert users[1].age == 18
        assert users[1].nickname is None
        assert users[1].lastlogin is None
        assert users[2].create_at is None
        assert users[2].name == 'keyoxu'
        assert users[2].age == 28

        users = await User.mget(
            User.name.startswith('at')
        )
        assert users.count == 1
        assert users[0].name == 'at7h'

        try:
            assert await User.mget({'id': 1})
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        try:
            assert await User.mget([])
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_add(self):
        uid = await User.add(
            name='add', gender=1, age=45, nickname='passadd'
        )
        assert uid == 5
        user = await User.get(uid)
        assert user.password is None
        assert user.name == 'add'
        assert user.age == 45
        assert user.nickname == 'passadd'
        assert isinstance(user.lastlogin, datetime.datetime)
        assert isinstance(user.create_at, datetime.datetime)
        assert isinstance(user.update_at, datetime.datetime)

        user.name = 'add1'
        user.nickname = 'addn1'
        user_dict = user.__self__
        try:
            uid = await User.add(user_dict)
            assert False, "Should raise NotAllowedError"
        except err.NotAllowedError:
            pass
        del user.id
        user_dict = user.__self__
        uid = await User.add(user_dict)
        assert uid == 6
        user = await User.get(6)
        assert user.name == 'add1'

        uid = await User.add({
            'name': 'add2',
            'nickname': 'add2n',
            'password': 'p2'
        })
        assert uid == 7
        user = await User.get(7)
        assert user.password == 'p2'
        try:
            await User.add({'n': 1})
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.add(None)
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.add({})
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        uid = await User.add({'name': 'nnnn'})
        assert uid == 8
        user = await User.get(8)
        assert user.name == 'nnnn'
        assert user.nickname is None
        assert user.age is None
        try:
            assert await User.add(None)
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        try:
            await Employee.add(email='gg')
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_madd(self):
        user1 = User(name='user1', age=1)
        user2 = User(name='user2', age=2)
        user3 = User(name='user3', age=3)
        user4 = User(name='user4', age='4')
        affected = await User.madd([user1, user2, user3, user4])
        assert affected == 4
        user = await User.get(12)
        assert user.name == 'user4'
        assert user.age == 4
        user.name = 'user4forsave'
        uid = await user.save()
        user = await User.get(uid)
        assert user.id == 12
        assert user.name == 'user4forsave'

        users = [
            {'name': 'user5'},
            {'name': 'user6'},
            {'name': 'user7'},
            {'name': 'user8'},
        ]
        affected = await User.madd(users)
        assert affected == 4
        user = await User.get(16)
        assert user.name == 'user8'
        assert user.age is None

        try:
            assert await User.madd([])
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        users = [
            ('name', 'user5'),
            ('name', 'user6'),
            ('name', 'user7'),
            ('name', 'user8'),
        ]
        try:
            assert await User.madd(users)
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        roles = [
            {'name': 'Manager'},
            {'name': 'admin'},
            {'name': 'developer'},
            {'name': 'member'},
        ]
        ret = await Role.madd(roles)
        assert ret == 4

    async def for_set(self):
        ret = await User.set(16, name='user8forset', age=90)
        assert ret == 1
        user = await User.get(16)
        assert user.name == 'user8forset'
        assert user.age == 90

        try:
            assert await User.set(1, email='')
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            assert await User.set(1)
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        employee = await Employee.get(1)
        assert employee.email == 'g@at7h.com'
        try:
            await employee.set(1, email='gg')
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_select(self):
        users = await User.select().all()
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert isinstance(users[1], User)
        assert users[0].id == 1
        assert users[0].name == 'at7h'
        assert users[1].id == 2
        assert users[2].id == 4
        assert users[2].name == 'keyoxu'
        assert users[11].name == 'user5'
        assert repr(users[10]) == '<User object at 12>'
        assert str(users[10]) == '<User object at 12>'
        assert users[14].name == 'user8forset'

        users = await User.select(
            User.id, User.name
        ).where(User.id > 10).all()
        assert users.count == 6
        assert isinstance(users[0], User)
        assert users[1].id == 12
        assert users[1].name == 'user4forsave'
        assert users[1].lastlogin is None
        assert users[2].password is None
        assert users[3].id == 14
        assert users[3].name == 'user6'

        users = await User.select(
            User.id.as_('uid'), User.name.as_('username')
        ).where(User.id < 3).all()
        assert users.count == 2
        assert isinstance(users[0], User)
        assert users[0].id == 1
        assert users[0].name == 'at7h'
        users = await User.select(
            User.id.as_('uid'), User.name.as_('username')
        ).where(User.id < 3).all(False)
        assert users.count == 2
        assert isinstance(users[0], util.adict)
        assert users[0].id == 1
        assert users[0].name == 'at7h'

        users = await User.select().all()
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert users[-1].id == 16
        assert users[-2].name == 'user7'

        users = await User.select().paginate(1, 100, wrap=False)
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert isinstance(users[-1], util.adict)
        assert users[6].id == 8
        assert users[9].age == 3
        users_ = await User.select().paginate(1, 100, wrap=False)
        assert users == users_

        try:
            await User.select().order_by(User.id.desc()).rows(-10)
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.select().order_by(User.id.desc()).paginate(-1)
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.select().order_by(User.id.desc()).paginate(1, -1)
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        user = await User.select().order_by(User.id.desc()).first()
        assert isinstance(user, User)
        assert user.id == 16
        assert user.name == 'user8forset'
        user = await (User.select()
                      .order_by(User.id.desc())
                      .first(False))
        assert isinstance(user, util.adict)
        assert user.id == 16
        assert user.name == 'user8forset'
        users = await (User.select()
                       .order_by(User.id.desc())
                       .limit(1)
                       .offset(4)
                       .all(False))
        user = users[0]
        assert isinstance(user, util.adict)
        assert user.id == 12
        assert user.name == 'user4forsave'
        assert user.age == 4
        users = await (User.select()
                       .where(User.id.between(100, 200))
                       .all())
        assert isinstance(users, db.FetchResult)
        assert users.count == 0
        assert users == []
        users = await (User.select()
                       .where(User.id.between(100, 200))
                       .rows(1))
        assert isinstance(users, db.FetchResult)
        assert users.count == 0
        assert users == []
        users = await (User.select()
                       .where(User.id.between(100, 200))
                       .order_by(User.id.desc())
                       .paginate(1))
        assert isinstance(users, db.FetchResult)
        assert users.count == 0
        assert users == []
        users = await (User.select()
                       .where(User.id.between(100, 200))
                       .order_by(User.create_at.desc())
                       .first())
        assert users is None
        users = await (User.select()
                       .where(User.id.between(100, 200))
                       .get())
        assert users is None

        user = await (User.select()
                      .order_by(User.age.desc())
                      .get())
        assert isinstance(user, User)
        assert user.age == 90
        assert user.id == 16
        assert user.name == 'user8forset'

        users = await User.select().where(
            User.lastlogin < deltanow(1),
            User.age < 25,
            User.name != 'at7h'
        ).order_by(
            User.age
        ).rows(10)
        assert users.count == 5
        assert users[1].name == 'user2'
        assert users[4].name == 'mejor'

        users = await User.select().where(
            (User.password == 'xxxx') | (User.name.startswith('at'))
        ).all()
        assert users.count == 2
        assert users[0].id == 1
        assert users[0].gender == 0
        assert users[0].name == 'at7h'
        assert users[1].name == 'mejor'
        users = await User.select().where(
            util.or_(User.password == 'xxxx', User.name.startswith('at'))
        ).rows(10, 0)
        assert users.count == 2
        assert users[0].id == 1
        assert users[0].gender == 0
        assert users[0].name == 'at7h'
        assert users[1].name == 'mejor'

        users = await User.select().paginate(1, 5)
        assert users.count == 5
        assert users[-1].id == 6
        users = await User.select().paginate(2, 5)
        assert users.count == 5
        assert users[-1].id == 11

        users = await User.select(
            User.gender, t.F.count(t.SQL('1')).as_('num')
        ).group_by(
            User.gender
        ).all(False)
        assert users.count == 3
        assert isinstance(users[0], util.adict)
        for user in users:
            if user.gender is None:
                assert user.num == 10
            elif user.gender == 0:
                assert user.num == 2
            elif user.gender == 1:
                assert user.num == 3

        users = await User.select(
            User.gender, t.F.count(t.SQL('1')).as_('num')
        ).group_by(
            User.gender
        ).all()
        assert users.count == 3
        assert isinstance(users[0], util.adict)

        users = await User.select(
            User.age, t.F.count(t.SQL('*')).as_('num')
        ).group_by(
            User.age
        ).having(
            User.age >= 10
        ).all()
        assert users.count == 5
        for user in users:
            if user.age in (18, 25, 28, 90):
                assert user.num == 1
            elif user.age == 45:
                assert user.num == 2

        users = await User.select().order_by(
            User.name).limit(10).offset(7).all()
        assert users.count == 8
        assert isinstance(users[0], User)
        assert users[0].id == 9
        assert users[0].name == 'user1'

        user = await User.select().where(
            User.name == 'xxxx'
        ).exist()
        assert user is False
        user = await User.select().where(
            User.name == 'at7h'
        ).exist()
        assert user is True
        user = await User.select().where(
            User.age > 10
        ).exist()
        assert user is True
        user = await User.select().where(
            User.age > 80
        ).exist()
        assert user is True
        user = await User.select().where(
            User.age > 90
        ).exist()
        assert user is False
        user = await User.select().where(
            User.id.in_(
                People.select(People.id).where(
                    People.id > 1)
            )
        ).all()
        assert user.count == 0
        user = await User.select().where(
            User.id.in_(
                Employee.select(Employee.id).where(
                    Employee.name.nin_(
                        People.select(People.name).where(
                            People.id > 1)
                    )
                )
            )
        ).all()
        assert user.count == 1

        user_count = await User.select().count()
        assert user_count == 15
        ret = await User.select().join(
            Employee
        ).all()
        assert ret.count == 15

        ret = await User.select(
            User.id, Employee.id
        ).join(
            Employee, on=(User.name == Employee.name)
        ).all()
        assert ret.count == 1
        assert ret[0]['id'] == 1
        assert ret[0]['t2.id'] == 1

        assert (await Employee(name='mejor').save()) == 2

        ret = await User.select(
            User.id, Employee.id
        ).join(
            Employee, on=(User.name == Employee.name)
        ).all()
        assert ret.count == 2
        assert ret[1]['id'] == 2
        assert ret[1]['t2.id'] == 2

        ret = await User.select(
            User.id, Employee.id
        ).join(
            Employee, JOINTYPE.LEFT, on=(User.name == Employee.name)
        ).all()
        assert ret.count == 15

        ret = await User.select(
            User.id, User.name, Employee.id
        ).join(
            Employee, JOINTYPE.RIGHT, on=(User.name == Employee.name)
        ).all()
        assert ret.count == 2
        assert ret[0].name == 'at7h'
        assert ret[1].name == 'mejor'

        try:
            await User.select().group_by()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.select().group_by(1)
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        try:
            await User.select().order_by()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await User.select().order_by(1)
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        try:
            await User.select().offset(1)
            assert False, "Should raise err.ProgrammingError"
        except err.ProgrammingError:
            pass

        s = User.select(User.name).where(User.name == 'at7h')
        q = _builder.Query(
            'SELECT `t1`.`name` FROM `helo`.`user_` AS `t1` '
            'WHERE (`t1`.`name` = %s);',
            params=['at7h']
        )
        assert str(s) == str(q)
        assert repr(s) == repr(q)

        # test funcs and scalar
        user_count = await User.select().count()
        assert user_count == 15

        user_count = await User.select().where(
            User.age > 25
        ).count()
        assert user_count == 4

        user_count = await User.select().where(
            User.id.in_(list(range(10)))
        ).count()
        assert user_count == 8

        users = await User.select(User.age).all()
        sum_age = 0
        for u in users:
            if u.age is not None:
                sum_age += u.age
        assert sum_age == 261

        age_sum = await User.select(
            t.F.sum(User.age)
        ).scalar()
        assert age_sum == 261

        user_count = await User.select(
            t.F.count(User.age).as_('age_count')
        ).where(
            User.age > 25
        ).get()
        assert user_count == {'age_count': 4}

        user_count = await User.select(
            t.F.count(User.age)
        ).where(
            User.gender == 0
        ).scalar()
        assert user_count == 2

        user_count = await User.select(
            t.F.max(User.age)
        ).where(
            User.age > 25
        ).scalar()
        assert user_count == 90

        age_max = await User.select(
            t.F.sum(User.age)
        ).where(
            User.age > 25
        ).scalar()
        assert age_max == 208

    async def for_insert(self):
        ret = await User.insert(
            name='iii1', gender=1, age=20,
            nickname='nnn1', password='ppp1'
        ).do()
        assert ret.affected == 1
        assert ret.last_id == 17
        user = await User.get(ret.last_id)
        assert user.password == 'ppp1'
        assert isinstance(user.lastlogin, datetime.datetime)

        employee = {
            'name': 'eee1', 'gender': 1, 'age': 40,
            'salary': 10000, 'departmentid': 17,
            'phone': 2312421421,
        }
        ret = await Employee.insert(employee).do()
        assert ret.affected == 1
        assert ret.last_id == 3
        em = await Employee.select().where(
            Employee.name == employee['name']
        ).all()
        assert em.count == 1
        assert em[0].name == employee['name']
        assert em[0].salary == employee['salary']
        try:
            employee = {'id': 2}
            ret = await Employee.insert(employee).do()
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass
        ret = await People.insert(name='ip1', age=10).do()
        assert ret.affected == 1
        assert ret.last_id == 1
        employee = {
            'name': 'eee1', 'gender': 1, 'age': 40,
            'salary': 10000, 'departmentid': 17,
            'phone': 2312421421, 'unkown': 'xx'
        }
        try:
            await People.insert(gae='str_age').do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            await Employee.insert(email='..@..').do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.insert(employee).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.insert({}).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_minsert(self):
        people_list = [
            ('np1', 0, 37),
            ('np2', 1, 38),
            ('np3', 1, 39),
            ('np4', 0, 40),
        ]
        try:
            ret = await People.minsert(people_list).do()
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        ret = await People.minsert(
            people_list, columns=[People.name, People.gender, People.age]
        ).do()
        assert ret.affected == 4
        assert ret.last_id == 2
        people = await People.select().order_by(
            People.age.desc()
        ).all()
        assert people.count == 5
        assert people[-1].age == 10

        try:
            ret = await Employee.minsert(
                people_list, columns=[User.password]
            ).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        people_list = [
            ('np1',),
            ('np2',),
            ('np3',),
            ('np4',),
        ]
        try:
            ret = await Employee.minsert(
                people_list, columns=[People.name, People.gender]
            ).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        people_list = [
            (1, 'np1', 0, 37),
            (2, 'np2', 1, 38),
        ]
        try:
            ret = await People.minsert(
                people_list,
                columns=[People.id, People.name, People.gender, People.age]
            ).do()
            assert False, "Should raise err.NotAllowedError"
        except err.NotAllowedError:
            pass

        people_list = [
            ('np1', 0, 37),
            {'name': 'eee1', 'gender': 1, 'age': 40},
        ]
        try:
            ret = await People.minsert(
                people_list,
                columns=(People.name, People.gender, People.age)
            )
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.minsert(
                [('np1', 0, 37)],
                columns=[1]
            ).do()
            assert False
        except TypeError:
            pass
        try:
            ret = await Employee.minsert(
                [{'unknow': 'xxxx'}]
            ).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.minsert([]).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_insert_from(self):
        select = Employee.select(Employee.name).where(Employee.id < 3)
        ems = await select.all()
        assert ems.count == 2
        try:
            User.insert_from(select, [])
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            User.insert_from(None, [User.name])
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            User.insert_from(1, [User.name])
            assert False, "Should raise TypeError"
        except TypeError:
            pass
        ret = await User.insert_from(select, [User.name]).do()
        assert ret.last_id == 18
        assert ret.affected == 2
        users = await User.select().where(
            User.id.in_([ret.last_id, ret.last_id+1])
        ).all()
        assert ems[0].name == users[0].name == 'at7h'
        assert ems[1].name == users[1].name == 'mejor'

        ret = await User.insert_from(select, ['name']).do()
        assert ret.last_id == 21
        assert ret.affected == 2
        users = await User.select().where(
            User.id.in_([ret.last_id, ret.last_id+1])
        ).all()
        assert ems[0].name == users[0].name == 'at7h'
        assert ems[1].name == users[1].name == 'mejor'

    async def for_update(self):
        ret = await People.update(
            name='up1'
        ).where(
            People.name == 'np1'
        ).do()
        assert ret.affected == 1
        people = await People.get(2)
        assert people.name == 'up1'
        ret = await People.insert(name='up2', age=23).do()
        assert ret.affected == 1
        assert ret.last_id == 6
        ret = await People.update(
            age=29
        ).where(
            People.id == ret.last_id
        ).do()
        people = await People.get(6)
        assert people.name == 'up2'
        assert people.age == 29

        dt = "2019-10-10 10:10:10"
        dtpformat = "%Y-%m-%d %H:%M:%S"
        ret = await People.update(
            create_at=dt
        ).where(
            People.id == 6
        ).do()
        people = await People.get(6)
        assert people.create_at == datetime.datetime.strptime(dt, dtpformat)
        ret = await User.update(
            lastlogin=dt
        ).where(
            User.id == 1
        ).do()
        user = await User.get(1)
        assert user.lastlogin == datetime.datetime.strptime(dt, dtpformat)

        try:
            ret = await Employee.update().do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.update(
                email='..@..'
            ).where(Employee.id == 1).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.update(
                url='https://abc.com'
            ).where(Employee.id == 1).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_delete(self):
        ret = await People.delete().where(
            People.id == 6
        ).limit(1).do()
        assert ret.affected == 1
        assert ret.last_id == 0
        people = await People.get(6)
        assert people is None
        try:
            await People.delete().do()
            assert False, "Should raise err.DangerousOperation"
        except err.DangerousOperation:
            pass

    async def for_update_from(self):
        user_id, user_name = 18, 'bobo'
        await User.set(user_id, name=user_name)
        user = await User.get(user_id)
        assert user.name == user_name
        employee_id, employee_name = 1, 'at7h'
        employee = await Employee.get(employee_id)
        assert employee.name == employee_name

        try:
            ret = await User.update(
                name=Employee.name
            ).from_(
                Employee
            ).where(
                Employee.id == User.id
            ).do()
            assert ret.affected == 1
            user = await User.get(user_id)
            assert user.name == employee_name
            assert False, "MySQL does not support this syntax"
        except err.ProgrammingError:
            pass

    async def for_replace(self):
        user_id = 18
        user = await User.get(user_id)
        assert user.name == 'bobo'
        assert user.age is None
        ret = await User.replace(id=user_id, name='rp1', age=78).do()
        user = await User.get(user_id)
        assert user.name == 'rp1'
        assert user.age == 78
        assert ret.affected == 2
        assert ret.last_id == 18

        user = await User.get(ret.last_id)
        assert user.name == 'rp1'
        assert user.age == 78
        ret = await User.replace(id=ret.last_id, name='rp1forreplace').do()
        user = await User.get(ret.last_id)
        assert user.name == 'rp1forreplace'
        try:
            ret = await Employee.replace({}).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_mreplace(self):
        people_list = [
            (0, 37),
            (1, 38),
            (1, 39),
            (0, 40),
        ]
        try:
            ret = await People.mreplace(people_list).do()
            assert False, "Should raise TypeError"
        except TypeError:
            pass

        try:
            ret = await People.mreplace(
                people_list, columns=(People.gender, People.age)).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        people_list = [
            (1, 'rnp1', 0, 37),
            (2, 'rnp2', 1, 38),
            (3, 'rnp3', 1, 39),
            (4, 'rnp4', 0, 40),
        ]
        ret = await People.mreplace(
            people_list,
            columns=[People.id, People.name, People.gender, People.age]
        ).do()
        assert ret.affected == 8
        assert ret.last_id == 4
        people = await People.select().order_by(
            People.age.desc()
        ).all()
        assert people.count == 6
        assert people[-1].age == 29

        people_list = [
            ('rnp1', 0, 57),
            ('rnp2', 1, 58),
        ]
        ret = await People.mreplace(
            people_list,
            columns=[People.name, People.gender, People.age]
        ).do()
        people = await People.get(ret.last_id)
        assert people.age == 57

        people_list = [
            ('np1', 0, 37),
            {'name': 'eee1', 'gender': 1, 'age': 40},
        ]
        try:
            ret = await People.mreplace(
                people_list,
                columns=[People.name, People.gender, People.age]
            )
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        try:
            ret = await Employee.mreplace([]).do()
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    async def for_aiter(self):
        allc = await User.select().count()
        count = 0
        async for user in User:
            assert isinstance(user, User)
            if user.id == 1:
                assert user.name == 'at7h'
            count += 1
        assert count == allc

        count = 0
        async for user in User.select():
            assert isinstance(user, User)
            if user.id == 1:
                assert user.name == 'at7h'
            count += 1
        assert count == allc

        for i in range(20, 270):
            await User.insert(password=i).do()

        allc = await User.select().count()
        count = 0
        async for user in User:
            assert isinstance(user, User)
            if user.id == 1:
                assert user.name == 'at7h'
            count += 1
        assert count == allc

        try:
            assert User[1]
            assert False, "Should raise NotImplementedError"
        except NotImplementedError:
            pass
        try:
            assert user in User
            assert False, "Should raise NotImplementedError"
        except NotImplementedError:
            pass


def test_values():
    from helo.model import ValuesMatch
    try:
        ValuesMatch(('1', '2', 3))
        assert False, "Should raise ValueError"
    except ValueError:
        pass


def test_model():
    from helo.model import get_attrs, get_table

    assert isinstance(People.id, t.Auto)
    assert _builder.parse(People.name.__def__()).sql == (
        '`name` varchar(45) DEFAULT NULL;')
    table = get_table(People)
    assert table.name == 'people'
    assert isinstance(table.indexes, list)
    assert len(table.indexes) == 1
    assert table.indexes[0].name == 'idx_name'
    assert table.db is None
    assert table.auto_increment == 1
    assert table.engine == ENGINE.innodb
    assert table.charset == ENCODING.UTF8MB4
    assert table.comment == ''
    assert table.primary.auto is True
    assert get_attrs(People) == ({
        'age': 'age',
        'create_at': 'create_at',
        'gender': 'gender',
        'id': 'id',
        'name': 'name',
        'update_at': 'update_at'}
    )
    assert isinstance(People.Meta.indexes, list)

    table = get_table(Employee)
    assert table.name == 'employee'
    assert isinstance(table.indexes, list)
    assert len(table.indexes) == 1
    assert table.indexes[0].name == 'idx_age_salary'
    assert table.comment == ''
    assert table.primary.auto is True
    assert get_attrs(Employee) == ({
        'age': 'age',
        'create_at': 'create_at',
        'departmentid': 'departmentid',
        'gender': 'gender',
        'id': 'id',
        'name': 'name',
        'phone': 'phone',
        'email': 'email',
        'salary': 'salary',
        'update_at': 'update_at'
    })

    table = get_table(User)
    assert isinstance(table.indexes, tuple)
    assert len(table.indexes) == 2
    assert table.name == 'user_'
    assert table.indexes[0].name == 'idx_name'
    assert table.indexes[1].name == 'unidx_nickname'
    assert get_attrs(User) == ({
        'age': 'age',
        'create_at': 'create_at',
        'gender': 'gender',
        'id': 'id',
        'loginat': 'lastlogin',
        'name': 'name',
        'nickname': 'nickname',
        'pwd': 'password',
        'role': 'role',
        'update_at': 'update_at'
    })
    assert len(get_table(User).fields_dict) == 10
    assert isinstance(User.nickname, t.VarChar)
    assert isinstance(User.password, t.VarChar)
    assert isinstance(User.lastlogin, t.DateTime)
    assert User.Meta.name == 'user_'
    assert isinstance(User.Meta.indexes, tuple)

    try:
        People.id = 12123123
        People.name = 'at7h'
        del People.name
        assert False, 'Should raise NotAllowedError'
    except err.NotAllowedError:
        pass

    try:
        del People.name
        assert False, 'Should raise NotAllowedError'
    except err.NotAllowedError:
        pass

    try:
        class TM(People):
            id_ = t.BigAuto()
        assert False, "Should raise DuplicatePKError"
    except err.DuplicatePKError:
        pass

    try:
        class TM1(Model):
            tp = t.Int()
        assert False, "Should raise NoPKError"
    except err.NoPKError:
        pass
    try:
        class TM2(Model):
            class Meta:
                indexes = 'idx'
            tp = t.Int()
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    try:
        class TM3(Model):
            class Meta:
                indexes = ['idx', 1]

            tp = t.Int()
        assert False, "Should raise TypeError"
    except TypeError:
        pass

    class TM4(Model):
        pk = t.Auto()
    assert get_table(TM4).name == 'tm4'

    assert repr(User) == "Model<User>"
    assert str(User) == "User"

    assert str(get_table(People)) == 'people'
    assert repr(get_table(People)) == '<Table `people`>'
    assert repr(get_table(User)) == '<Table `helo`.`user_`>'
    assert hash(User) == hash(User()) == hash('helo.user_')
    try:
        get_table({})
        assert False, "Should raise err.ProgrammingError"
    except err.ProgrammingError:
        pass
    try:
        get_attrs(None)
        assert False, "Should raise err.ProgrammingError"
    except err.ProgrammingError:
        pass


def test_model_instance():
    userinfo = {
        'name': '1', 'gender': 1, 'age': 50,
        'nickname': 'n1', 'password': 1234
    }
    user1 = User(**userinfo)
    user2 = User(**userinfo)
    assert user1 == user2

    user = User(name='at7h', age=20)
    assert user
    assert not User()
    assert repr(user) == '<User object at None>'
    assert str(user) == "<User object at None>"
    assert user.name == 'at7h'
    assert user.age == 20
    assert user.id is None
    assert user.nickname is None
    assert user.password is None
    assert user.lastlogin is None

    try:
        assert user.ll is None
        assert False, "Should raise AttributeError"
    except AttributeError:
        pass

    try:
        user.id = 1
        assert False, "Should raise NotAllowedError"
    except err.NotAllowedError:
        pass
    try:
        user.pwd = 'xxx'
        assert False, "Should raise NotAllowedError"
    except err.NotAllowedError:
        pass
    try:
        user.age = '50s'
        assert False, "Should raise ValueError"
    except ValueError:
        pass

    try:
        user.gender = 'f'
        assert False, "Should raise ValueError"
    except ValueError:
        pass

    user.age = '30'
    user.password = 'XXXX'
    create_at = datetime.datetime(2020, 1, 1, 0, 0, 0)
    user.create_at = create_at
    assert user.__self__ == {
        'name': 'at7h', 'age': 30, 'password': "XXXX",
        'create_at': create_at
    }
    user.lastlogin = '2020-01-01 00:00:00'
    assert user.lastlogin == create_at
