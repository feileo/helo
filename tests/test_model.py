import asyncio
from datetime import datetime

import pytest

from trod import db, types, err, util, Model
from trod.model import ROWTYPE

from .models import People, Employee, User


class TestModel:

    def setup(self):

        async def init():
            await db.binding(db.DefaultURL.get())
            await People.create()
            await Employee.create()
            await User.create()

        asyncio.get_event_loop().run_until_complete(init())

    def teardown(self):

        async def clear():
            await People.drop()
            await Employee.drop()
            await User.drop()
            await db.unbinding()

        asyncio.get_event_loop().run_until_complete(clear())

    @pytest.mark.asyncio
    async def test_api(self):

        try:
            await Model.create()
            assert False
        except err.NotAllowedError:
            pass
        try:
            await Model.drop()
            assert False
        except err.NotAllowedError:
            pass
        csql = await People.show().create_syntax()
        assert csql == (
            "CREATE TABLE `people` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            "  `name` varchar(45) DEFAULT NULL,\n"
            "  `gender` tinyint(1) unsigned DEFAULT NULL,\n"
            "  `age` tinyint(4) unsigned DEFAULT NULL,\n"
            "  `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "  `update_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP"
            " ON UPDATE CURRENT_TIMESTAMP,\n"
            "  PRIMARY KEY (`id`),\n"
            "  KEY `idx_name` (`name`)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8"
        )
        csql = await Employee.show().create_syntax()
        assert csql == (
            "CREATE TABLE `employee` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            "  `name` varchar(45) DEFAULT NULL,\n"
            "  `gender` tinyint(1) unsigned DEFAULT NULL,\n"
            "  `age` tinyint(4) unsigned DEFAULT NULL,\n"
            "  `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "  `update_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP"
            " ON UPDATE CURRENT_TIMESTAMP,\n"
            "  `salary` float DEFAULT NULL,\n"
            "  `departmentid` int(11) DEFAULT NULL,\n"
            "  `phone` varchar(255) DEFAULT '',\n"
            "  PRIMARY KEY (`id`),\n"
            "  KEY `idx_age_salary` (`age`,`salary`)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8"
        )
        csql = await User.show().create_syntax()
        assert csql == (
            "CREATE TABLE `users` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            "  `name` varchar(45) DEFAULT NULL,\n"
            "  `gender` tinyint(1) unsigned DEFAULT NULL,\n"
            "  `age` tinyint(4) unsigned DEFAULT NULL,\n"
            "  `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "  `update_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP"
            " ON UPDATE CURRENT_TIMESTAMP,\n"
            "  `nickname` varchar(100) DEFAULT NULL,\n"
            "  `pwd` varchar(255) DEFAULT NULL,\n"
            "  `ll` datetime DEFAULT NULL,\n"
            "  PRIMARY KEY (`id`),\n"
            "  UNIQUE KEY `unidx_nickname` (`nickname`),\n"
            "  KEY `idx_name` (`name`)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8"
        )
        assert (await User.show().columns())[0]['Field'] == 'id'
        assert len(await User.show().indexes()) == 3
        assert (await User.show().indexes())[0]['Key_name'] == 'PRIMARY'
        assert (await User.show().indexes())[1]['Key_name'] == 'unidx_nickname'
        assert (await User.show().indexes())[1]['Column_name'] == 'nickname'

        try:
            User.alter()
            assert False
        except NotImplementedError:
            pass

        # test save remove
        user = User(name='at7h', gender=0, age=25)
        ret = await user.save()
        assert isinstance(ret, db.ExecResult)
        assert ret.last_id == 1
        assert ret.affected == 1
        assert user.id == 1
        assert user.password is None
        assert user.lastlogin is None

        user = User(
            name='mejor', gender=1, age=22,
            password='xxxx', nickname='huhu',
            lastlogin=datetime.now()
        )
        ret = await user.save()
        assert isinstance(ret, db.ExecResult)
        assert ret.last_id == 2
        assert ret.affected == 1
        assert user.id == 2
        assert user.age == 22
        assert user.nickname == 'huhu'
        assert user.password == 'xxxx'
        assert isinstance(user.lastlogin, datetime)

        user.age = 18
        user.gender = 0
        ret = await user.save()
        assert ret.last_id == 2
        assert ret.affected == 2
        user = await User.get(2)
        assert user.id == 2
        assert user.nickname == 'huhu'
        assert user.password == 'xxxx'
        assert user.age == 18
        assert user.gender == 0

        try:
            user = User(
                id=3,
                name='mejor', gender=1, age=22,
                password='xxxx', nickname='huhu',
                lastlogin=datetime.now()
            )
            assert False, "Should be raise NotAllowedError"
        except err.NotAllowedError:
            pass

        user = User(
            name='keyoxu', gender=1, age=28,
            password='mmmm', nickname='jiajia',
            lastlogin=datetime.now()
        )
        ret = await user.save()
        assert ret.last_id == 3
        ret = await user.remove()
        assert ret.last_id == 0
        assert ret.affected == 1
        user = User(name='n')
        try:
            await user.remove()
            assert False
        except RuntimeError:
            pass

        # test get
        user = User(
            name='keyoxu', gender=1, age=28,
            password='mmmm', nickname='jiajia',
            lastlogin=datetime.now()
        )
        ret = await user.save()
        assert ret.last_id == 4
        user = await User.get(4)
        assert isinstance(user, User)
        assert user.name == 'keyoxu'
        assert user.gender == 1
        assert user.age == 28
        assert user.password == 'mmmm'
        assert user.nickname == 'jiajia'
        assert isinstance(user.lastlogin, datetime)
        assert isinstance(user.create_at, datetime)

        user = await User.get(1, rowtype=ROWTYPE.TDICT)
        assert isinstance(user, util.tdict)
        assert user.id == 1
        assert user.name == 'at7h'
        assert user.gender == 0
        assert user.age == 25
        assert user.password is None
        assert user.nickname is None

        user = await User.get(1, rowtype=ROWTYPE.TUPLE)
        assert isinstance(user, tuple)
        # (1, 'at7h', 0, 25,
        #  datetime.datetime(2019, 11, 3, 20, 49, 3),
        #  datetime.datetime(2019, 11, 3, 20, 49, 3),
        #  None, None, None
        #  )
        assert len(user) == 9
        assert user[0] == 1
        assert user[1] == 'at7h'
        assert user[2] == 0
        assert user[3] == 25
        assert isinstance(user[4], datetime)
        assert user[6] is None

        user = await User.get(10000)
        assert user is None

        # test mget
        # no 3
        user_ids = [1, 2, 3, 4]
        users = await User.mget(user_ids)
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert isinstance(users[0], User)
        assert isinstance(users[2], User)
        assert users[0].id == 1
        assert users[0].name == 'at7h'
        assert users[0].age == 25
        assert users[1].id == 2
        assert users[1].name == 'mejor'
        assert users[1].nickname == 'huhu'
        assert users[1].gender == 0
        assert users[1].age == 18
        assert users[2].name == 'keyoxu'
        assert users[2].password == 'mmmm'
        assert users[2].nickname == 'jiajia'
        assert isinstance(users[2].lastlogin, datetime)

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
        assert users[1].id is None
        assert users[1].name == 'mejor'
        assert users[1].age == 18
        assert users[1].nickname is None
        assert users[1].lastlogin is None
        assert users[2].name == 'keyoxu'
        assert users[2].create_at is None
        assert users[2].age == 28

        users = await User.mget(
            user_ids,
            columns=[User.name, User.age, User.password],
            rowtype=ROWTYPE.TDICT
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert isinstance(users[0], util.tdict)
        assert isinstance(users[2], util.tdict)
        try:
            assert users[0].id is None
            assert False, "Should be raise AttributeError"
        except AttributeError:
            pass
        assert users[0].name == 'at7h'
        assert users[0].age == 25
        assert users[1].name == 'mejor'
        assert users[1].age == 18
        try:
            assert users[1].nickname is None
            assert users[1].lastlogin is None
            assert users[2].create_at is None
            assert False, "Should be raise AttributeError"
        except AttributeError:
            pass
        assert users[2].name == 'keyoxu'
        assert users[2].age == 28

        user_ids = [1, 2]
        users = await User.mget(user_ids, rowtype=ROWTYPE.TUPLE)
        assert isinstance(users, db.FetchResult)
        assert users.count == 2
        assert isinstance(users[1], tuple)
        assert users[0][0] == 1
        assert users[0][1] == 'at7h'
        assert users[0][2] == 0
        assert users[0][3] == 25
        assert users[0][6] is None
        assert users[1][0] == 2
        assert users[1][1] == 'mejor'
        assert users[1][2] == 0
        assert users[1][3] == 18
        assert users[1][6] == 'huhu'
        assert users[1][7] == 'xxxx'

        # test add
        user = User(name='add', gender=1, age=45, nickname='addn')
        user.password = 'passadd'
        ret = await User.add(user)
        assert ret.last_id == 5
        assert ret.affected == 1
        user = await User.get(5)
        assert user.password == 'passadd'
        assert user.name == 'add'
        assert user.nickname == 'addn'
        user.name = 'add1'
        user.nickname = 'addn1'
        try:
            ret = await User.add(user)
            assert False, "Should raise NotAllowedError"
        except err.NotAllowedError:
            pass
        del user.id
        ret = await User.add(user)
        assert ret.last_id == 6
        user = await User.get(6)
        assert user.name == 'add1'

        ret = await User.add({'name': 'add2', 'nickname': 'add2n', 'password': 'p2'})
        assert ret.last_id == 7
        user = await User.get(7)
        assert user.password == 'p2'

        try:
            await User.add({'n': 1})
        except ValueError:
            pass
        try:
            await User.add(None)
        except ValueError:
            pass
        try:
            ret = await User.add({})
            assert False, "Should raise ValueError"
        except ValueError:
            pass
        ret = await User.add({'name': 'nnnn'})
        assert ret.last_id == 8
        user = await User.get(8)
        assert user.name == 'nnnn'
        assert user.nickname is None
        assert user.age is None
        # try:
        #     ret = await User.add({'name': 'aaaa'})
        #     assert False, "Should raise ValueError"
        # except ValueError:
        #     pass

        # test madd
        user1 = User(name='user1', age=1)
        user2 = User(name='user2', age=2)
        user3 = User(name='user3', age=3)
        user4 = User(name='user4', age='4')
        ret = await User.madd([user1, user2, user3, user4])
        assert ret.last_id == 9
        assert ret.affected == 4
        user = await User.get(12)
        assert user.name == 'user4'
        assert user.age == 4
        user.name = 'user4forsave'
        await user.save()
        user = await User.get(12)
        assert user.name == 'user4forsave'

        users = [
            {'name': 'user5'},
            {'name': 'user6'},
            {'name': 'user7'},
            {'name': 'user8'},
        ]
        ret = await User.madd(users)
        assert ret.last_id == 13
        assert ret.affected == 4
        user = await User.get(16)
        assert user.name == 'user8'
        assert user.age is None

        # test set
        ret = await User.set(16, name='user8forset', age=90)
        assert ret.last_id == 0
        assert ret.affected == 1
        user = await User.get(16)
        assert user.name == 'user8forset'
        assert user.age == 90

        # test model aiter
        count = 0
        async for user in User:
            assert isinstance(user, User)
            if user.id == 1:
                assert user.name == 'at7h'
            count += 1
        assert count == 15

        count = 0
        async for user in User.select():
            assert isinstance(user, User)
            if user.id == 1:
                assert user.name == 'at7h'
            count += 1
        assert count == 15

        idx = 7
        async for user in User.select()[slice(5, 10)]:
            assert isinstance(user, User)
            assert user.id == idx
            idx += 1
        assert idx == 12

        idx, step = 7, 2
        count = 0
        async for user in User.select()[slice(5, 10, step)]:
            assert isinstance(user, User)
            assert user.id == idx
            idx += 2
            count += 1
        assert idx == 13
        assert count == 3

        # test select
        users = await User.select().all()
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert users[0].id == 1
        assert users[0].name == 'at7h'
        assert users[1].id == 2
        assert users[2].id == 4
        assert users[2].name == 'keyoxu'
        assert users[11].name == 'user5'
        assert repr(users[10]) == '<User object> at 12'
        assert str(users[10]) == '<User object> at 12'
        assert users[14].name == 'user8forset'

        users = await User.select(
            User.id, User.name
        ).where(User.id > 10).all()
        assert users.count == 6
        assert users[1].id == 12
        assert users[1].name == 'user4forsave'
        assert users[1].lastlogin is None
        assert users[2].password is None
        assert users[3].id == 14
        assert users[3].name == 'user6'

        users = await User.select().all()
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert users[-1].id == 16
        assert users[-2].name == 'user7'

        users = await User.select().rows(10, rowtype=ROWTYPE.TUPLE)
        assert isinstance(users, db.FetchResult)
        assert users.count == 10
        assert isinstance(users[0], tuple)
        assert users[0][0] == 1
        assert users[0][1] == 'at7h'
        assert users[0][3] == 25
        assert users[-1][0] == 11
        assert users[-1][1] == 'user3'
        assert users[-1][3] == 3

        users = await User.select().paginate(1, 100, ROWTYPE.TDICT)
        assert isinstance(users, db.FetchResult)
        assert users.count == 15
        assert isinstance(users[-1], util.tdict)
        assert users[6].id == 8
        assert users[9].age == 3

        user = await User.select().order_by(User.id.desc()).first()
        assert isinstance(user, User)
        assert user.id == 16
        assert user.name == 'user8forset'
        user = await (User.select()
                      .order_by(User.id.desc())
                      .first(ROWTYPE.TDICT))
        assert isinstance(user, util.tdict)
        assert user.id == 16
        assert user.name == 'user8forset'
        users = await (User.select()
                       .order_by(User.id.desc())
                       .limit(1)
                       .offset(4)
                       .all(ROWTYPE.TUPLE))
        user = users[0]
        assert isinstance(user, tuple)
        assert user[0] == 12
        assert user[1] == 'user4forsave'
        assert user[2] is None
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

        users = await (User.select()
                       .where(
                           User.lastlogin < datetime.now(),
                           User.age < 25,
                           User.name != 'at7h')
                       .order_by(User.age)
                       .rows(10))
        assert users.count == 5
        assert users[1].name == 'user2'
        assert users[4].name == 'mejor'

        users = await (User.select()
                       .where((
                           User.password == 'xxxx')
                           | (User.name.startswith('at')))
                       .all())
        assert users.count == 2
        assert users[0].id == 1
        assert users[0].gender == 0
        assert users[0].name == 'at7h'
        assert users[1].name == 'mejor'
        users = await (User.select()
                       .where(
                           util.or_(User.password == 'xxxx',
                                    User.name.startswith('at')))
                       .rows(10, 0))
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

        users = await (User.select(User.gender,
                                   types.F.count(types.SQL('1')).as_('num'))
                       .group_by(User.gender)
                       .all(ROWTYPE.TDICT))
        assert users.count == 3
        assert isinstance(users[0], util.tdict)
        assert users[0].gender is None
        assert users[0].num == 10
        assert users[1].gender == 0
        assert users[1].num == 2
        assert users[2].gender == 1
        assert users[2].num == 3
        users = await (User.select(User.gender,
                                   types.F.count(types.SQL('1')).as_('num'))
                       .group_by(User.gender)
                       .all())
        assert users.count == 3
        assert isinstance(users[0], util.tdict)
        assert users[0].gender is None
        assert users[0].num == 10
        assert users[1].gender == 0
        assert users[1].num == 2
        assert users[2].gender == 1
        assert users[2].num == 3

        users = await (User.select(User.age,
                                   types.F.count(types.SQL('*')).as_('num'))
                       .group_by(User.age)
                       .having(User.age >= 10)
                       .all())
        assert users.count == 5
        assert users == [
            {'age': 18, 'num': 1},
            {'age': 25, 'num': 1},
            {'age': 28, 'num': 1},
            {'age': 45, 'num': 2},
            {'age': 90, 'num': 1}
        ]

        users = await (User.select()
                       .order_by(User.name)
                       .limit(10)
                       .offset(7)
                       .all())
        assert users.count == 8
        assert isinstance(users[0], User)
        assert users[0].id == 9
        assert users[0].name == 'user1'

        user = await (User.select()
                      .where(User.name == 'xxxx')
                      .exist())
        assert user is False
        user = await (User.select()
                      .where(User.name == 'at7h')
                      .exist())
        assert user is True
        user = await (User.select()
                      .where(User.age > 10)
                      .exist())
        assert user is True
        user = await (User.select()
                      .where(User.age > 80)
                      .exist())
        assert user is True
        user = await (User.select()
                      .where(User.age > 90)
                      .exist())
        assert user is False

        # test funcs and scalar
        user_count = await User.select().count()
        assert user_count == 15

        user_count = await (User.select()
                            .where(User.age > 25)
                            .count())
        assert user_count == 4

        user_count = await (User.select()
                            .where(User.id.in_(list(range(10))))
                            .count())
        assert user_count == 8

        users = await (User.select(User.age).all())
        sum_age = 0
        for u in users:
            if u.age is not None:
                sum_age += u.age
        assert sum_age == 261

        age_sum = await (User.select(types.F.sum(User.age))
                         .scalar())
        assert age_sum == 261

        user_count = await (User.select(types.F.count(User.age)
                                        .as_('age_count'))
                            .where(User.age > 25)
                            .get())
        assert user_count == {'age_count': 4}

        user_count = await (User.select(types.F.count(User.age))
                            .where(User.gender == 0)
                            .scalar())
        assert user_count == 2

        user_count = await (User.select(types.F.max(User.age))
                            .where(User.age > 25)
                            .scalar())
        assert user_count == 90

        age_max = await (User.select(types.F.sum(User.age))
                         .where(User.age > 25)
                         .scalar())
        assert age_max == 208

        # test insert
        ret = await User.insert(
            name='iii1', gender=1, age=20,
            nickname='nnn1', password='ppp1'
        ).do()
        assert ret.affected == 1
        assert ret.last_id == 17
        user = await User.get(ret.last_id)
        assert user.password == 'ppp1'
        assert isinstance(user.lastlogin, datetime)

        employee = {
            'name': 'eee1', 'gender': 1, 'age': 40,
            'salary': 10000, 'departmentid': 17,
            'phone': 2312421421,
        }
        ret = await Employee.insert(employee).do()
        assert ret.affected == 1
        assert ret.last_id == 1
        em = await (Employee.select()
                    .where(Employee.name == employee['name'])
                    .all())
        assert em.count == 1
        assert em[0].name == employee['name']
        assert em[0].salary == employee['salary']
        try:
            employee = {'id': 2}
            ret = await Employee.insert(employee).do()
            assert False
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
            ret = await Employee.insert(employee).do()
            assert False
        except ValueError:
            pass

        people_list = [
            ('np1', 0, 37),
            ('np2', 1, 38),
            ('np3', 1, 39),
            ('np4', 0, 40),
        ]
        try:
            ret = await People.minsert(people_list).do()
            assert False
        except TypeError:
            pass
        ret = await People.minsert(
            people_list, columns=[People.name, People.gender, People.age]).do()
        assert ret.affected == 4
        assert ret.last_id == 2
        people = await (People.select()
                        .order_by(People.age.desc())
                        .all())
        assert people.count == 5
        assert people[-1].age == 10

        people_list = [
            (1, 'np1', 0, 37),
            (2, 'np2', 1, 38),
        ]
        try:
            ret = await People.minsert(
                people_list,
                columns=[People.id, People.name, People.gender, People.age]
            ).do()
            assert False
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
            assert False
        except ValueError:
            pass

        # test update
        ret = await (People.update(name='up1')
                     .where(People.name == 'np1')
                     .do())
        assert ret.affected == 1
        people = await People.get(2)
        assert people.name == 'up1'
        ret = await People.insert(name='up2', age=23).do()
        assert ret.affected == 1
        assert ret.last_id == 6
        ret = await People.update(age=29).where(People.id == ret.last_id).do()
        people = await People.get(6)
        assert people.name == 'up2'
        assert people.age == 29

        # test delete
        ret = await People.delete().where(People.id == 6).do()
        assert ret.affected == 1
        assert ret.last_id == 0
        people = await People.get(6)
        assert people is None
        try:
            await People.delete().do()
            assert False
        except err.DangerousOperation:
            pass

        # test replace
        ret = await User.replace(id=18, name='rp1', age=78).do()
        assert ret.affected == 1
        assert ret.last_id == 18
        user = await User.get(ret.last_id)
        assert user.name == 'rp1'
        assert user.age == 78
        # try:
        #     ret = await User.replace(password='pssforrep').do()
        #     assert ret.affected == 1
        #     assert False
        # except ValueError:
        #     pass
        ret = await User.replace(id=ret.last_id, name='rp1forreplace').do()
        user = await User.get(ret.last_id)
        assert user.name == 'rp1forreplace'

        people_list = [
            (0, 37),
            (1, 38),
            (1, 39),
            (0, 40),
        ]
        try:
            ret = await People.mreplace(people_list).do()
            assert False
        except TypeError:
            pass

        try:
            ret = await People.mreplace(
                people_list, columns=(People.gender, People.age)).do()
            assert False
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
        people = await (People.select()
                        .order_by(People.age.desc())
                        .all())
        assert people.count == 5
        assert people[-1].age == 37

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
            assert False
        except ValueError:
            pass


def test_model():
    from trod.model._impl import for_attrs, for_table

    assert isinstance(People.id, types.Auto)
    assert str(People.name) == '`name` varchar(45) DEFAULT NULL;'
    assert People.__tablename__ == 'people'
    assert isinstance(People.__indexes__, list)
    assert len(People.__indexes__) == 1
    assert People.__indexes__[0].name == 'idx_name'
    assert People.__db__ is None
    assert People.__auto_increment__ == 1
    assert People.__engine__ == 'InnoDB'
    assert People.__charset__ == 'utf8'
    assert People.__comment__ == ''
    assert People.__table__.primary.auto is True
    assert People.test() == 1
    assert for_attrs(People) == (
        {'age': 'age',
            'create_at': 'create_at',
            'gender': 'gender',
            'id': 'id',
            'name': 'name',
            'update_at': 'update_at'}
    )

    assert Employee.__tablename__ == 'employee'
    assert isinstance(Employee.__indexes__, list)
    assert len(People.__indexes__) == 1
    assert Employee.__indexes__[0].name == 'idx_age_salary'
    assert Employee.test() == 2
    assert for_attrs(Employee) == (
        {'age': 'age',
            'create_at': 'create_at',
            'departmentid': 'departmentid',
            'gender': 'gender',
            'id': 'id',
            'name': 'name',
            'phone': 'phone',
            'salary': 'salary',
            'update_at': 'update_at'
         })

    assert isinstance(User.__indexes__, list)
    assert len(User.__indexes__) == 2
    assert User.__tablename__ == 'users'
    assert User.__indexes__[0].name == 'idx_name'
    assert User.__indexes__[1].name == 'unidx_nickname'
    assert User.test() == 1
    assert for_attrs(User) == (
        {'age': 'age',
            'create_at': 'create_at',
            'gender': 'gender',
            'id': 'id',
            'll': 'lastlogin',
            'name': 'name',
            'nickname': 'nickname',
            'pwd': 'password',
            'update_at': 'update_at'
         })
    assert for_table(User).fields_dict == {
        'id': types.Auto('`id` int(11) NOT NULL AUTO_INCREMENT; [PRIMARY KEY, AUTO_INCREMENT]'),
        'name': types.VarChar('`name` varchar(45) DEFAULT NULL;'),
        'gender': types.Tinyint('`gender` tinyint(1) unsigned DEFAULT NULL;'),
        'age': types.Tinyint('`age` tinyint(4) unsigned DEFAULT NULL;'),
        'create_at': types.Timestamp('`create_at` timestamp DEFAULT CURRENT_TIMESTAMP;'),
        'update_at': types.Timestamp(
            '`update_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;'),
        'nickname': types.VarChar('`nickname` varchar(100) DEFAULT NULL;'),
        'password': types.VarChar('`pwd` varchar(255) DEFAULT NULL;'),
        'lastlogin': types.DateTime('`ll` datetime DEFAULT NULL;')
    }
    assert isinstance(User.nickname, types.VarChar)
    assert isinstance(User.password, types.VarChar)
    assert isinstance(User.lastlogin, types.DateTime)

    try:
        People.id = 12123123
        People.name = 'at7h'
        del People.name
        assert False, 'Should be raise NotAllowedError'
    except err.NotAllowedError:
        pass

    try:
        del People.name
        assert False, 'Should be raise NotAllowedError'
    except err.NotAllowedError:
        pass

    try:
        class TM(People):
            id_ = types.BigAuto()

        assert False, "Should be raise DuplicatePKError"
    except err.DuplicatePKError:
        pass

    assert repr(User) == "Model<User>"
    assert str(User) == "User"

    assert str(for_table(People)) == 'people'
    assert repr(for_table(People)) == '<Table `people`>'
    assert repr(for_table(User)) == '<Table `trod`.`users`>'
    try:
        for_table({})
        assert False
    except err.ProgrammingError:
        pass
    try:
        for_attrs(None)
        assert False
    except err.ProgrammingError:
        pass


def test_model_instance():
    user = User(name='at7h', age=20)
    assert user
    assert not User()

    assert repr(user) == '<User object> at None'
    # assert str(user) == "{'name': 'at7h', 'age': 20}"
    assert str(user) == "<User object> at None"

    assert user.name == 'at7h'
    assert user.age == 20
    assert user.id is None
    assert user.nickname is None
    assert user.password is None
    assert user.lastlogin is None
    try:
        assert user.ll is None
        assert False, "Should be raise AttributeError"
    except AttributeError:
        pass

    try:
        user.id = 1
        assert False, "Should be raise NotAllowedError"
    except err.NotAllowedError:
        pass
    try:
        user.pwd = 'xxx'
        assert False, "Should be raise NotAllowedError"
    except err.NotAllowedError:
        pass
    try:
        user.age = '50s'
        assert False, "Should be raise ValueError"
    except ValueError:
        pass

    try:
        user.gender = 'f'
        assert False, "Should be raise ValueError"
    except ValueError:
        pass

    user.age = '30'
    user.password = 'XXXX'
    create_at = datetime(2020, 1, 1, 0, 0, 0)
    user.create_at = create_at
    assert user.__self__ == {
        'name': 'at7h', 'age': 30, 'password': "XXXX",
        'create_at': create_at
    }
    user.lastlogin = '2020-01-01 00:00:00'
    assert user.lastlogin == create_at
