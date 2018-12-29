from datetime import datetime

from tests.base import UnitTestBase, unittest
from tests.models import TestTypesModel, User
from trod import Func, And, Or
from trod.errors import MissingPKError, AddEmptyInstanceError
from trod.utils import Dict


class TestCRUD(UnitTestBase):

    def test_a_add_get(self):

        async def do_test():
            t_model = TestTypesModel()
            try:
                await TestTypesModel.add(t_model)
                assert False, 'Should be raise a AddEmptyInstanceError'
            except AddEmptyInstanceError:
                pass
            test = 'hehe'
            t_model.text = test
            self.assertIsNone(t_model.id)
            self.assertEqual(await TestTypesModel.add(t_model), 1)

            self.assertEqual(t_model.id, 1)
            t_model = await TestTypesModel.get(1)
            self.assertEqual(t_model.text, test)
            self.assertEqual(
                t_model.float_, float(t_model._get_value('float_'))
            )

            user = User(
                name='test1', num=133459, password='mima',
                sex=1, age=20, date=datetime.now()
            )
            try:
                await User.add(user)
                assert False, 'Should be raise a MissingPKError'
            except MissingPKError:
                pass

            user_id = 1
            user.id = user_id
            self.assertEqual(user.id, user_id)

            query = await User.get(user_id)
            self.assertIsNone(query)
            self.assertEqual(await User.add(user), user_id)
            query = await User.get(user_id)
            self.assertEqual(query.id, 1)
            self.assertEqual(query.num, 133459)
            self.assertEqual(query.age, 20)

        self.loop.run_until_complete(do_test())

    def test_b_batch_add_get(self):

        async def do_test():
            users = []
            users.append(
                User(id=2, name='test2', num=556789, password='hello', sex=2, age=30)
            )
            users.append(
                User(id=3, name='test3', num=556790, password='world', sex=1, age=25)
            )
            result = await User.batch_add(users)
            self.assertEqual(result.affected, 2)

            query_users = await User.batch_get([1, 2, 3])

            self.assertEqual(len(query_users), 3)
            self.assertEqual(query_users[0].id, 1)
            self.assertEqual(query_users[1].name, 'test2')
            self.assertEqual(query_users[2].num, 556790)

        self.loop.run_until_complete(do_test())

    def test_c_remove(self):

        async def do_test():
            user_id = await User.add(
                User(id=4, name='test4', num=556799, password='ohno', sex=2, age=24)
            )
            result = await User.remove(User.num == 556799)
            self.assertEqual(result.affected, 1)
            self.assertIsNone(await User.get(user_id))

            result = await User.remove(
                And(User.id.in_([1, 2, 3]), User.sex == 2)
            )
            self.assertEqual(result.affected, 1)
            self.assertIsNone(await User.get(2))

        self.loop.run_until_complete(do_test())

    def test_d_update(self):

        async def do_test():
            user = User(id=5, name='test5', num=589223, password='555555', sex=1, age=45)
            result = await User.add(user)
            self.assertEqual(result, 5)

            self.assertEqual((await User.get(5)).num, 589223)

            new_num = 787878
            update_data = Dict(num=new_num)
            result = await User.update(
                update_data, And(User.sex == user.sex, User.age == user.age)
            )
            self.assertEqual(result.affected, 1)

            updated_user = await User.get(5)
            self.assertEqual(updated_user.num, new_num)

        self.loop.run_until_complete(do_test())

    def test_e_query(self):

        async def do_test():
            current_count = await User.query(Func.count()).scalar()
            self.assertEqual(current_count, 3)

            current_sum = await User.query(Func.sum(User.age)).scalar()
            self.assertEqual(current_sum, 90)

            users = []
            users.append(
                User(id=6, name='test6', num=656789, password='666666', sex=2, age=60)
            )
            users.append(
                User(id=7, name='test7', num=756790, password='777777', sex=1, age=70)
            )
            result = await User.batch_add(users)
            self.assertEqual(result.affected, 2)

            query = await User.query(
                User.name, User.num
            ).filter(
                And(User.sex == 1, User.age < 30)
            ).order_by(
                User.age, desc=True
            ).first()

            self.assertIsInstance(query, User)
            self.assertEqual(query.name, 'test3')
            self.assertEqual(query.num, 556790)
            self.assertIsNone(query.age)
            self.assertIsNone(query.sex)
            self.assertIsNone(query.id)

            query = await User.query().filter(
                And(Or(User.sex == 1, User.age < 45), User.id.in_([3, 4, 5, 6, 7]))
            ).order_by(
                User.age
            ).all()

            self.assertEqual(len(query), 3)
            self.assertIsInstance(query[0], User)
            self.assertEqual(query[0].age, 25)
            self.assertEqual(query[1].num, 787878)

            query = await User.query(
                User.sex, Func.count()
            ).group_by(User.sex).rows()

            self.assertEqual(len(query), 2)
            self.assertEqual(query[0].sex, 1)
            self.assertEqual(query[0].count, 4)

        self.loop.run_until_complete(do_test())

    def test_f_instance(self):

        async def do_test():
            user = User(name='test8', num=888888, password='888888', sex=2, age=80)

            try:
                await user.save()
                assert False, 'Should be raise a MissingPKError'
            except MissingPKError:
                pass

            user.id = 8
            user_id = await user.save()
            query_user = await user.get(user_id)

            self.assertEqual(query_user.id, user.id)
            self.assertEqual(query_user.num, user.num)

            self.assertTrue(await query_user.delete())
            query_user = await user.get(user_id)
            self.assertIsNone(query_user)

        self.loop.run_until_complete(do_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
