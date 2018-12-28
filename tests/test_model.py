from tests.base import UnitTestBase, unittest
from tests.models import TestTypesModel, User, db
from trod.errors import ModifyAutoPkError, ModelSetAttrError, DuplicateBindError
from trod.types import field
from trod.types.index import Key


class TestModel(UnitTestBase):

    def test_model(self):

        async def do_test():

            self.assertTrue(await TestTypesModel.show_create())
            self.assertTrue(await TestTypesModel.show_struct())

            self.assertIsInstance(TestTypesModel.smallint, field.Smallint)
            self.assertIsInstance(TestTypesModel.key, Key)

            try:
                TestTypesModel.new_attr = field.Smallint(3)
                assert False, 'Should be raise a ModelSetAttrError'
            except ModelSetAttrError:
                pass

            test_value = 'test'
            t_model = TestTypesModel(string=test_value)
            self.assertIsInstance(t_model.key, Key)
            self.assertIsNone(t_model.id)
            self.assertIsNone(t_model.now)
            self.assertEqual(t_model.string, test_value)
            try:
                t_model.no_has_attr = 1
                assert False, 'Should be raise a AttributeError'
            except AttributeError:
                pass

            try:
                t_model.id = 1
                assert False, 'Should be raise a ModifyAutoPkError'
            except ModifyAutoPkError:
                pass
            user_id = 1234457
            user = User(id=user_id)
            self.assertEqual(user.id, user_id)

            self.assertIsNone(await TestTypesModel.alter())
            self.assertTrue(
                await TestTypesModel.alter(
                    modify_col=[TestTypesModel.string, TestTypesModel.decimal],
                    drop_col=TestTypesModel.now
                )
            )

        self.loop.run_until_complete(do_test())

    def test_trod(self):

        async def do_test():

            self.assertTrue(db.is_bind)

            try:
                await db.bind('')
                assert False, 'Should be raise a DuplicateBindError'
            except DuplicateBindError:
                pass

            self.assertTrue(db.db_info)

            succeed = await db.batch_drop(TestTypesModel, User)
            self.assertEqual(len(succeed), 2)
            succeed = await db.batch_create(TestTypesModel, User)
            self.assertEqual(len(succeed), 2)

            sql = "INSERT INTO `user` (`id`, `name`, `num`) values (100, 'test', 1234)"
            result = await db.text(sql)
            self.assertEqual(result.affected, 1)

            sql = 'SELECT `name`, `num` FROM `user` WHERE id=100'
            query_user = await db.text(sql, rows=1)
            self.assertEqual(query_user.name, 'test')
            self.assertEqual(query_user.num, 1234)

        self.loop.run_until_complete(do_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
