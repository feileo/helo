from tests.base import UnitTestBase, unittest
from tests.models import TestTypesModel, User
from trod.errors import ModifyAutoPkError, ModelSetAttrError
from trod.types.index import Key
from trod.types import field


class TestModel(UnitTestBase):

    def test_model(self):

        async def do_test():

            # table_is_exist = await TestTypesModel.exist()
            # if table_is_exist:
            #     self.assertTrue(await TestTypesModel.drop())
            # self.assertTrue(await TestTypesModel.create())

            # table_is_exist = await User.exist()
            # if table_is_exist:
            #     self.assertTrue(await User.drop())
            # self.assertTrue(await User.create())

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
                    modify_col=['string', 'decimal'], drop_col='now'
                )
            )

        self.loop.run_until_complete(do_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
