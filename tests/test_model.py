# -*- coding=utf8 -*-
"""
test model and types
"""
from tests.base import UnitTestBase, unittest
from tests.data import TestTypesModel


class TestModel(UnitTestBase):

    def test_model_ddl(self):

        async def do():
            # if await TestTypesModel.exist():
            #     await TestTypesModel.drop()
            # await TestTypesModel.create()
            from pprint import pprint
            print(await TestTypesModel.alter(modify_col='float_', add_col='now_too'))
            # pprint.pprint(await TestTypesModel.show_struct())
        self.loop.run_until_complete(do())


if __name__ == '__main__':
    unittest.main()
