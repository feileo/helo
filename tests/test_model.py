# -*- coding=utf8 -*-
"""
test model and types
"""
from tests.base import UnitTestBase, unittest
from tests.data import TestTypesModel


class TestModel(UnitTestBase):

    def test_model_new(self):
        test_types_model = TestTypesModel()
        print(test_types_model.show_create())

        async def do():
            # await test_types_model.create()
            print(await test_types_model.show())
        self.loop.run_until_complete(do())


if __name__ == '__main__':
    unittest.main()
