import unittest

from tests import test_model, test_crud, test_db
from tests.base import AsyncioTestBase, Tester


def runtests():
    """ run all test case """
    # Skip temporarily
    pass

    # suite = unittest.TestSuite()
    # suite.addTests(
    #     [
    #         unittest.makeSuite(test_db.TestDB),
    #         unittest.makeSuite(test_model.TestModel),
    #         unittest.makeSuite(test_crud.TestCRUD),
    #     ]
    # )
    # TODO
    # async with Tester() as t:
    #     t.run(2, suite)


if __name__ == '__main__':
    runtests()
