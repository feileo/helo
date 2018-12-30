import unittest

from tests import test_model, test_crud, test_db
from tests.base import AsyncioTestBase


def runtests():
    """ run all test case """

    suite = unittest.TestSuite()
    suite.addTests(
        [
            unittest.makeSuite(test_db.TestDB),
            unittest.makeSuite(test_model.TestModel),
            unittest.makeSuite(test_crud.TestCRUD),
        ]
    )
    AsyncioTestBase.prepare()
    unittest.TextTestRunner(verbosity=2).run(suite)
    AsyncioTestBase.end()


if __name__ == '__main__':
    runtests()
