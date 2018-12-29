import unittest

from tests import test_model, test_crud, test_db


def run():
    """ run all test case """

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    bind_db = loader.loadTestsFromName('tests.base.TestPrepare.bind_db')
    unbind_db = loader.loadTestsFromName('tests.base.TestPrepare.unbind_db')

    test_db_case = loader.loadTestsFromModule(test_db)
    test_model_case = loader.loadTestsFromModule(test_model)
    test_crud_case = loader.loadTestsFromModule(test_crud)

    suite = unittest.TestSuite(
        [bind_db, test_model_case, test_crud_case, unbind_db]
    )
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    run()
