""" asyncio test base """

import asyncio
import os
import unittest

from tests import models
from tests.models import db


def get_test_db_url():
    """ read dburl from environment """

    test_dburl_key = 'TEST_DBURL'
    return os.environ.get(test_dburl_key)


class AsyncioTestBase(unittest.TestCase):
    """ async tests base """

    loop = None

    @classmethod
    def setUpClass(cls):
        """  called before tests in an individual class """

        async def do_prepare():
            await db.create_all(models)

        cls.loop.run_until_complete(do_prepare())

    @classmethod
    def tearDownClass(cls):
        """  called after tests in an individual class """

        async def end():
            await db.drop_all(models)

        cls.loop.run_until_complete(end())

    @classmethod
    def prepare(cls):
        """ Must be explicitly call before run all tests """

        async def do_bind():
            await db.bind(get_test_db_url(), echo=True)

        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        cls.loop.run_until_complete(do_bind())

    @classmethod
    def end(cls):
        """ Must be explicitly call after run all tests """

        async def do_unbind():
            await db.unbind()

        cls.loop.run_until_complete(do_unbind())
        cls.loop.close()


class Tester:

    async def __aenter__(self):
        """ Must be explicitly call before run all tests """
        await db.bind(get_test_db_url(), echo=True)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """ Must be explicitly call after run all tests """
        await db.unbind()

    def run(self, verbosity, suite):
        unittest.TextTestRunner(verbosity=verbosity).run(suite)
