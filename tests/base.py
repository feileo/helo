""" asyncio test base """

import asyncio
import unittest

from tests import models
from tests.models import db


TEST_DBURL = 'mysql://root:txymysql1234@cdb-96x2qj2a.bj.tencentcdb.com:10004/trod?charset=utf8'


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
            await db.bind(TEST_DBURL, echo=True)

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
