""" asyncio test base """

import asyncio
import unittest

from tests import models
from tests.models import db


TEST_DBURL = 'mysql://root:txymysql1234@cdb-m0f0sibq.bj.tencentcdb.com:10036/trod?charset=utf8'

# TEST_DBURL = 'mysql://root:gjwlmj1190@localhost:3306/trod?charset=utf8'


class UnitTestBase(unittest.TestCase):
    """ asyncio test base """

    @classmethod
    def setUpClass(cls):

        async def do_prepare():
            await db.create_all(models)

        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        cls.loop.run_until_complete(do_prepare())

    @classmethod
    def tearDownClass(cls):

        async def end():
            await db.drop_all(models)

        cls.loop.run_until_complete(end())
        cls.loop.close()


class TestPrepare(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def bind_db(self):
        async def do_bind():
            await db.bind(TEST_DBURL, echo=True)

        self.loop.run_until_complete(do_bind())

    def unbind_db(self):
        async def do_unbind():
            await db.unbind()

        self.loop.run_until_complete(do_unbind())
