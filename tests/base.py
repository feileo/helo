""" asyncio test base """

import asyncio
import unittest

from tests.models import db
from tests import models


TEST_DBURL = 'mysql://root:txymysql1234@cdb-m0f0sibq.bj.tencentcdb.com:10036/trod?charset=utf8'

# TEST_DBURL = 'mysql://root:gjwlmj1190@localhost:3306/trod?charset=utf8'


class UnitTestBase(unittest.TestCase):
    """ asyncio test base """

    @classmethod
    def setUpClass(cls):

        async def do_prepare():
            await db.bind(TEST_DBURL, echo=True)

            # await db.batch_create(models.TestTypesModel, models.User)
            await db.create_all(models)

        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        cls.loop.run_until_complete(do_prepare())

    @classmethod
    def tearDownClass(cls):

        async def end():
            # await db.batch_drop(models.TestTypesModel, models.User)
            await db.drop_all(models)

            await db.unbind()

        cls.loop.run_until_complete(end())
        cls.loop.close()
