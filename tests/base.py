"""
asyncio test base
"""

import asyncio
import unittest
from trod.model.model import Model
from trod.connector import Connector


class UnitTestBase(unittest.TestCase):

    def setUp(self):
        async def activate_model():
            url = 'mysql://root:txymysql1234@cdb-m0f0sibq.bj.tencentcdb.com:10036/trod?charset=utf8'
            Model.activate(await Connector.from_url(url))

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop.run_until_complete(activate_model())

    def tearDown(self):

        async def close():
            await Model.session.close()
        self.loop.run_until_complete(close())
        self.loop.close()
