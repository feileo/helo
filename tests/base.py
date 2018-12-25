"""
asyncio test base
"""

import asyncio
import unittest

from trod import Trod
from .data import TEST_DBURL

db = Trod()


class UnitTestBase(unittest.TestCase):
    """ asyncio test base """

    def setUp(self):

        async def set_bind():
            await db.bind(TEST_DBURL)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop.run_until_complete(set_bind())

    def tearDown(self):

        async def end():
            await db.unbind()

        self.loop.run_until_complete(end())
        self.loop.close()
