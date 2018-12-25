""" test Connector """

import asyncio
import aiomysql

from tests.base import UnitTestBase, unittest
from trod.db.connector import Connector
from trod.db.executer import Transitioner
from tests.data import TEST_DBURL


class TestConnector(UnitTestBase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_connector(self):
        async def do():
            connector = await Connector.create(TEST_DBURL)

            await Transitioner.bind_db_by_conn(connector)
            sql = "SELECT * FROM `teacher` WHERE `name`= %s ORDER BY `id` ASC LIMIT 1"
            args = 'gjwdw'
            result = await Transitioner.text(sql, args=args)
            self.assertEqual(result[-1].id, 1)

            conn = await connector.get()
            self.assertIsInstance(conn, aiomysql.connection.Connection)
            self.assertTrue(connector.release(conn))

            await connector.close()
        self.loop.run_until_complete(do())

    def test_executer(self):
        pass


if __name__ == '__main__':
    unittest.main()
