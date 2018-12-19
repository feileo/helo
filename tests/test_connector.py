""" test Connector """
import sys

import asyncio
import aiomysql

from tests.base import UnitTestBase, unittest
from trod.connector import Connector
from trod.model.session import Session


class TestConnector(UnitTestBase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_connector(self):
        async def do():
            # url = 'mysql://root:txymysql1234@cdb-m0f0sibq.bj.tencentcdb.com:10036/trod?charset=utf8'
            # connector = await Connector.from_url(url)
            connector = await Connector(
                scheme='mysql', user='root', password='txymysql1234',
                host='cdb-m0f0sibq.bj.tencentcdb.com', port=10036,
                database='trod', query={'charset': 'utf8'}
            )
            # print(connector.meta)

            session = Session(connector)
            sql = "SELECT `id` FROM `teacher` WHERE `name`= %s AND `age`= %s ORDER BY `id` ASC"
            args = ['gjwdw', 64]
            result = await session.select(sql, args=args)
            self.assertEqual(result[-1].id, 59)

            conn = await connector.get()
            self.assertIsInstance(conn, aiomysql.connection.Connection)
            connector.release(conn)

            await connector.close()
        self.loop.run_until_complete(do())


if __name__ == '__main__':
    unittest.main()
