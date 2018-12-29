import asyncio
import unittest

import aiomysql

from tests.base import TEST_DBURL
from trod.db.connector import Connector, DefaultConnConfig, Schemes
from trod.db.executer import RequestClient
from trod.errors import InvaildDBUrlError, DuplicateBindError, NoBindError


class TestDB(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test(self):

        async def do_test():

            # Connector Test
            connector = await Connector.create('')
            self.assertIsNone(connector)

            invalid_url = 'hehe'
            try:
                await Connector.create(url=invalid_url)
                assert False, 'Should be raise a InvaildDBUrlError'
            except InvaildDBUrlError:
                pass

            timeout = 10
            connector = await Connector.create(TEST_DBURL, timeout=timeout)
            self.assertIsInstance(connector, Connector)

            pool_status = connector.status
            self.assertEqual(pool_status.minsize, DefaultConnConfig.MINSIZE)
            self.assertEqual(pool_status.maxsize, DefaultConnConfig.MAXSIZE)
            pool_info = connector.db
            self.assertEqual(pool_info.db.scheme, Schemes.MYSQL.name.lower())
            self.assertEqual(pool_info.extra.connect_timeout, timeout)

            a_conn = await connector.get()
            self.assertIsInstance(a_conn, aiomysql.connection.Connection)
            self.assertTrue(connector.release(a_conn))
            self.assertTrue(await connector.clear())

            # RequestClient Test
            self.assertRaises(NoBindError, RequestClient)

            is_success = await RequestClient.bind_db_by_conn(connector)
            self.assertTrue(is_success)

            self.assertTrue(RequestClient.is_usable())

            try:
                await RequestClient.bind_db(url='url')
                assert False, 'Should be raise a DeprecateBindError'
            except DuplicateBindError:
                pass

            r_c = RequestClient()
            self.assertTrue(r_c.get_conn_status())

            self.assertTrue(await RequestClient.unbind())

        self.loop.run_until_complete(do_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
