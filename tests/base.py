""" asyncio test base """
import os

from trod import db


def get_test_db_url():
    """ read dburl from environment """

    test_dburl_key = 'TEST_DBURL'
    return os.environ.get(test_dburl_key)


class Binder:

    async def __aenter__(self):
        """ Must be explicitly call before run all tests """
        await db.binding(get_test_db_url(), echo=True)
        return db

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """ Must be explicitly call after run all tests """
        await db.unbinding()
