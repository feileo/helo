import asyncio
import pytest

from helo import _sql
from helo import db

from . import case

SETUP_MYSQL_QUERY = _sql.Query(
    "CREATE TABLE IF NOT EXISTS `user` ("
    "`id` int(20) unsigned NOT NULL AUTO_INCREMENT,"
    "`name` varchar(100) NOT NULL DEFAULT '' COMMENT 'username',"
    "`age` int(20) NOT NULL DEFAULT '0' COMMENT 'user age',"
    "`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    "`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
    "PRIMARY KEY (`id`),"
    "UNIQUE KEY `idx_name` (`name`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=17 "
    "DEFAULT CHARSET=utf8 COMMENT='user info table'"
)
TEARDOWN_MYSQL_QUERY = _sql.Query("DROP TABLE `user`")


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='class')
async def init_mysql_table():
    database = db.Database(case.db_url(), debug=True)
    await database.connect()
    await database.execute(SETUP_MYSQL_QUERY)

    yield

    await database.execute(TEARDOWN_MYSQL_QUERY)
    await database.close()
