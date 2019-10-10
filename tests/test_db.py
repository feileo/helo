import pytest

from trod.g import _helper

from . import base


@pytest.mark.asyncio
async def test_db():

    async with base.Binder() as db:

        assert db.poolstate().maxsize == 15

        await db.execute(
            _helper.Query(
                "CREATE TABLE IF NOT EXISTS `user` ("
                "`id` int(20) unsigned NOT NULL AUTO_INCREMENT,"
                "`name` varchar(100) NOT NULL DEFAULT '' COMMENT '用户名',"
                "`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "PRIMARY KEY (`id`),"
                "UNIQUE KEY `idx_name` (`name`)"
                ") ENGINE=InnoDB AUTO_INCREMENT=26 "
                " DEFAULT CHARSET=utf8 COMMENT='用户表';"
            )
        )

        await db.execute(
            _helper.Query(
                "INSERT INTO `user` (`id`, `name`) VALUES (%s, %s);",
                params=[(1, 'acth'), (2, 'asdkj'), (3, 'dsd')]),
            many=True,
        )

        result = await db.execute(
            _helper.Query(
                "SELECT * FROM `user` where id in %s;", params=((1, 2, 3),)
            )
        )
        print(result)

        assert result[0].name == 'acth'

        result = await db.execute(
            _helper.Query(
                "DELETE FROM `user` where id=%s;", params=1
            ))
        print(result)
        assert result[0] == 1

        await db.execute(
            _helper.Query(
                "DROP TABLE `user`;"
            )
        )
