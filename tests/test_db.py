import datetime

import pytest
import pytz

from trod import db, err, util, _helper as helper

tz = pytz.timezone('Asia/Shanghai')

SETUP_QUERY = helper.Query(
    "CREATE TABLE IF NOT EXISTS `user` ("
    "`id` int(20) unsigned NOT NULL AUTO_INCREMENT,"
    "`name` varchar(100) NOT NULL DEFAULT '' COMMENT 'username',"
    "`age` int(20) NOT NULL DEFAULT '0' COMMENT 'user age',"
    "`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    "PRIMARY KEY (`id`),"
    "UNIQUE KEY `idx_name` (`name`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=26 "
    "DEFAULT CHARSET=utf8 COMMENT='user info table';"
)
TEARDOWN_QUERY = helper.Query("DROP TABLE `user`;")


@pytest.mark.asyncio
async def test_sin():

    async def init():
        await db.execute(SETUP_QUERY)

    async def clear():
        await db.execute(TEARDOWN_QUERY)

    async with db.Binder(init=init, clear=clear, echo=True):

        assert db.get_state().minsize == 1
        assert db.get_state().maxsize == 15
        assert db.get_state().size == 1
        assert db.get_state().freesize == 1
        assert db._impl.Executer.pool.echo is True

        async with db._impl.Executer.pool.acquire() as conn:
            assert db.get_state().size == 1
            assert db.get_state().freesize == 0
            assert conn.echo is True
            assert conn.db == 'trod'
            assert conn.charset == 'utf8'

            async with db._impl.Executer.pool.acquire() as conn:
                assert db.get_state().size == 2

        await db._impl.Executer.pool.clear()

        try:
            await db.binding(db.DefaultURL.get())
            assert False, 'Should be raise DuplicateBinding'
        except err.DuplicateBinding:
            pass

        await db.unbinding()
        assert db.get_state() is None

        try:
            await db.unbinding()
            await db.execute(helper.Query("SELECT * FROM `user`;"))
            assert False, 'Should be raise UnboundError'
        except err.UnboundError:
            pass

        try:
            await db.binding("postgres:/user:password@host:port/db")
            assert False, 'Should be raise ValueError'
        except ValueError:
            pass

        try:
            await db.binding("postgres://user:password@host:port/db")
            assert False, 'Should be raise NotSupportedError'
        except err.NotSupportedError:
            pass

        try:
            await db.binding(
                host='127.0.0.1', user='root', password='xxxx'
            )
            assert False, 'Should be raise OperationalError'
        except err.OperationalError:
            pass

        await db.binding(db.DefaultURL.get(), maxsize=7, autocommit=True)
        assert str(db._impl.Executer.pool) == (
            "<Pool[1:7] for {}:{}/{}>".format(
                db._impl.Executer.pool.connmeta.host,
                db._impl.Executer.pool.connmeta.port,
                db._impl.Executer.pool.connmeta.db
            )
        )
        assert db.get_state().maxsize == 7
        assert db.is_bound() is True

        try:
            result = await db.execute('')
            assert False, 'Should be raise TypeError'
        except TypeError:
            pass
        try:
            result = await db.execute(helper.Query(''))
            assert False, 'Should be raise ValueError'
        except ValueError:
            pass

        result = await db.execute(
            helper.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=[('at7h', 22), ('gaven', 23), ('mejer', 24)]
            ),
            many=True,
        )
        assert isinstance(result, db.ExecResult)
        assert result.affected == 3
        assert result.last_id == 26

        result = await db.execute(
            helper.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=['suwei', 35]
            ),
        )
        assert result.affected == 1
        assert result.last_id == 29
        assert str(result) == '(1, 29)'

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(78, 79)]
            ),
        )
        assert isinstance(users, list)
        assert not users

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(78, 79)]
            ),
            rows=1
        )
        assert users is None

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(26, 27, 28)]
            ),
            tdict=False
        )
        assert isinstance(users, db.FetchResult)
        assert isinstance(users[0], tuple)
        assert users.count == 3
        assert users[0][1] == 'at7h'
        assert users[1][2] == 23
        assert users[2][1] == 'mejer'
        assert isinstance(users[2][3], datetime.datetime)

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(27, 28)]
            ),
            tdict=False,
            rows=1
        )
        assert isinstance(users, tuple)
        assert len(users) == 4
        assert users[0] == 27
        assert users[1] == 'gaven'
        assert users[2] == 23

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s LIMIT 1;", params=[(26, 27, 28)]
            ),
            tdict=False,
        )
        assert isinstance(users, db.FetchResult)
        assert isinstance(users[0], tuple)
        assert users.count == 1
        assert users[0][0] == 26
        assert users[0][1] == 'at7h'

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id`=%s;", params=[100]
            ),
            tdict=False,
            rows=1
        )
        assert not users

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(26, 27, 28)]
            ),
            rows=1
        )
        assert isinstance(users, util.tdict)
        assert len(users) == 4
        assert users.id == 26
        assert users.name == 'at7h'
        assert users.age == 22

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s LIMIT 1;", params=[(28, 29)]
            ),
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 1
        assert isinstance(users[0], util.tdict)
        assert users[0].id == 28
        assert users[0].name == 'mejer'

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(26, 27, 28)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert users[0].name == 'at7h'
        assert users[1].age == 23
        assert users[2].name == 'mejer'
        assert isinstance(users[2].created_at, datetime.datetime)

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `created_at` <= %s ;",
                params=[datetime.datetime.now(tz)+datetime.timedelta(minutes=1)]
            )
        )

        assert isinstance(users, db.FetchResult)
        assert users.count == 4
        assert isinstance(users[0], util.tdict)
        assert users[0].name == 'at7h'
        assert users[1].age == 23
        assert users[2].name == 'mejer'

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `created_at` >= %s ;",
                params=[datetime.datetime.now(tz)+datetime.timedelta(minutes=1)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert not users

        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `created_at` >= %s ;",
                params=[datetime.datetime.now(tz)+datetime.timedelta(minutes=1)]
            ),
            rows=1
        )
        assert users is None

        result = await db.execute(
            helper.Query(
                "DELETE FROM `user` WHERE `id`=%s;", params=[1]
            ))
        assert result.affected == 0
        assert result.last_id == 0
        assert repr(result) == 'ExecResult(affected: 0, last_id: 0)'

        result = await db.execute(
            helper.Query(
                "DELETE FROM `user` WHERE `id`=%s;", params=[26]
            ))
        assert result.affected == 1
        assert result.last_id == 0
        assert repr(result) == 'ExecResult(affected: 1, last_id: 0)'

        try:
            await db.execute(
                helper.Query(
                    "SELECT * FROM `user` WHER `id` IN %s;", params=[(26, 27, 28)]
                )
            )
            assert False, "Should be raise ProgrammingError"
        except err.ProgrammingError:
            pass

        try:
            await db.execute(
                helper.Query(
                    "INSERT INTO `user` (`name`, `age`) VALUS ('n', 1);",
                ),
            )
            assert False, "Should be raise ProgrammingError"
        except err.ProgrammingError:
            pass


@pytest.mark.asyncio
async def test_mul():

    async def init():
        await db.execute(helper.Query(f'CREATE DATABASE `{t1}`;'))

    async def clear():
        await db.execute(helper.Query(f'DROP DATABASE `{t1}`;'))

    t1 = 'trod_1'
    async with db.Binder(init=init, clear=clear):

        async with db._impl.Executer.pool.acquire() as conn:
            assert db.get_state().size == 1
            assert db.get_state().freesize == 0
            assert conn.echo is False
            assert conn.db == 'trod'
            assert conn.charset == 'utf8'

        await db.select_db(t1)
        async with db._impl.Executer.pool.acquire() as conn:
            conn.db == t1
        await db.execute(SETUP_QUERY)

        await db.execute(
            helper.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=[('at7h', 22), ('gaven', 23), ('mejer', 24)]
            ),
            many=True,
        )
        users = await db.execute(
            helper.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;", params=[(26, 27, 28)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert users[0].name == 'at7h'
        assert users[1].age == 23
        assert users[2].name == 'mejer'
        assert isinstance(users[2].created_at, datetime.datetime)

        await db.execute(TEARDOWN_QUERY)

    try:
        async with db.Binder(test='testarg'):
            pass
        assert False, 'Should be raise TypeError'
    except TypeError:
        pass

    try:
        await db.unbinding()
        assert False, 'Should be raise UnboundError'
    except err.UnboundError:
        pass

    await db.binding(db.DefaultURL.get())
    db._impl.Executer.pool.terminate()
    try:
        await db.unbinding()
        assert False, 'Should be raise UnboundError'
    except err.UnboundError:
        pass


@pytest.mark.asyncio
async def test_url():

    async with db.Binder(autocommit=True):
        connmeta = db._impl.Executer.pool.connmeta

        assert connmeta.unix_socket is None
        assert connmeta.read_default_file is None
        assert connmeta.init_command is None
        assert connmeta.connect_timeout == 20
        assert connmeta.local_infile is False
        assert connmeta.ssl is None
        assert connmeta.auth_plugin == ''
        assert connmeta.program_name == ''
        assert connmeta.server_public_key is None

        async with db._impl.Executer.pool.acquire() as conn:
            assert connmeta.host == conn.host
            assert connmeta.port == conn.port
            assert connmeta.user == conn.user
            assert connmeta.db == conn.db
            assert connmeta.charset == conn.charset
            assert connmeta.autocommit == conn.get_autocommit()

    async with db.Binder(
        ("mysql://root:HELLOxm123@10.235.158.241:3306/trod"
         "?charset=utf8mb4&maxsize=20&connect_timeout=15"
         )
    ):
        connmeta = db._impl.Executer.pool.connmeta

        assert connmeta.unix_socket is None
        assert connmeta.read_default_file is None
        assert connmeta.init_command is None
        assert connmeta.connect_timeout == 15
        assert connmeta.local_infile is False
        assert connmeta.ssl is None
        assert connmeta.charset == 'utf8mb4'
        assert connmeta.auth_plugin == ''
        assert connmeta.program_name == ''
        assert connmeta.server_public_key is None

        async with db._impl.Executer.pool.acquire() as conn:
            assert connmeta.host == conn.host
            assert connmeta.port == conn.port
            assert connmeta.user == conn.user
            assert connmeta.db == conn.db
            assert connmeta.charset == conn.charset
            assert connmeta.autocommit == conn.get_autocommit()
