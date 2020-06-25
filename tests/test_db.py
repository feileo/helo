#  type: ignore
#  pylint: disable=too-many-statements
"""
Tests for db module
"""

import datetime

import pytest

from helo import db, err, util, _builder, ENCODING, G

from . import case

AUTO_INCREMENT = 26
TEARDOWN_QUERY = _builder.Query("DROP TABLE `user`;")
SETUP_QUERY = _builder.Query(
    "CREATE TABLE IF NOT EXISTS `user` ("
    "`id` int(20) unsigned NOT NULL AUTO_INCREMENT,"
    "`name` varchar(100) NOT NULL DEFAULT '' COMMENT 'username',"
    "`age` int(20) NOT NULL DEFAULT '0' COMMENT 'user age',"
    "`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    "PRIMARY KEY (`id`),"
    "UNIQUE KEY `idx_name` (`name`)"
    f") ENGINE=InnoDB AUTO_INCREMENT={AUTO_INCREMENT} "
    "DEFAULT CHARSET=utf8 COMMENT='user info table';"
)


@pytest.mark.asyncio
async def test_connect_pool():
    g = G()

    async with db.Binder(debug=True):
        assert db.isbound() is True
        assert db.state().minsize == 1
        assert db.state().maxsize == 15
        assert db.state().size == 1
        assert db.state().freesize == 1
        assert db.Executer.record is True

        assert g.state == db.state()
        assert g.isbound == db.isbound()

        async with db.Executer.pool.acquire() as conn:
            assert db.state().size == 1
            assert db.state().freesize == 0
            assert conn.echo is False
            assert conn.db == 'helo'
            assert conn.charset == ENCODING.UTF8

            async with db.Executer.pool.acquire() as conn:
                assert db.state().size == 2
        await db.Executer.pool.clear()

        try:
            await db.binding(db.EnvKey.get())
            assert False, 'Should raise err.DuplicateBinding'
        except err.DuplicateBinding:
            pass
        await db.unbinding()
        assert db.state() is None
        assert db.isbound() is False
        try:
            await db.execute(_builder.Query("SELECT * FROM `user`;"))
            assert False, 'Should raise err.UnboundError'
        except err.UnboundError:
            pass
        try:
            await db.binding("")
            assert False, 'Should raise ValueError'
        except ValueError:
            pass
        try:
            await db.binding("postgres:/user:password@host:port/db")
            assert False, 'Should raise err.InvalidValueError'
        except err.InvalidValueError:
            pass
        try:
            await db.binding("postgres://user:password@host:port/db")
            assert False, 'Should raise err.NotSupportedError'
        except err.NotSupportedError:
            pass
        try:
            await db.binding(
                host='127.0.1.1', user='root', password='xxxx'
            )
            assert False, 'Should raise err.OperationalError'
        except err.OperationalError:
            pass

        await db.binding(db.EnvKey.get(), maxsize=7, autocommit=True)
        assert str(db.Executer.pool) == (
            "<Pool[1:7] for {}:{}/{}>".format(
                db.Executer.pool.connmeta.host,
                db.Executer.pool.connmeta.port,
                db.Executer.pool.connmeta.db
            )
        )
        assert db.state().maxsize == 7
        assert db.isbound() is True


@pytest.mark.asyncio
async def test_muldb():

    h1 = 'helo1'

    async def init():
        await db.execute(_builder.Query(f'CREATE DATABASE `{h1}`;'))

    async def clear():
        await db.execute(_builder.Query(f'DROP DATABASE `{h1}`;'))

    async with db.Binder(init=init, clear=clear, debug=True):
        assert db.Executer.record is True
        async with db.Executer.pool.acquire() as conn:
            assert db.state().size == 1
            assert db.state().freesize == 0
            assert conn.echo is False
            assert conn.db == 'helo'
            assert conn.charset == ENCODING.UTF8

        await db.select_db(h1)
        async with db.Executer.pool.acquire() as conn:
            assert conn.db == h1
        await db.execute(SETUP_QUERY)

        await db.execute(
            _builder.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=[('at7h', 22), ('gaven', 23), ('mejer', 24)]
            ),
            many=True,
        )
        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(26, 27, 28)]
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
        assert False, 'Should raise TypeError'
    except TypeError:
        pass

    try:
        await db.unbinding()
        assert False, 'Should raise UnboundError'
    except err.UnboundError:
        pass

    await db.binding(db.EnvKey.get())
    db.Executer.pool.terminate()

    assert (await db.Executer.death()) is False
    try:
        await db.unbinding()
        assert False, 'Should raise UnboundError'
    except err.UnboundError:
        pass
    assert (await db.Executer.death()) is False


@pytest.mark.asyncio
async def test_execute():

    async def init():
        await db.execute(SETUP_QUERY)

    async def clear():
        await db.execute(TEARDOWN_QUERY)

    async with db.Binder(init=init, clear=clear, debug=True):
        try:
            result = await db.execute('')
            assert False, 'Should raise TypeError'
        except TypeError:
            pass
        try:
            result = await db.execute(_builder.Query(''))
            assert False, 'Should raise ValueError'
        except ValueError:
            pass

        result = await db.execute(
            _builder.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=[('at7h', 22), ('gaven', 23), ('mejer', 24)]
            ),
            many=True,
        )
        assert isinstance(result, db.ExecResult)
        assert result.affected == 3
        assert result.last_id == AUTO_INCREMENT

        result = await db.execute(
            _builder.Query(
                "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                params=['suwei', 35]
            ))
        assert result.affected == 1
        assert result.last_id == 29
        assert str(result) == "(1, 29)"
        assert repr(result) == "ExecResult(affected: 1, last_id: 29)"

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(78, 79)]
            ))
        assert isinstance(users, db.FetchResult)
        assert users.count == 0

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(78, 79)]
            ),
            rows=1
        )
        assert users is None

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(26, 27, 28)]
            ),
            adicts=False
        )
        assert isinstance(users, db.FetchResult)
        assert isinstance(users[0], tuple)
        assert users.count == 3
        assert users[0][1] == 'at7h'
        assert users[1][2] == 23
        assert users[2][1] == 'mejer'
        assert isinstance(users[2][3], datetime.datetime)

        user = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(27, 28)]
            ),
            adicts=False,
            rows=1
        )
        assert isinstance(user, tuple)
        assert len(user) == 4
        assert user[0] == 27
        assert user[1] == 'gaven'
        assert user[2] == 23

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s LIMIT 1;",
                params=[(26, 27, 28)]
            ),
            adicts=False
        )
        assert isinstance(users, db.FetchResult)
        assert isinstance(users[0], tuple)
        assert users.count == 1
        assert users[0][0] == 26
        assert users[0][1] == 'at7h'

        user = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` = %s;",
                params=[100]
            ),
            adicts=False,
            rows=1
        )
        assert user is None

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(26, 27, 28)]
            ),
            rows=1
        )
        assert isinstance(users, util.adict)
        assert len(users) == 4
        assert users.id == 26
        assert users.name == 'at7h'
        assert users.age == 22

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s LIMIT 1;",
                params=[(28, 29)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 1
        assert isinstance(users[0], util.adict)
        assert users[0].id == 28
        assert users[0].name == 'mejer'

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `id` IN %s;",
                params=[(26, 27, 28)]
            ),
            rows=10
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 3
        assert users[0].name == 'at7h'
        assert users[1].age == 23
        assert users[2].name == 'mejer'
        assert isinstance(users[2].created_at, datetime.datetime)

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `created_at` <= %s ;",
                params=[case.deltanow(1)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert users.count == 4
        assert isinstance(users[0], util.adict)
        assert users[0].name == 'at7h'
        assert users[1].age == 23
        assert users[2].name == 'mejer'

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `created_at` >= %s ;",
                params=[case.deltanow(1)]
            )
        )
        assert isinstance(users, db.FetchResult)
        assert not users

        users = await db.execute(
            _builder.Query(
                "SELECT * FROM `user` WHERE `created_at` >= %s ;",
                params=[case.deltanow(1)]
            ),
            rows=1
        )
        assert users is None

        result = await db.execute(
            _builder.Query(
                "DELETE FROM `user` WHERE `id`=%s;", params=[1]
            ))
        assert result.affected == 0
        assert result.last_id == 0

        result = await db.execute(
            _builder.Query(
                "DELETE FROM `user` WHERE `id`=%s;", params=[26]
            )
        )
        assert result.affected == 1
        assert result.last_id == 0
        assert repr(result) == 'ExecResult(affected: 1, last_id: 0)'

        try:
            await db.execute(
                _builder.Query(
                    "INSERT INTO `user` (`name`, `age`) VALUES (%s, %s);",
                    params=["n1", "a25"]
                ),
            )
            assert False, "Should raise MySQLError"
        except err.MySQLError:
            pass

        try:
            await db.execute(
                _builder.Query(
                    "SELECT * FROM `user` WHER `id` IN %s;",
                    params=[(26, 27, 28)]
                )
            )
            assert False, "Should raise ProgrammingError"
        except err.ProgrammingError:
            pass

        try:
            await db.execute(
                _builder.Query(
                    "INSERT INTO `user` (`name`, `age`) VALUS (%s, %s);",
                    params=["n1", 25]
                ),
            )
            assert False, "Should raise ProgrammingError"
        except err.ProgrammingError:
            pass


@pytest.mark.asyncio
async def test_from_url():

    async with db.Binder(autocommit=True):
        connmeta = db.Executer.pool.connmeta
        assert connmeta.unix_socket is None
        assert connmeta.read_default_file is None
        assert connmeta.init_command is None
        assert connmeta.connect_timeout == 20
        assert connmeta.local_infile is False
        assert connmeta.ssl is None
        assert connmeta.auth_plugin == ''
        assert connmeta.program_name == ''
        assert connmeta.server_public_key is None

        async with db.Executer.pool.acquire() as conn:
            assert connmeta.host == conn.host
            assert connmeta.port == conn.port
            assert connmeta.user == conn.user
            assert connmeta.db == conn.db
            assert connmeta.charset == conn.charset
            assert connmeta.autocommit == conn.get_autocommit()

    async with db.Binder(
        (db.EnvKey.get() +
         "?charset=utf8mb4&maxsize=20&connect_timeout=15&autocommit=False"
         ),
        debug=True,
    ):
        assert db.Executer.record is True
        connmeta = db.Executer.pool.connmeta
        assert connmeta.unix_socket is None
        assert connmeta.read_default_file is None
        assert connmeta.init_command is None
        assert connmeta.connect_timeout == 15
        assert connmeta.local_infile is False
        assert connmeta.ssl is None
        assert connmeta.charset == ENCODING.UTF8MB4
        assert connmeta.auth_plugin == ''
        assert connmeta.program_name == ''
        assert connmeta.server_public_key is None

        async with db.Executer.pool.acquire() as conn:
            assert connmeta.host == conn.host
            assert connmeta.port == conn.port
            assert connmeta.user == conn.user
            assert connmeta.db == conn.db
            assert connmeta.charset == conn.charset
            assert connmeta.autocommit == conn.get_autocommit()
