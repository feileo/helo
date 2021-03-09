
import pytest

from helo import db
from helo import err
from helo import Query

from . import case


@pytest.mark.usefixtures('init_mysql_table')
class TestDataBase:

    @pytest.mark.asyncio
    async def test_new(self):
        with pytest.raises(ValueError):
            database = db.Database('')

        with pytest.raises(TypeError):
            database = db.Database({''})

        with pytest.raises(err.UnSupportedError):
            database = db.Database('redis://')

        with pytest.raises(ValueError):
            database = db.Database('mysql://root@127.0.0.1')

        url = f'{case.db_url()}?maxsize=7'
        database = db.Database(url)
        assert database.url.options == {'maxsize': 7}
        assert database._backend._pool.maxsize == 7

        database = db.Database(url, maxsize=10)
        assert database.url.options == {'maxsize': 7}
        assert database._backend._pool.maxsize == 10

        database = db.Database(case.db_url(), debug=True)
        assert database.options == {}
        assert database.url.scheme == 'mysql'
        assert database.url.db == 'helo'
        assert database.url.user == 'root'
        assert database.url.options == {}
        assert database.echo is True
        assert database.is_connected is False

        with pytest.raises(err.UnconnectedError):
            await database.close()

        with pytest.raises(err.UnconnectedError):
            await database.execute(Query(''))

        with pytest.raises(err.UnconnectedError):
            async for _ in database.iterate(Query('')):
                pass

        with pytest.raises(err.UnconnectedError):
            _ = database.connection()

        with pytest.raises(err.UnconnectedError):
            _ = database.transaction()

    @pytest.mark.asyncio
    async def test_connect(self):
        pass
        # await database.connect()

        # assert database.is_connected is True
        # await database.close()
