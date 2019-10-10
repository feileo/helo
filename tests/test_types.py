from datetime import datetime

import pytest

from trod import Trod, types, g

db = Trod()  # pylint: disable=invalid-name


class M(db.Model):  # type: ignore
    __table__ = 'test_types_table'
    __comment__ = 'test case table'
    __indexes__ = (
        types.K('key', ['tinyint', 'datetime_'], comment='key test'),
        types.UK('ukey', 'varchar', comment='unique key test'),
    )

    id = types.Auto(comment='permary_key')
    tinyint = types.Tinyint(1, unsigned=True, default=0, comment='tinyint test')
    smallint = types.Smallint(default=0, comment='smallint test')
    int_ = types.Int(unsigned=True, null=False, default=0, comment='int test')
    bigint = types.Bigint(45, null=False, default=0, comment='bigint test')
    text = types.Text(encoding=g.ENCODINGS.utf8mb4, null=False, comment='text test')
    char = types.Char(45, null=False, default='', comment='char test')
    varchar = types.VarChar(45, null=False, default='', comment='varchar test')
    float_ = types.Float((3, 3), default=0, comment='float test')
    double_ = types.Double((4, 4), unsigned=True, default=0, comment='double test')
    decimal = types.Decimal((4, 4), unsigned=True, default=0, comment='decimal test')
    time_ = types.Time(default=datetime.now, comment='datetime test')
    date_ = types.Date(default=datetime.now, comment='datetime test')
    datetime_ = types.DateTime(default=datetime.now, comment='datetime test')
    now_ts = types.Timestamp(default=datetime.now, comment='now ts test')
    created_at = types.Timestamp(default=g.ON_CREATE, comment='created_at')
    updated_at = types.Timestamp(default=g.ON_UPDATE, comment='updated_at')


@pytest.mark.asyncio
async def test_types():
    await db.bind(
        url="mysql://root:HELLOxm123@10.235.184.244/trod",
        echo=True
    )

    await db.create_tables(M)
    from pprint import pprint

    pprint(await M.show().columns())

    await db.unbind()
