from datetime import datetime

from trod import types as t
from trod.model import Model


class People(Model):

    __indexes__ = [t.K('idx_name', 'name')]

    id = t.Auto()
    name = t.VarChar(length=45)
    gender = t.Tinyint(length=1, unsigned=True)
    age = t.Tinyint(unsigned=True)
    create_at = t.Timestamp(default=t.ON_CREATE)
    update_at = t.Timestamp(default=t.ON_UPDATE)

    class Meta:
        idxes = t.K('idx_name', 'name')

    @classmethod
    def test(cls):
        return 1


class Employee(People):

    __indexes__ = [t.K('idx_age_salary', ['age', 'salary'])]

    salary = t.Float()
    departmentid = t.Int()
    phone = t.VarChar(default='')

    # class Table:
    #     idxes = t.K('idx_age_salary', ['age', 'salary'])

    @classmethod
    def test(cls):
        return 2


class User(People):

    __tablename__ = 'users'
    __indexes__ = [
        t.K('idx_name', 'name'),
        t.UK('unidx_nickname', 'nickname'),
    ]

    nickname = t.VarChar(length=100)
    password = t.VarChar(name='pwd')
    lastlogin = t.DateTime(default=datetime.now, name='ll')

    # class Table:
    #     name = 'users'
    #     idxes = [
    #         t.K('idx_name', 'name'),
    #         t.UK('unidx_nickname', 'nickname'),
    #     ]


class TypesModel(Model):

    __tablename__ = 'test_types_table'
    __comment__ = 'type case table'
    __auto_increment__ = 7
    __indexes__ = (
        t.K('key', ['tinyint', 'datetime_'], comment='key test'),
        t.UK('ukey', 'varchar', comment='unique key test'),
    )

    id = t.Auto(comment='permary_key')
    tinyint = t.Tinyint(1, unsigned=True, zerofill=True, comment='tinyint')
    smallint = t.Smallint(null=False, default=0, comment='smallint')
    int_ = t.Int(unsigned=True, null=False, default=0, comment='int')
    bigint = t.Bigint(45, null=False, default=0, comment='bigint')
    text = t.Text(encoding=t.ENCODING.utf8mb4, null=False, comment='text')
    char = t.Char(45, null=False, default='', comment='char')
    varchar = t.VarChar(45, null=False, default='', comment='varchar')
    uuid = t.UUID(comment='uuid test')
    float_ = t.Float((3, 3), default=0, comment='float')
    double_ = t.Double((4, 4), unsigned=True, default=0, comment='double')
    decimal = t.Decimal((10, 2), unsigned=True, default=0, comment='decimal')
    time_ = t.Time(default=datetime.now, comment='time')
    date_ = t.Date(default=datetime.now, comment='date')
    datetime_ = t.DateTime(default=datetime.now, comment='datetime')
    now_ts = t.Timestamp(default=datetime.now, comment='now ts')
    created_at = t.Timestamp(default=t.ON_CREATE, comment='created_at')
    updated_at = t.Timestamp(default=t.ON_UPDATE, comment='updated_at')


del Model
