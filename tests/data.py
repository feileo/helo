# -*- coding=utf8 -*-


from trod.model import Model
from trod.types import field, index

from trod.connector import Connector


class TestTypesModel(Model):
    __table__ = 'test_types_table'
    __comment__ = '测试用例'
    __auto_pk__ = True

    # id = field.Bigint(length=45, unsigned=True, primary_key=True, comment='主键')
    tinyint = field.Tinyint(length=20, unsigned=True, allow_null=False, default=0, comment='tinyint test')
    smallint = field.Smallint(length=25, default=0, comment='smallint test')
    int_ = field.Int(length=30, unsigned=True, allow_null=False, default=0, comment='int test')
    bigint = field.Bigint(length=30, unsigned=True, allow_null=False, default=0, comment='bigint test')
    text = field.Text(encoding='utf8mb4', allow_null=False, comment='text test')
    string = field.String(length=45, use_varchar=True, allow_null=False, default='', comment='string test')
    float_ = field.Float(name='float', length=3, float_length=3, default=0, comment='float test')
    double_ = field.Double(length=4, float_length=4, unsigned=True, default=0, comment='double test')
    decimal = field.Decimal(length=4, float_length=4, unsigned=True, default=0, comment='decimal test')
    now = field.Datetime(comment='datetime test')
    now_too = field.Timestamp(comment='now ts')
    created_at = field.Timestamp(auto='on_create', comment='created_at')
    updated_at = field.Timestamp(auto='on_update', comment='updated_at')

    key = index.Key(column=['tinyint', 'now'], comment='key test')
    unique_key = index.UniqueKey(column='string', comment='unique key test')
