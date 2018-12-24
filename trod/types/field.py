# -*- coding=utf8 -*-
"""
    Field types and Index types
"""

import traceback
from datetime import datetime


class BaseField:
    """ Field types base """
    _py_type = None
    _db_type = None
    _type_sql_tpl = None
    _attr_key_num = 0

    def __init__(self, allow_null, default, comment, name=None):

        self.allow_null = allow_null
        self.default = default
        self.comment = comment
        self.attr_key = self._attr_key
        self.name = name
        self.is_modify = False

    def __str__(self):
        return '<{} ({}: {})>'.format(self.__class__.__name__, self.name, self.comment)

    def build_type(self):
        raise NotImplementedError

    def build(self):
        field_sql = [self.build_type()]
        field_sql.extend(self.build_stmt())
        return ' '.join(field_sql)

    def build_stmt(self):
        stmt = []

        if self.allow_null is True or self.allow_null == 1:
            stmt.append('NULL')
        elif self.allow_null is False or self.allow_null == 0:
            stmt.append('NOT NULL')
        else:
            raise TypeError('Allow_null value must be True, False, 0 or 1')
        if self.default is not None:
            default = self.default
            if isinstance(self, Float):
                default = float(default)
            if not isinstance(default, self._py_type):
                raise TypeError(
                    f'Except default value {self._py_type} now is {default}'
                )
            if isinstance(default, str):
                default = f"'{self.default}'"
            stmt.append(f'DEFAULT {default}')
        elif not self.allow_null:
            print(f'Not to give default value for NOT NULL field {self.name}')
        stmt.append(f"COMMENT '{self.comment}'")
        return stmt

    def modify(self):
        self.is_modify = True
        return self

    @classmethod
    def build_default_id(cls):
        id_field = "`id` bigint(45) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键'"
        return id_field, 'id'

    @property
    def _attr_key(self):
        BaseField._attr_key_num += 1
        return BaseField._attr_key_num


class Tinyint(BaseField):
    _py_type = int
    _db_type = 'tinyint'
    _type_sql_tpl = '{type}({length})'

    def __init__(self,
                 length,
                 unsigned=False,
                 allow_null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=default, comment=comment, name=name
        )
        self.unsigned = unsigned
        self.length = length

    def build_type(self):
        type_sql = self._type_sql_tpl.format(
            type=self._db_type, length=self.length
        )
        if self.unsigned is True:
            type_sql += ' unsigned'
        return type_sql


class Smallint(Tinyint):
    _db_type = 'smallint'


class Int(Tinyint):
    _db_type = 'int'

    def __init__(self,
                 length,
                 # auto_increase=False,
                 unsigned=False,
                 allow_null=True,
                 primary_key=False,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            name=name, allow_null=allow_null, default=default,
            comment=comment, length=length, unsigned=unsigned
        )
        self.primary_key = primary_key
        if self.primary_key is True:
            if self.allow_null:
                print('primary_key is not allow null, use default')
            self.allow_null = False
            self.default = None


class Bigint(Int):
    _db_type = 'bigint'


class Text(BaseField):
    _py_type = str
    _db_type = 'text'
    _type_sql_tpl = '{type}'

    def __init__(self,
                 encoding=None,
                 allow_null=True,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=None, comment=comment, name=name
        )
        self.encoding = encoding

    def build_type(self):
        return self._type_sql_tpl.format(type=self._db_type)

    def build_stmt(self):
        stmt = []
        if self.encoding is not None:
            stmt.append(f'CHARACTER SET {self.encoding}')
        if self.allow_null is True or self.allow_null == 1:
            stmt.append('NULL')
        elif self.allow_null is False or self.allow_null == 0:
            stmt.append('NOT NULL')
        else:
            raise TypeError('Allow_null value must be True, False, 0 or 1]')
        stmt.append(f"COMMENT '{self.comment}'")
        return stmt


class String(BaseField):
    _py_type = str
    _db_type = 'char'
    _type_sql_tpl = '{type}({length})'

    def __init__(self,
                 length,
                 use_varchar=False,
                 encoding=None,
                 allow_null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=default, comment=comment,
            name=name
        )
        self.encoding = encoding
        self.length = length
        if use_varchar is True:
            self._db_type = 'varchar'

    def build_type(self):
        return self._type_sql_tpl.format(
            type=self._db_type, length=self.length
        )

    def build_stmt(self):
        stmt = []
        if self.encoding is not None:
            stmt.append(f'CHARACTER SET {self.encoding}')
        if self.allow_null is True or self.allow_null == 1:
            stmt.append('NULL')
        elif self.allow_null is False or self.allow_null == 0:
            stmt.append('NOT NULL')
        else:
            raise TypeError('Allow_null value must be True, False, 0 or 1]')
        if self.default is not None:
            default = self.default
            if not isinstance(default, self._py_type):
                raise TypeError(
                    f'Except default value {self._py_type} now is {default}')
            if isinstance(default, str):
                default = f"'{self.default}'"
            stmt.append(f'DEFAULT {default}')
        elif not self.allow_null:
            print(f'Not to give default value for NOT NULL field {self.name}')
        stmt.append(f"COMMENT '{self.comment}'")
        return stmt


class Float(BaseField):
    _py_type = float
    _db_type = 'float'
    _type_sql_tpl = '{type}({length},{float_length})'

    def __init__(self,
                 length,
                 unsigned=False,
                 allow_null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=default, comment=comment, name=name
        )
        if not isinstance(length, (tuple)):
            raise TypeError('Length type error')
        self.length = length[0]
        if len(length) != 2:
            print('Warning: length format error')
            self.float_length = length[-1]
        else:
            self.float_length = length[1]
        self.unsigned = unsigned

    def build_type(self):
        type_sql = self._type_sql_tpl.format(
            type=self._db_type, length=self.length, float_length=self.float_length
        )
        if self.unsigned is True:
            type_sql += ' unsigned'
        return type_sql


class Double(Float):
    _db_type = 'double'


class Decimal(Float):
    _db_type = 'decimal'


class Datetime(BaseField):
    _py_type = datetime
    _db_type = 'datetime'
    _type_sql_tpl = '{type}'

    def __init__(self,
                 allow_null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=default, comment=comment, name=name
        )

    def __call__(self, *args, **kwargs):
        return datetime.now()

    def build_type(self):
        return self._type_sql_tpl.format(type=self._db_type)


class Timestamp(Datetime):
    _db_type = 'timestamp'

    def __init__(self,
                 allow_null=True,
                 auto=None,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            allow_null=allow_null, default=default, comment=comment, name=name
        )
        self.auto = auto
        if auto in ['on_create', 'on_update']:
            self.auto = auto
        elif auto is not None:
            raise Exception("auto parameter must be 'on_create' or 'on_update'")

    def build_stmt(self):
        stmt = []
        if self.allow_null is True or self.allow_null == 1:
            stmt.append('NULL')
        elif self.allow_null is False or self.allow_null == 0:
            stmt.append('NOT NULL')
        else:
            raise TypeError('Allow_null value must be True, False, 0 or 1]')

        if self.auto is not None:
            if self.auto == 'on_create':
                stmt.append('DEFAULT CURRENT_TIMESTAMP')
            elif self.auto == 'on_update':
                stmt.append('DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
        elif self.default is not None:
            if not isinstance(self.default, self._py_type):
                raise TypeError(
                    f'Except default value {self._py_type} now is {self.default}'
                )
            stmt.append(f'DEFAULT {self.default}')
        elif not self.allow_null:
            print(f'Not to give default value for NOT NULL field {self.name}')

        stmt.append(f"COMMENT '{self.comment}'")

        return stmt
