#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from datetime import datetime

class BaseField(object):
    _this_type = None
    _type_sql = None
    _id_count = 0

    def __init__(self, name,comment,default):
        self.name = name
        self.comment = comment
        self.default = default

    def __str__(self):
        return '{} [{}: {}]'.format(self.__class__.__name__,self.name,self.comment)


    def build(self):
        out_char = [self.parse_type()]
        out_char.extend(self.parse_common())
        return ' '.join(out_char)

    def parse_type(self):
        if self.__class__ is StringField:
            return self._type_sql.format(type=self.type,length=self.length)
        elif self.__class__ is IntegerField:
            return self._type_sql.format(type=self.type,length=self.length)
        elif self.__class__ is DecimalField:
            return self._type_sql.format(type=self.type,length=self.length,float_length=self.float_length)
        elif self.__class__ is FloatField:
            return self._type_sql.format(type=self.type,length=self.length,float_length=self.float_length)
        elif self.__class__ is TimestampField or self.__class__ is DatetimeField:
            return self._type_sql

    def parse_common(self):
        add_str = []
        # if hasattr(self, 'self.unsigned'):
        try:
            unsigned = self.unsigned
            if unsigned is True:
                add_str.append('UNSIGNED')
        except AttributeError as e:
            pass
        blank = self.blank
        if blank is not True and blank is not None and blank is not False:
            if not isinstance(blank, self._this_type):
                raise TypeError('Except blank value {} now is {}'.format(self._this_type, blank))
            if isinstance(blank, str):
                default_value = '\'{}\''.format(blank)
            else:
                default_value = blank
            add_str.append('NOT NULL DEFAULT {}'.format(default_value))
        elif blank is None or blank is True:
            add_str.append('DEFAULT NULL')
        elif blank is False:
            add_str.append('NOT NULL')
        # if hasattr(self, 'self.auto_increase'):
        try:
            auto_increment = self.auto_increase
            if auto_increment is True:
                add_str.append('AUTO_INCREMENT')
        except AttributeError as e:
            pass
        try:
            auto = self.auto
            if auto is not None:
                if auto == 'on_create':
                    add_str.append('DEFAULT CURRENT_TIMESTAMP')
                elif auto == 'on_update':
                    add_str.append('DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
        except AttributeError as e:
            pass
        comment = self.comment
        if comment:
            add_str.append("COMMENT '{}'".format(comment))
        return add_str

    @staticmethod
    def _get_id_count():
        BaseField._id_count += 1
        return BaseField._id_count

class StringField(BaseField):
    _this_type = str
    _type_sql = '{type}({length})'

    def __init__(self, 
                 name,
                 length,
                 comment='',
                 default=None, 
                 blank=None,
                 varchar=False,
                 primary_key=False):
        super(StringField,self).__init__(name=name, comment=comment,default=default)
        self.length = length
        self.blank = blank
        if varchar is True:
            self.type = 'VARCHAR'
        else:
            self.type = 'CHAR'
        self.primary_key = primary_key
        if primary_key is True:
            self.blank = False
            self.default = None
        self.id_count = self._get_id_count()


class IntegerField(BaseField):
    _this_type = int
    _type_sql = '{type}({length})'

    def __init__(self, 
                 name,
                 length,
                 comment='',
                 default=None,
                 auto_increase=False,
                 blank=None,
                 bigint=False,
                 unsigned=False,
                 primary_key=False):
        super(IntegerField,self).__init__(name=name, comment=comment,default=default)
        self.length = length
        self.auto_increase = auto_increase
        self.blank = blank
        if bigint is True:
            self.type = 'BIGINT'
        else:
            self.type = 'INT'
        self.unsigned = unsigned
        self.primary_key = primary_key
        if primary_key is True:
            self.blank = False
            self.default = None
        self.id_count = self._get_id_count()

class DecimalField(BaseField):
    _this_type = float
    _type_sql = '{type}({length},{float_length})'

    def __init__(self,
                 name,
                 length,
                 float_length,
                 comment='',
                 default=None,
                 auto_increase=False,
                 blank=None,
                 unsigned=False,
                 primary_key=False):
        super(DecimalField,self).__init__(name=name,comment=comment,default=default)
        self.length = length
        self.float_length = float_length
        self.auto_increase = auto_increase
        self.blank = blank
        self.unsigned= unsigned
        self.type = 'DECIMAL'
        self.primary_key = primary_key
        if primary_key is True:
            self.blank = False
            self.default = None
        self.id_count = self._get_id_count()


class FloatField(BaseField):
    _this_type = float
    _type_sql = '{type}({length},{float_length})'

    def __init__(self,
                 name,
                 length,
                 float_length,
                 comment='',
                 default=None,
                 auto_increase=False,
                 blank=None,
                 unsigned=False,
                 double=False,
                 primary_key=False):
        super(FloatField,self).__init__(name=name,comment=comment,default=default)
        self.length = length
        self.float_length = float_length
        self.auto_increase = auto_increase
        self.blank = blank
        self.unsigned= unsigned
        if double is True:
            self.type = 'DOUBLE'
        else:
            self.type = 'FLOAT'
        self.primary_key = primary_key
        if primary_key is True:
            self.blank = False
            self.default = None
        self.id_count = self._get_id_count()


class TimestampField(BaseField):
    _this_type = datetime
    _type_sql = 'TIMESTAMP'

    def __init__(self,
                 name,
                 comment='',
                 default=None,
                 blank=True,
                 auto=None):
        super(TimestampField,self).__init__(name=name,comment=comment,default=default)
        self.blank = blank
        self.primary_key = False
        self.id_count = self._get_id_count()
        if auto in ['on_create','on_update']:
            self.auto = auto
        elif auto is not None:
            raise Exception('auto parameter must be \'on_create\' or \'on_update\'')

    def __call__(self, *args, **kwargs):
        return datetime.now()


class DatetimeField(BaseField):
    _this_type = datetime
    _type_sql = 'DATETIME'

    def __init__(self,
                 name,
                 comment='',
                 default=None,
                 blank=True):
        super(DatetimeField,self).__init__(name=name,comment=comment,default=default)
        self.blank = blank
        self.primary_key = False
        self.id_count = self._get_id_count()

    def __call__(self, *args, **kwargs):
        return datetime.now()
