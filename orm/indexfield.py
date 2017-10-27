#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 索引

class BaseIndexField(object):
    _type_sql = None
    _id_count = 0

    def __init__(self,key_name, col_name,comment=''):
        self.comment = comment
        if isinstance(col_name,list):
            self.col_name = col_name
        else:
            self.col_name = [col_name]
        self.key_name = key_name
        self.options = {'col': ','.join(self.col_name),
                        'key': key_name}
        self.id_count = self._get_id()

    def __str__(self):
        return '{} [{}: {}]'.format(self.__class__.__name__,self.key_name,self.comment)

    def build(self):
        return self._type_sql.format(**self.options)

    @staticmethod
    def _get_id():
        BaseIndexField._id_count += 1
        return BaseIndexField._id_count

class Key(BaseIndexField):
    _type_sql = 'KEY {key} ({col})'

class UniqueKey(BaseIndexField):
    _type_sql = 'UNIQUE KEY {key} ({col})'
