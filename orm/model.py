#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from logger import EventLogger
from .table import Table
from .sqlgenerator import SelectGenerator
from .const import space,create_args_string
from .datafield import BaseField
from .indexfield import BaseIndexField,Key,UniqueKey

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        table_name = attrs.get('__table__', None) or name
        table_comment = attrs.get('__name__',None)
        EventLogger.log('found model [{}], table_name [{}]'.format(name, table_name))
        fieldmap = dict()
        build_info = dict()
        build_id = dict()
        fields = []
        primaryKey,key,unique_key,build_key,build_unique_key = None,None,None,None,None
        for k, v in attrs.items():
            if isinstance(v, BaseField):
                EventLogger.log('{}found mapping {} --> {}'.format(space,k,v))
                EventLogger.log('{} {}'.format('    '+space,v.build()))
                fieldmap[k] = v
                build_info[k] = v.build() 
                build_id[v.id_count] = k
                if v.primary_key:
                    EventLogger.log('{}found primarykey {}'.format(space,k))
                    # 如果已经存在一个主键
                    if primaryKey:
                        raise RuntimeError('{}Duplicate primary key found for field : {}'.format(sapce,k))
                    primaryKey = k
            if isinstance(v,BaseIndexField):
                EventLogger.log('{}found mapping {} --> {}'.format(space,k,v))
                EventLogger.log('{} {}'.format('    '+space,v.build()))
                fieldmap[k] = v
                if v.__class__ is Key:
                    key = k
                    build_key = v.build()
                elif v.__class__ is UniqueKey:
                    unique_key = k
                    build_unique_key = v.build()
        if not primaryKey:
            raise RuntimeError('Primary key not found')
        for k in fieldmap.keys():
            attrs.pop(k)
        attrs['__fieldmap__'] = fieldmap  # 保存属性名和列对象的映射关系
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primaryKey
        attrs['__key__'] = key
        attrs['__unique_key__'] = unique_key
        BaseField._id_count = 0
        # 组装
        build_items = []
        for i in range(len(build_id)):
            build_items.append(build_id[i+1] + ' '+ build_info[build_id[i+1]])
            # print(fields)
            fields.append(build_id[i+1])
        build_items.append("PRIMARY KEY({})".format(primaryKey))
        if build_key is not None:
            build_items.append(build_key)
        if build_unique_key is not None:
            build_items.append(build_unique_key)
        if primaryKey in fields:
            fields.remove(primaryKey)
        attrs['__fields__'] = fields
        # 生成建表/删表语句
        attrs['__create_sql__'] = 'CREATE TABLE {} ( {} ) comment=\'{}\';'.format(
            table_name, ','.join(build_items),table_comment)
        attrs['__drop_sql__'] = 'DROP TABLE {}.{};'
        # 默认语句,通过id简单操作直接使用
        attrs['__select__'] = 'SELECT {}, {} FROM {} '.format(primaryKey, ', '.join(fields), table_name)
        attrs['__insert__'] = 'INSERT INTO {} ({}, {}) VALUES ({});'.format(table_name, primaryKey,', '.join(
            fields),create_args_string(len(fields)+1))
        attrs['__delete__'] = 'DELETE FROM {} WHERE {}=%s'.format(table_name, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict,Table,metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __repr__(self):
        return '{}'.format(self.__class__)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            if key == self.__primary_key__:
                return None
            else:    
                raise AttributeError(r"'Model' object has no attribute '{}'".format(key))

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key,None)

    def set_value(self,key,value):
        return setattr(self,key,value)

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__fieldmap__[key]
            if field.default is not None:
                value = (field.default() if callable(field.default) else field.default)
                EventLogger.warning('Using default value for {}:{}'.format(key, str(value)))
                setattr(self, key, value)
        return value
    
    def _update_data(self,self_id):
        self.set_value(self.__primary_key__,self_id)

    # 添加
    async def add(self):
        args = []
        # 至今不知道这个__fields__莫名其妙会包含id,只能暴力预防
        for key in self.__fields__:
            args.append(self.get_value_or_default(key))
        args.insert(0,self.get_value_or_default(self.__primary_key__))
        self_id = await self.__class__.submit(self.__insert__, args)
        self._update_data(self_id)
        if self.__affected__ != 1:
            EventLogger.error('failed to insert rows,affected rows: {}'.format(rows))

    async def delete(self):
        uid = self.get_value(self.__primary_key__)
        await self.__class__.submit(self.__delete__,uid)

    # 更新当前
    async def save(self):
        # 先判断主键是不是auto_increase
        pk_field = self.__fieldmap__[self.__primary_key__]
        # 如果是
        if pk_field.auto_increase:
            # 目前还不知道怎么判断此对象的记录是不是存在,可气
            pass
        # 如果不是auto_increase
        else:
            if len(await self.get(self.get_value_or_default(self.__primary_key__))) == 0:
                # 记录不存在,就添加
                self.add()
            else:
                what, data = {}, []
                for field in self.__fields__:
                    data.append(self.get_value(field))
                what = dict(zip(self.__fields__,data))
                await self.update(uid=self.get_value_or_default(self.__primary_key__),what=what)
              
    @classmethod
    async def update(cls, uid=None, where={}, what={}):
        if uid is None:
            where_data = where
        else:
            where_data = {cls.__primary_key__: uid}
        where_fields = where_data.keys()
        where_values = [where_data[n] for n in where_fields]
        where_clause = ' AND '.join(['='.join([n, '%s']) for n in where_fields])
        what_fields = what.keys()
        what_values = [what[n] for n in what_fields]
        what_clause = ','.join(['='.join([n, '%s']) for n in what_fields])
        execute_sql = 'UPDATE %s SET %s WHERE %s;' % (cls.__table__, what_clause, where_clause)
        values = what_values
        values.extend(where_values)
        # print(execute_sql, values)
        await cls.submit(execute_sql, values)

    @classmethod
    async def get(cls,uid):
        temp_sql = 'WHERE {}=%s ORDER BY {};'.format(cls.__primary_key__,cls.__primary_key__) 
        execute_sql = cls.__select__ + temp_sql
        object = await cls.select(execute_sql,uid)
        return object

    @classmethod
    def query_all(cls,*args):
        for n in args:
            if n not in cls.__fields__:
                raise Exception('%s not in model %s\'s query fields' % (n, cls.__name__))
        if args:
            tmp_fields = list(args)
        else:
            tmp_fields = cls.__fields__
        if cls.__primary_key__ is not None:
            tmp_fields.insert(0,cls.__primary_key__)
        return SelectGenerator(tmp_fields, cls)