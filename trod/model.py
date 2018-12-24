#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from component import EventLogger
from .table import Table
from .sqlgenerator import SelectGenerator
from .const import space
from .datafield import BaseField
from .indexfield import BaseIndexField, Key, UniqueKey
from .error import DeleteNoneTypeError
MODEL_LIST = []


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        MODEL_LIST.append(name)
        table_name = attrs.get('__table__', None) or name
        table_comment = attrs.get('__comment__', None)
        EventLogger.info('found model \'{}\', table_name [{}]'.format(name, table_name), task='scanning')
        EventLogger.info('{} buliding model \'{}\'...'.format(space, name), task='omapping')
        fieldmap, build_info, build_id = dict(), dict(), dict()
        primaryKey, key, unique_key, build_key, build_unique_key = None, None, None, None, None
        for k, v in attrs.items():
            if isinstance(v, BaseField):
                # EventLogger.info('{}found mapping {} --> {}'.format(space,k,v),task='omapping')
                # EventLogger.info('{} {}'.format('    '+space,v.build()),task='omapping')
                fieldmap[k] = v
                build_info[k] = v.build()
                build_id[v.id_count] = k
                if v.primary_key:
                    # EventLogger.info('{}found primarykey {}'.format(space,k),task='omapping')
                    # 如果已经存在一个主键
                    if primaryKey:
                        raise RuntimeError('{}Duplicate primary key found for field : {}'.format(sapce, k))
                    primaryKey = k
            if isinstance(v, BaseIndexField):
                # EventLogger.info('{}found mapping {} --> {}'.format(space,k,v),task='omapping')
                # EventLogger.info('{} {}'.format('    '+space,v.build()),task='omapping')
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
        # 保存属性名和列对象的映射关系
        attrs['__fieldmap__'] = fieldmap
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primaryKey
        attrs['__key__'] = key
        attrs['__unique_key__'] = unique_key
        BaseField._id_count = 0
        # 开始组装
        build_items, fields = [], []
        for i in range(len(build_id)):
            build_items.append(build_id[i+1] + ' ' + build_info[build_id[i+1]])
            if build_id[i+1] == primaryKey:
                continue
            fields.append(build_id[i+1])
        build_items.append("PRIMARY KEY({})".format(primaryKey))
        if build_key is not None:
            build_items.append(build_key)
        if build_unique_key is not None:
            build_items.append(build_unique_key)
        attrs['__fields__'] = fields
        # 生成建表/删表语句
        attrs['__create_sql__'] = 'CREATE TABLE {} ( {} ) comment=\'{}\';'.format(
            table_name, ','.join(build_items), table_comment)
        attrs['__drop_sql__'] = 'DROP TABLE {}.{};'
        attrs['__select__'] = 'SELECT {},{} FROM {} {} ORDER BY {};'
        return type.__new__(cls, name, bases, attrs)


# 同时继承字典 两种属性访问方式
class Model(dict, Table, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __repr__(self):
        return '{}'.format(self.__class__)

    def __getattr__(self, key):
        self._update_fields()
        try:
            return self[key]
        except KeyError:
            if key == self.__primary_key__:
                return None
            if key in self.__fields__:
                return None
            else:
                raise AttributeError("'Model' object has no attribute '{}'".format(key))

    def __setattr__(self, key, value):
        self._update_fields()
        self[key] = value

    def _update_fields(self):
        for each_field in self.__fields__:
            if each_field == self.__primary_key__:
                self.__fields__.remove(each_field)

    def get_value(self, key):
        return getattr(self, key, None)

    def set_value(self, key, value):
        return setattr(self, key, value)

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__fieldmap__[key]
            if field.default is not None:
                value = (field.default() if callable(field.default) else field.default)
                EventLogger.warning('Using default value for {}: {}'.format(key, str(value)), task='get_value')
                setattr(self, key, value)
        return value

    def _update_id(self, self_id):
        self.set_value(self.__primary_key__, self_id)

    def _delete_datamap(self):
        for field in self.__fields__:
            self.set_value(field, None)
        self.set_value(self.__primary_key__, None)

    def _get_datamap(self):
        datamap, data = {}, []
        for field in self.__fields__:
            data.append(self.get_value(field))
        datamap = dict(zip(self.__fields__, data))
        return datamap

    async def _add(self):
        args = []
        for key in self.__fields__:
            args.append(self.get_value_or_default(key))
        insert_sql = 'INSERT INTO {} ({}) VALUES ({});'.format(
            self.__table__, ', '.join(self.__fields__), ', '.join(['%s'] * len(self.__fields__))
        )
        self_id = await self.__class__.submit(insert_sql, args)
        self._update_id(self_id)
        if self.__affected__ != 1:
            EventLogger.error('failed to insert,affected rows: {}'.format(
                self.__affected__), task='save_add')

    async def _save_update(self):
        datamap = self._get_datamap()
        await self.update(uid=self.get_value_or_default(self.__primary_key__), what=datamap)

    # 这个方法 要大改
    async def save(self):
        # 先判断主键是不是auto_increase, 不强制但建议AI
        pk_field = self.__fieldmap__[self.__primary_key__]
        if pk_field.auto_increase:
            if self.get_value(self.__primary_key__) is not None:
                if len(await self.get(self.get_value(self.__primary_key__))) != 0:
                    # 这里还是一个问题
                    await self._add()
            else:
                await self._add()
        else:
            if len(await self.get(self.get_value_or_default(self.__primary_key__))) == 0:
                await self._add()
            else:
                await self._save_update()

    async def delete(self):
        uid = self.get_value(self.__primary_key__)
        if uid is None:
            raise DeleteNoneTypeError('Attempt to delete unsaved object {}.'.format(self))
        else:
            delete_sql = 'DELETE FROM {} WHERE {}=%s'.format(self.__table__, self.__primary_key__)
            await self.__class__.submit(delete_sql, uid)
            self._delete_datamap()

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
        execute_sql = 'UPDATE {} SET {} WHERE {};'.format(cls.__table__, what_clause, where_clause)
        values = what_values
        values.extend(where_values)
        self_id = await cls.submit(execute_sql, values)
        # cls._update_id(self_id)

    @classmethod
    async def get(cls, uid):
        if uid is None or uid == 0:
            EventLogger.error('\'uid\' value error --- [{}]'.format(uid), task='get_id')
            return ()
        select_sql = 'SELECT {} FROM {} {} ORDER BY {};'
        execute_sql = select_sql.format(','.join(cls.__fields__),
                                        cls.__table__, 'WHERE {}=%s'.format(cls.__primary_key__), cls.__primary_key__)
        object = await cls.select(execute_sql, uid)
        return object

    @classmethod
    def query_all(cls, *query_fields):
        for n in query_fields:
            if n not in cls.__fields__:
                if n != cls.__primary_key__:
                    raise Exception('{} not in model {}\'s query fields'.format(n, cls.__name__))
        if query_fields:
            tmp_fields = list(query_fields)
        else:
            tmp_fields = cls.__fields__
            if cls.__primary_key__ not in tmp_fields:
                tmp_fields.insert(0, cls.__primary_key__)
        return SelectGenerator(tmp_fields, cls)
