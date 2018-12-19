# -*- coding=utf8 -*-
""" medel """
from collections import OrderedDict

from trod.model.session import Session
from trod.types.field import BaseField
from trod.types.index import BaseIndex
from trod.utils import Dict, dict_formatter


# from trod.component import EventLogger
MODEL_LIST = []

TABLE_DEFAULT = {
    '__table__': '',
    '__auto_pk__': False,
    '__engine__': 'InnoDB',
    '__charset__': 'utf8',
    '__comment__': ''
}


def gen_create_sql(t_n, i_c, eg, cs, cm):
    return f"CREATE TABLE `{t_n}` (\n{i_c}\n) ENGINE={eg} DEFAULT CHARSET={cs} COMMENT='{cm}'"


class _ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        @dict_formatter
        def build_meta():
            meta = {}
            for arg in TABLE_DEFAULT:
                if arg == '__table__':
                    table_name = attrs.get(arg, None)
                    if table_name is None:
                        print('w: no table name,use model name')
                        table_name = name
                    meta[arg.strip('_')] = table_name
                else:
                    meta[arg.strip('_')] = attrs.get(arg, None) or TABLE_DEFAULT[arg]
                if arg in attrs.keys():
                    attrs.pop(arg)
            return meta

        attrs['__meta__'] = build_meta()
        table_name = attrs['__meta__'].table

        primary_key = None
        field_stmt_map, key_stmt_map = {}, {}
        field_inst_map, index_inst_map = OrderedDict(), OrderedDict()

        pk_stmt = None

        if attrs['__meta__'].auto_pk:
            pk_stmt, primary_key = BaseField.build_default_id()

        for attr_name, attr_instance in attrs.items():
            if primary_key and attr_name == primary_key:
                raise RuntimeError('Duplicate field name `id`')
            if isinstance(attr_instance, BaseField):
                field_name = attr_instance.name if attr_instance.name else attr_name
                field_stmt_map[attr_name] = f'`{field_name}` {attr_instance.build()}'
                if hasattr(attr_instance, 'primary_key') and attr_instance.primary_key:
                    if primary_key is not None:
                        raise RuntimeError(
                            f'Duplicate primary key found for field {attr_name}'
                        )
                    primary_key = attr_name
                field_inst_map[attr_name] = attr_instance

            elif isinstance(attr_instance, BaseIndex):
                key_stmt_map[attr_name] = attr_instance.build()
                index_inst_map[attr_name] = attr_instance
            else:
                if not (attr_name.endswith('__') and attr_name.endswith('__', 0, 2)):
                    raise TypeError('TypeError child')

        if not primary_key:
            raise RuntimeError(f'Primary key not found for table `{table_name}`')

        build_items = []
        if pk_stmt is not None:
            build_items.append(pk_stmt)
        for field in field_inst_map:
            build_items.append(field_stmt_map[field])
        build_items.append(f'PRIMARY KEY(`{primary_key}`)')
        for index in index_inst_map:
            build_items.append(key_stmt_map[index])
        inner_create = ',\n'.join(build_items)
        attrs['__ddl__'] = Dict()
        attrs['__table__'] = Dict()
        attrs['__ddl__'].create_sql = gen_create_sql(
            table_name, inner_create,
            attrs['__meta__'].engine,
            attrs['__meta__'].charset,
            attrs['__meta__'].comment
        )
        attrs['__ddl__'].drop_sql = 'DROP TABLE {}.{};'
        attrs['__table__'].field_dict = field_inst_map
        attrs['__table__'].index_dict = index_inst_map
        attrs['__table__'].pk = primary_key

        for field in field_inst_map:
            attrs.pop(field)
        for index in index_inst_map:
            attrs.pop(index)

        return type.__new__(cls, name, bases, attrs)


class Model(metaclass=_ModelMetaclass):
    session = None

    @classmethod
    def activate(cls, connector):
        cls.session = Session(connector)

    def __repr__(self):
        return f"<{self.__class__.__name__}(table '{self.__meta__.table}' : {self.__meta__.comment})>"

    def __str__(self):
        return f"<{self.__class__.__name__}(table '{self.__meta__.table}' : {self.__meta__.comment})>"

    def __getattr__(self, key):
        try:
            return self.__dict__[key]
        except KeyError:
            if key == self.__table__.pk:
                return None
            if key in self.__table__.fields:
                return None
            elif key in self.__table__.index_dict:
                return self.__table__.index_dict[key]
            else:
                raise AttributeError("'Model' object has no attribute '{}'".format(key))

    def __setattr__(self, key, value):
        if key not in self.__table__.fields:
            raise TypeError(f'Not allow set attribute {key}')
        self.__dict__[key] = value

    # Model's DDL statement
    #
    @classmethod
    async def create(cls):
        await cls.session.executer.submit(cls.__ddl__.create_sql)

    @classmethod
    def show_create(cls):
        return cls.__ddl__.create_sql

    @classmethod
    def show(cls):
        show_sql = f'show full columns from {cls.__meta__.table}'
        return cls.session.executer.select(show_sql)

    @classmethod
    def alter(cls):
        pass

    @classmethod
    def drop(cls):
        pass

    # Model's DML statement
    #
    @classmethod
    def add(instance):
        pass

    @classmethod
    def delete(cls):
        pass

    @classmethod
    def updete(cls):
        pass

    @classmethod
    def get(cls):
        pass

    @classmethod
    def batch_get(cls):
        pass

    @classmethod
    def query(cls):
        pass

    # instance method
    def save(self):
        pass

    def dd(self):
        pass
