# -*- coding=utf8 -*-

""" medel """
from collections import OrderedDict

from trod.component import EventLogger
from trod.db import Transitioner
from trod.model.loader import Loader
from trod.model import SQL
from trod.model.sql import Generator, _Logic, Where
from trod.types.field import BaseField
from trod.types.index import BaseIndex
from trod.utils import Dict, dict_formatter, to_list


# from trod.component import EventLogger

TABLE_DEFAULT = {
    '__table__': '',
    '__auto_pk__': False,
    '__engine__': 'InnoDB',
    '__charset__': 'utf8',
    '__comment__': '',
}


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
            raise RuntimeError(
                f"Primary key not found for table `{attrs['__meta__'].table}`"
            )

        build_items = []
        if pk_stmt is not None:
            build_items.append(pk_stmt)
        for field in field_inst_map:
            build_items.append(field_stmt_map[field])
        build_items.append(f'PRIMARY KEY(`{primary_key}`)')
        for index in index_inst_map:
            build_items.append(key_stmt_map[index])
        attrs['__meta__'].coldef = ', '.join(build_items)

        attrs['__table__'] = Dict(
            field_dict=field_inst_map,
            index_dict=index_inst_map,
            pk=primary_key
        )

        for field in field_inst_map:
            attrs.pop(field)
        for index in index_inst_map:
            attrs.pop(index)

        return type.__new__(cls, name, bases, attrs)


class Model(metaclass=_ModelMetaclass):

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
            elif key in self.__table__.field_dict:
                return None
            elif key in self.__table__.index_dict:
                return self.__table__.index_dict[key]
            else:
                raise AttributeError("'Model' object has no attribute '{}'".format(key))

    def __setattr__(self, key, value):
        if key in self.__table__.index_dict:
            raise BaseException('Key type not allow instantiation')
        elif key not in self.__table__.field_dict:
            if not (self.__meta__.auto_pk is True and key == self.__table__.pk):
                raise TypeError(f'Not allow set attribute {key}')

        self.__dict__[key] = value

    def _get_value(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__table__.field_dict[key]
            if field.default is not None:
                value = (field.default() if callable(field.default) else field.default)
                print(f'Using default value for {key}: {value}')
                setattr(self, key, value)
        return value

    def set_value(self, key, value):
        return setattr(self, key, value)

    def save(self):
        self._save_pk_checker(self)
        return await self._add(self)

    # def modify(self):
    #     return self.updete()

    def delete(self):
        self_pk = self.__table__.pk
        pk_value = self._get(self_pk)
        if pk_value is None:
            raise RuntimeError('Modify unsaved objects')
        return self.remove(
            Where(column=self_pk, operator='==', value=pk_value)
        )

    @classmethod
    def _get_create_sql(cls):
        return SQL.create.format(
            tn=cls.__meta__.table,
            cd=cls.__meta__.coldef,
            eg=cls.__meta__.engine,
            cs=cls.__meta__.charset,
            cm=cls.__meta__.comment
        )

    @classmethod
    async def create(cls):
        return await Transitioner.execute(cls._get_create_sql())

    @classmethod
    async def drop(cls):
        drop_sql = SQL.drop.format(table_name=cls.__meta__.table)
        return await Transitioner.execute(drop_sql)

    @classmethod
    async def alter(cls, modify_col=None, add_col=None, drop_col=None):
        if not any([modify_col, add_col, drop_col]):
            return None

        modify_col, add_col, drop_col = to_list([modify_col, add_col, drop_col])

        alter_sql = Generator.alter(
            cls.__table__, cls.__meta__.table,
            modify_list=modify_col, add_list=add_col, drop_list=drop_col
        )
        return await Transitioner.execute(alter_sql)

    @classmethod
    async def show_create(cls):
        show_create_sql = SQL.show.create.format(table_name=cls.__meta__.table)
        return await Transitioner.text(
            show_create_sql, rows=1
        )

    @classmethod
    async def show_struct(cls):
        table_name = cls.__meta__.table
        show_clo_sql = SQL.show.columns.format(
            table_name=table_name, rows=1
        )
        show_idx_sql = SQL.show.indexs.format(
            table_name=table_name, rows=1
        )
        result = Dict(
            columns=await Transitioner.text(show_clo_sql),
            indexs=await Transitioner.text(show_idx_sql)
        )
        return result

    @classmethod
    async def exist(cls):
        exist_sql = SQL.exist.format(table_name=cls.__meta__.table)
        return await Transitioner.exist(exist_sql)

    @classmethod
    def _get_add_values(cls, instance, cols):
        if not isinstance(instance, cls):
            raise TypeError('Add instance not Model')
        return [instance._get_value(c) for c in cols]

    @classmethod
    def _get_insert_sql(cls, cols):
        return 'INSERT INTO {} ({}) VALUES ({});'.format(
            cls.__meta__.table, ', '.join(cols), ', '.join(['%s']*len(cols))
        )

    @classmethod
    def has_cols_checker(cls, cols):
        for c in cols:
            if c not in cls.__table__.field_dict and c != cls.__table__.pk:
                raise Exception(f"{c} not in model {cls.__name__}'s query fields")

    @classmethod
    def _save_pk_checker(cls, instance):
        if cls.__meta__.auto_pk is False:
            if instance.getattr(cls.__table__.pk) is None:
                raise Exception('has not pk to save')

    @classmethod
    async def _add(cls, instance):
        cols = list(instance.__dict__.keys())
        values = cls._get_add_values(instance, cols)
        insert_sql = cls._get_insert_sql(cols)
        result = await Transitioner.execute(insert_sql, values=values)
        instance.set_value(instance.__table__.pk, result.last_id)
        if result.affected != 1:
            EventLogger.error(
                f'failed to insert, affected rows: {result.affected}'
            )
        return result

    @classmethod
    async def add(cls, instance):
        cls._save_pk_checker(instance)
        return await cls._add(instance)

    @classmethod
    async def batch_add(cls, instances):
        if not isinstance(instances, (list, tuple)):
            raise TypeError(f'Get illgal type {instances}')

        cols = list(cls.__table__.field_dict.keys())
        for c in cols:
            c_type = cls.__meta__.field_dict[c]
            if hasattr(c_type, 'auto') and c_type.auto:
                cols.remove(c)
        values = []
        for inst in instances:
            values.append(cls._get_add_values(inst, cols))
        insert_sql = cls._get_insert_sql(cols)
        result = await Transitioner.execute(
            insert_sql, values=values, batch=True
        )
        if result.affected != len(values):
            EventLogger.error(
                f'Failed to insert, affected rows: {result.affected}'
            )

        if cls.__meta__.auto_pk is True:
            for inst in instances:
                inst.set_value(cls.__table__.pk, 0)
        return result

    @classmethod
    async def remove(cls, where):
        """
        where: Where object
        """
        if not isinstance(where, (Where, _Logic)):
            raise TypeError('')
        where_format = where.format_()
        cls.has_cols_checker(where_format.col)
        remove_sql = "DELETE FROM `{}` WHERE {};".format(
            cls.__meta__.table, where_format.where
        )
        return await Transitioner.execute(remove_sql, values=where_format.arg)

    @classmethod
    async def updete(cls, data, where=None):
        """
        data: dict, {'name': 'hehe'} or Dict(name='hehe')
        where: Where object
        """
        if not isinstance(data, dict):
            raise TypeError('')

        where_format = None
        if where is not None:
            if not isinstance(where, (Where, _Logic)):
                raise TypeError('')
            where_format = where.format_()
            cls.has_cols_checker(where_format.col)

        update_fields = list(data.keys())
        update_values = [data[f] for f in update_fields]
        set_clause = ','.join(['='.join([f, '%s']) for f in update_fields])

        if where_format:
            updete_sql = 'UPDATE `{}` SET {} WHERE {};'.format(
                cls.__meta__.table, set_clause, where_format.where
            )
            update_values.extend(where_format.arg)
        else:
            updete_sql = 'UPDATE `{}` SET {}'.format(
                cls.__meta__.table, set_clause
            )

        return await Transitioner.execute(updete_sql, values=update_values)

    @classmethod
    async def get(cls, id_):
        """
        Get by id
        """
        select_sql = 'SELECT * FROM `{}` WHERE {}=%;'.format(
            cls.__meta__.table, cls.__table__.pk
        )
        result = await Transitioner(select_sql, args=id_, rows=1).fetch()
        return Loader(cls, result).load()

    @classmethod
    async def query(cls, *cols):
        query_cols = None
        if cols:
            cls.has_cols_checker(cols)
            query_cols = list(cols)
        return Generator(cls, query_cols)
