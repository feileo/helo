from collections import OrderedDict

from trod.db.executer import RequestClient
from trod.errors import (
    DuplicateFieldNameError, DuplicatePKError, NoPKError,
    InvalidFieldType, IllegalModelAttrAssigendError,
    DeleteUnsavedError, MissingPKError, ModifyAutoPkError,
    AddEmptyInstanceError, ModelSetAttrError
)
from trod.extra.logger import Logger
from trod.model.loader import Loader
from trod.model.sql import SQL, _Generator, _Logic, _Where, Func
from trod.types.field import BaseField
from trod.types.index import BaseIndex
from trod.utils import Dict, dict_formatter, to_list


TABLE_DEFAULT = {
    '__table__': '',
    '__auto_pk__': False,
    '__engine__': 'InnoDB',
    '__charset__': 'utf8',
    '__comment__': '',
}


class _ModelMetaclass(type):
    """ Metaclass of Trod Model """

    def __new__(cls, name, bases, attrs):
        if name in ['_Model', '_TrodModel']:
            return type.__new__(cls, name, bases, attrs)

        @dict_formatter
        def build_meta():
            meta = {}
            for arg in TABLE_DEFAULT:
                if arg == '__table__':
                    table_name = attrs.get(arg, None)
                    if table_name is None:
                        Logger.warning(
                            "Did not give the table name by '__table__', use the model name"
                        )
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
                raise DuplicateFieldNameError('Duplicate field name `id`')
            if isinstance(attr_instance, BaseField):
                field_name = attr_instance.name if attr_instance.name else attr_name
                attr_instance.name = field_name
                field_stmt_map[attr_name] = f'`{field_name}` {attr_instance.build()}'
                if hasattr(attr_instance, 'primary_key') and attr_instance.primary_key:
                    if primary_key is not None:
                        raise DuplicatePKError(
                            f'Duplicate primary key found for field {attr_name}'
                        )
                    primary_key = attr_name
                field_inst_map[attr_name] = attr_instance

            elif isinstance(attr_instance, BaseIndex):
                key_stmt_map[attr_name] = attr_instance.build()
                index_inst_map[attr_name] = attr_instance
            else:
                if not (attr_name.endswith('__') and attr_name.endswith('__', 0, 2)):
                    raise InvalidFieldType('Invalid model field {}'.format(attr_name))

        if not primary_key:
            raise NoPKError(
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

        # This will be very important
        for field in field_inst_map:
            attrs.pop(field)
        for index in index_inst_map:
            attrs.pop(index)

        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key in cls.__table__.field_dict:
            return cls.__table__.field_dict[key]
        elif key in cls.__table__.index_dict:
            return cls.__table__.index_dict[key]
        elif key == cls.__table__.pk:
            return None
        else:
            raise AttributeError(
                f'Model class does not have `{key}` attribute'
            )

    def __setattr__(cls, _key, _value):
        raise ModelSetAttrError('Model class not allow set attribute')


class _Model(metaclass=_ModelMetaclass):
    """ Interact with RequestClient to provide model functionality """

    def __init__(self, **kwargs):
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self):
        return "<{model_name}(table '{table_name}' : {table_comment})>".format(
            model_name=self.__class__.__name__, table_name=self.__meta__.table,
            table_comment=self.__meta__.comment
        )

    __str__ = __repr__

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
                raise AttributeError(
                    "'Model' object has no attribute '{}'".format(key)
                )

    def __setattr__(self, key, value, is_loader=False):
        if key in self.__table__.index_dict:
            raise IllegalModelAttrAssigendError("'Key' type cannot be assigned")
        if self.__meta__.auto_pk is True:
            if key == self.__table__.pk and not is_loader:
                raise ModifyAutoPkError('Auto_pk model not allowed modify pk')
        if not is_loader and (key not in self.__table__.field_dict):
            raise AttributeError(
                "'Model' object not allowed set attribute '{}'".format(key)
            )

        self.__dict__[key] = value

    def _set_value(self, key, value, is_loader=False):
        return self.__setattr__(key, value, is_loader=is_loader)

    def _get_value(self, key):
        value = self.__getattr__(key)
        if value is None:
            field = self.__table__.field_dict[key]
            value = (field() if callable(field) else field.default)
            Logger.info(f'Using default value for {key}: {value}')
            setattr(self, key, value)
        return value

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
    def _get_add_values(cls, instance, cols):
        if not cols:
            raise AddEmptyInstanceError(
                'Add a instance {} without no data'.format(instance)
            )
        if not isinstance(instance, cls):
            raise ValueError(
                'The object ({}) to be saved is illegal'.format(type(instance))
            )
        return [instance._get_value(c) for c in cols]

    @classmethod
    def _get_insert_sql(cls, cols):
        return SQL.insert.format(
            table_name=cls.__meta__.table,
            cols=', '.join([c.join('``') for c in cols]),
            values=', '.join(['%s']*len(cols))
        )

    @classmethod
    def _save_pk_checker(cls, instance):
        if cls.__meta__.auto_pk is False:
            if instance.__getattr__(cls.__table__.pk) is None:
                raise MissingPKError('Cannot save objects without a primary key')

    @classmethod
    def _get_all_select_cols(cls):
        select_cols = list(cls.__table__.field_dict.keys())
        if cls.__meta__.auto_pk is True:
            select_cols.insert(0, cls.__table__.pk)
        return select_cols

    @classmethod
    async def _create(cls):
        """ do create table """

        return await RequestClient().execute(cls._get_create_sql())

    @classmethod
    async def _drop(cls):
        """ do drop table """

        drop_sql = SQL.drop.format(table_name=cls.__meta__.table)
        return await RequestClient().execute(drop_sql)

    @classmethod
    async def _alter(cls, modify_col=None, add_col=None, drop_col=None):
        """ do alter table """

        if not any([modify_col, add_col, drop_col]):
            return None

        modify_col, add_col, drop_col = to_list(modify_col, add_col, drop_col)

        alter_sql = _Generator.alter(
            cls.__table__, cls.__meta__.table,
            modify_list=modify_col,
            add_list=add_col,
            drop_list=drop_col
        )
        return await RequestClient().execute(alter_sql)

    @classmethod
    async def _show_create(cls):
        """ do show table create """

        show_create_sql = SQL.show.create.format(table_name=cls.__meta__.table)
        result = await RequestClient().fetch(
            show_create_sql, rows=1
        )
        return Dict(
            table_name=result['Table'], create_syntax=result['Create Table']
        )

    @classmethod
    async def _show_struct(cls):
        """ do show table struct """

        table_name = cls.__meta__.table
        show_clo_sql = SQL.show.columns.format(
            table_name=table_name, rows=1
        )
        show_idx_sql = SQL.show.indexs.format(
            table_name=table_name, rows=1
        )
        result = Dict(
            columns=await RequestClient().fetch(show_clo_sql),
            indexs=await RequestClient().fetch(show_idx_sql)
        )
        return result

    @classmethod
    async def _exist(cls):
        """ query table is exist """

        if not RequestClient.is_usable():
            return False
        exist_sql = SQL.exist.format(
            schema=RequestClient.get_conn_info().db.db,
            table=cls.__meta__.table,
        )
        return await RequestClient().exist(exist_sql)

    @classmethod
    def _has_cols_checker(cls, cols):
        if isinstance(cols, str):
            cols = [cols]
        for _c in cols:
            if isinstance(_c, Func):
                continue
            if _c not in cls.__table__.field_dict and _c != cls.__table__.pk:
                raise ValueError(
                    f"Attribute {_c} not in model {cls.__name__}'s query fields"
                )

    @classmethod
    async def _do_add(cls, instance):
        """ do add a instance data """

        cols = list(instance.__dict__.keys())
        values = cls._get_add_values(instance, cols)
        insert_sql = cls._get_insert_sql(cols)
        result = await RequestClient().execute(insert_sql, values=values)
        if result.affected != 1:
            Logger.error(
                f'Failed to insert, affected rows: {result.affected}'
            )
            return None
        if cls.__meta__.auto_pk is True:
            instance._set_value(cls.__table__.pk, result.last_id, is_loader=True)
        else:
            result.last_id = instance.__dict__[cls.__table__.pk]
        return result.last_id

    @classmethod
    async def _add(cls, instance):
        """ add a instance data """

        cls._save_pk_checker(instance)
        return await cls._do_add(instance)

    @classmethod
    async def _batch_add(cls, instances):
        """ batch add instance """

        if not isinstance(instances, (list, tuple)):
            raise ValueError(f'Add illegal type {instances}')

        cols = list(cls.__table__.field_dict.keys())
        cols_copy = cols.copy()
        for c in cols_copy:
            c_type = cls.__table__.field_dict[c]
            if hasattr(c_type, 'auto') and c_type.auto:
                cols.remove(c)
        values = []
        for inst in instances:
            values.append(cls._get_add_values(inst, cols))

        insert_sql = cls._get_insert_sql(cols)
        result = await RequestClient().execute(
            insert_sql, values=values, is_batch=True
        )
        if result.affected != len(values):
            Logger.error(
                f'Failed to insert, affected rows: {result.affected}'
            )
        return result

    @classmethod
    async def _remove(cls, where):
        """ do remove by where condition """

        if not isinstance(where, (_Where, _Logic)):
            raise ValueError('Invalid where type {}'.format(type(where)))
        where_format = where.format_()

        cls._has_cols_checker(where_format.col)
        remove_sql = SQL.delete.format(
            table_name=cls.__meta__.table, condition=where_format.where
        )
        return await RequestClient().execute(
            remove_sql, values=where_format.arg
        )

    @classmethod
    async def _updete(cls, data, where=None):
        """ do update by where condition """

        if not isinstance(data, dict):
            raise ValueError('Invalid data type')

        where_format = None
        if where is not None:
            if not isinstance(where, (_Where, _Logic)):
                raise ValueError('Invalid where type {}'.format(type(where)))
            where_format = where.format_()
            cls._has_cols_checker(where_format.col)

        update_fields = list(data.keys())
        update_values = [data[f] for f in update_fields]
        set_clause = ','.join(['='.join([f, '%s']) for f in update_fields])

        if where_format:
            updete_sql = SQL.update_.complete.format(
                table_name=cls.__meta__.table,
                kv=set_clause,
                condition=where_format.where
            )
            update_values.extend(where_format.arg)
        else:
            updete_sql = SQL.update_.no_where.format(
                table_name=cls.__meta__.table, kv=set_clause
            )
            Logger.warning('Dangerous operation: {}'.format(updete_sql))

        return await RequestClient().execute(updete_sql, values=update_values)

    @classmethod
    async def _get(cls, id_):
        """ get by id """

        select_sql = SQL.select.by_id.format(
            cols=','.join([c.join('``') for c in cls._get_all_select_cols()]),
            table_name=cls.__meta__.table,
            condition=cls.__table__.pk
        )
        result = await RequestClient().fetch(select_sql, args=[id_], rows=1)
        return Loader(cls, result).load()

    @classmethod
    async def _batch_get(cls, id_list, cols=None):
        """ batch get by id list"""

        if not isinstance(id_list, (list, tuple)):
            raise ValueError('id_list must be a list or tuple')
        if cols:
            cls._has_cols_checker(cols)
        else:
            cols = cls._get_all_select_cols()
        select_sql = SQL.select.by_ids.format(
            cols=','.join([c.join('``') for c in cols]),
            table_name=cls.__meta__.table,
            condition=cls.__table__.pk,
            data=tuple(id_list)
        )
        result = await RequestClient().fetch(select_sql)
        return Loader(cls, result).load()

    @classmethod
    def _query(cls, *cols):
        """ model query """

        query_cols = []
        if cols:
            for _c in cols:
                if isinstance(_c, (Func, str)):
                    query_cols.append(_c)
                elif isinstance(_c, BaseField):
                    query_cols.append(_c.name)
            cls._has_cols_checker(query_cols)
        else:
            query_cols = cls._get_all_select_cols()

        return _Generator(cls, query_cols)

    async def _save(self):
        """ save self """

        self._save_pk_checker(self)
        return await self._do_add(self)

    async def _delete(self):
        """ delete self """

        self_pk = self.__table__.pk
        pk_value = self._get_value(self_pk)
        if pk_value is None:
            raise DeleteUnsavedError('Not allowed delete unsaved objects')
        return await self.remove(
            _Where(column=self_pk, operator='==', value=pk_value)
        )
