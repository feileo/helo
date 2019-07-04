import warnings
from collections import OrderedDict

from trod import types_ as types, errors, utils
from trod.model_ import crud, table


class _ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        if name in ('_Model', '_TrodModel'):
            return type.__new__(cls, name, bases, attrs)

        attrs = cls.__prepare__(name, attrs)
        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, key):
        if key in cls.__table__.fields:
            value = cls.__table__.fields[key]
        elif key in cls.__table__.indexs:
            value = cls.__table__.indexs[key]
        else:
            raise AttributeError(
                f"'{cls.__name__}' class does not have `{key}` attribute"
            )
        return value

    def __setattr__(cls, _key, _value):
        raise errors.ModelSetAttrError(
            f"'{cls.__name__}' class not allow set attribute")

    def __prepare__(cls, name, attrs):
        table_name = attrs.pop('__table__', None)
        if not table_name:
            warnings.warn(
                f'Did not give the table name, use the model name `{name}`',
                errors.ProgrammingWarning
            )
        table_name = name

        fields, indexs = OrderedDict(), OrderedDict()
        pk = utils.TrodDict(auto=False, field=None, ai=None)

        for attr in attrs.copy():
            if pk.field and attr == pk.field.name:
                raise errors.DuplicateFieldNameError(f'Duplicate field name `{attr}`')

            field = attrs.pop(attr)

            if isinstance(field, types.field.FieldBase):
                field.name = field.name or attr
                if getattr(field, 'pk', None):
                    # if hasattr(field, 'pk') and field.pk:
                    if pk.field is not None:
                        raise errors.DuplicatePKError(
                            f'Duplicate primary key found for field {field.name}'
                        )
                    pk.field = field
                    if field.ai:
                        pk.auto = True
                        pk.ai = int(field.ai)
                        if field.name != table.Table.AIPK:
                            warnings.warn(
                                "The field name of AUTO_INCREMENT primary key is suggested \
                                to use `id` instead of {field.name}",
                                errors.ProgrammingWarning
                            )

                fields[attr] = field

            elif isinstance(field, types.index.IndexBase):
                field.name = field.name or attr
                indexs[attr] = field
            elif attr not in table.Table.DEFAULT:
                raise errors.InvalidFieldType('Invalid model field {}'.format(attr))

        if not pk.field:
            raise errors.NoPKError(
                f"Primary key not found for table `{table_name}`"
            )

        attrs['__table__'] = table.Table(
            name=table_name, fields=fields, indexs=indexs, pk=pk,
            engine=attrs.pop('__engine__', None),
            charset=attrs.pop('__charset__', None),
            comment=attrs.pop('__comment__', None),
        )
        return attrs


class _Model(metaclass=_ModelMeta):

    def __init__(self, **kwargs):
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self):
        return "<{0}(table '{1}': {2})>".format(
            self.__class__.__name__, self.__table__.name, self.__table__.comment
        )

    __str__ = __repr__

    def __getattr__(self, key):
        try:
            return self.__dict__[key]
        except KeyError:
            if key == self.__table__.pk.field.name or key in self.__table__.fields:
                value = None
            elif key in self.__table__.indexs:
                value = self.__table__.indexs[key]
            else:
                raise AttributeError(
                    f"'{self.__class__.__name__}' object has no attribute '{key}'"
                )
            return value

    def __setattr__(self, key, value, is_loader=False):
        if key in self.__table__.indexs:
            raise errors.IllegalModelAttrAssigendError("'Key' type cannot be assigned")
        if self.__table__.pk.auto is True:
            if key == self.__table__.pk.field.name and not is_loader:
                raise errors.ModifyAutoPkError(
                    'AUTO_INCREMENT table not allowed modify primary key'
                )
        if not is_loader and (key not in self.__table__.fields):
            raise AttributeError(
                f"'{self.__class.__name__}' object not allowed set attribute '{key}'"
            )

        self.__dict__[key] = value

    @utils.troddict_formatter
    def __self__(self):
        return self.__dict__

    @classmethod
    async def _create_table(cls):
        """ Do create table """

        return await cls.__table__.create()

    @classmethod
    async def _drop(cls):
        """ Do drop table """
        return await cls.__table__.drop()

    # @classmethod
    # async def _exist(cls):
    #     """ query table is exist """

    #     return await cls.__table__.exist()

    # @classmethod
    # async def _show(cls):

    #     return await cls.__table__.show()

    # @classmethod
    # async def _add_index(cls):

    #     return await cls.__table__.add_index()

    # @classmethod
    # def _normalize_data(cls, data, kwargs):
    #     pass

    @classmethod
    async def _get(cls, _id):

        fields = [f for f in cls.__table__.fields]
        return await crud.Select(
            cls.__table__.name, *fields
        ).where(
            cls.__table__.fields[cls.__table__.pk.name] == _id
        ).all()

    @classmethod
    async def _get_many(cls, ids, *fields):
        pass

    @classmethod
    async def _select(cls, *fields):

        fields = ['`{f.name}`' for f in fields]
        return crud.Select(cls.__table__.name, *fields)

    @classmethod
    async def _insert(cls, **values):

        rows = [values]
        return crud.Insert(cls.__table__.name, rows)

    @classmethod
    async def _insert_many(cls, rows, fields=None):

        return crud.Insert(cls.__table__.name, rows, fields=fields)

    @classmethod
    async def _add(cls, instance):
        pass

    @classmethod
    async def _add_many(cls, instances):
        pass

    @classmethod
    async def _update(cls, **values):

        return crud.Update(cls.__table__.name, **values)

    @classmethod
    async def _delete(cls):

        return crud.Delete(cls.__table__.name)

    async def _save(self):
        """ save self
            a = M()
            a.f = 1
            a.save()
            print(a.f)  # 1

            a.f=2
            a.save()
            print(a.f) # 2
        """

        return crud.Replace(self)

    async def _remove(self):
        """ delete self """

        return crud.Delete(self)
