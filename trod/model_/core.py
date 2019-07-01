import warnings
from collections import OrderedDict

from trod import types_ as types, errors, utils, db_ as db
from trod.model_ import crud, session


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
                        if field.name != Table.AIPK:
                            warnings.warn(
                                "The field name of AUTO_INCREMENT primary key is suggested \
                                to use `id` instead of {field.name}",
                                errors.ProgrammingWarning
                            )

                fields[attr] = field

            elif isinstance(field, types.index.IndexBase):
                field.name = field.name or attr
                indexs[attr] = field
            elif attr not in Table.DEFAULT:
                raise errors.InvalidFieldType('Invalid model field {}'.format(attr))

        if not pk.field:
            raise errors.NoPKError(
                f"Primary key not found for table `{table_name}`"
            )

        attrs['__table__'] = Table(
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
    async def _create(cls, bind):
        """ Do create table """

        return await cls.__table__.create(bind)

    @classmethod
    async def _drop(cls, bind):
        """ Do drop table """

        return await cls.__table__.drop(bind)

    @classmethod
    async def _exist(cls, bind):
        """ query table is exist """

        return await cls.__table__.exist(bind)

    @classmethod
    async def _show(cls, bind):

        return await cls.__table__.show(bind)

    @classmethod
    async def _add_index(cls, bind):

        return await cls.__table__.add_index(bind)

    @classmethod
    def _normalize_data(cls, data, kwargs):
        pass

    @classmethod
    async def _get(cls, bind, _id):

        pass

    @classmethod
    async def _get_many(cls, bind, ids, *fields):

        pass

    @classmethod
    async def _select(cls, bind, *fields):

        return crud.Select(bind, cls.__table__.name, *fields)

    @classmethod
    async def _insert(cls, bind, **values):

        rows = [values]
        return crud.Insert(bind, cls.__table__.name, *rows)

    @classmethod
    async def _insert_many(cls, bind, *rows):

        return crud.Insert(bind, cls.__table__.name, *rows)

    @classmethod
    async def _update(cls, bind, **values):

        return crud.Update(bind, cls.__table__.name, **values)

    @classmethod
    async def _delete(cls, bind):

        return crud.Delete(bind, cls.__table__.name)

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

        return session.Session(self)

    async def _remove(self):
        """ delete self """

        return crud.Delete(self)


class Table(db.SQL):

    AIPK = 'id'
    DEFAULT = utils.TrodDict(
        __table__=None,
        __auto_increment__=1,
        __engine__='InnoDB',
        __charset__='utf8',
        __comment__='',
    )

    def __init__(self, bind, name, fields, indexs=None, pk=None,
                 engine=None, charset=None, comment=None):
        self.name = name
        self.fields = fields
        self.indexs = indexs
        self.pk = pk
        self.auto_increment = pk.ai
        self.engine = engine or self.DEFAULT.__engine__
        self.charset = charset or self.DEFAULT.__charset__
        self.comment = comment or self.DEFAULT.__comment__
        super().__init__(bind)

    def do(self):
        pass

    def create(self):
        pass

    def drop(self):
        pass

    def show(self):
        pass

    def exist(self):
        pass

    def add_index(self):
        pass
