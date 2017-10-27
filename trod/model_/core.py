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
        pk = utils.Tdict(auto=False, field=None, ai=None)

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

    def __hash__(self):
        pass

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

    @property
    @utils.tdictformatter
    def __self__(self):
        fields = [f for f in self.__table__.fields]
        values = {}
        for f in fields:
            v = self.__getattr__(f.name)
            if v is None and callable(f):
                v = f()
            if v is not None:
                values[f.name] = v
        return values

    @classmethod
    async def _create_table(cls, **options):
        """ Do create table """

        return await cls.__table__.create(**options)

    @classmethod
    async def _drop_table(cls, **options):
        """ Do drop table """

        return await cls.__table__.drop(**options)

    @classmethod
    def _show(cls):

        return table.Show(cls.__table__)

    @classmethod
    def _normalize_data(cls, data, kwargs):
        pass

    @classmethod
    async def _get(cls, _id, tdicts=False):

        fields = [f.sname for f in cls.__table__.fields]
        return await crud.Select(
            cls, fields
        ).where(
            cls.__table__.fields[cls.__table__.pk.name] == _id
        ).first(tdicts)

###############################################################################
    @classmethod
    async def _get_many(cls, ids, *fields, tdicts=False):

        fields = fields or cls.__table__.fields
        fields = [f.sname for f in fields]

        return await crud.Select(
            cls, fields
        ).where(
            cls.__table__.fields[cls.__table__.pk.name].in_(ids)
        ).all(tdicts)

    @classmethod
    def _add(cls, instance):

        rows = Rows([instance.__self__])
        return crud.Insert(cls.__table__.name, rows)

    @classmethod
    def _add_many(cls, instances):

        rows = Rows([instance.__self__ for instance in instances])
        return crud.Insert(cls.__table__.name, rows)

    @classmethod
    def _select(cls, *fields, distinct=False):

        fields = fields or cls.__table__.fields
        fields = [f.sname for f in fields]

        return crud.Select(cls, fields, distinct=distinct)

    @classmethod
    def _insert(cls, **values):

        rows = Rows([values])
        return crud.Insert(cls.__table__.name, rows)

    @classmethod
    def _insert_many(cls, rows, fields=None):

        rows = Rows(rows, fields=fields)
        return crud.Insert(cls.__table__.name, rows)

    @classmethod
    def _update(cls, **values):

        return crud.Update(cls.__table__.name, values)

    @classmethod
    def _delete(cls):

        return crud.Delete(cls.__table__.name)

    @classmethod
    def _replace(cls, **values):

        rows = Rows([values])
        return crud.Replace(cls.__table__.name, rows)

    async def _save(self):
        """ save self """

        rows = Rows([self.__self__])
        result = await crud.Replace(self.__table__.name, rows).do()
        self.__setattr__(self.__table__.name, result.last_id)
        return result

    async def _remove(self):
        """ delete self """

        pk = self.__getattr__(self.__table__.pk.field.name)
        if not pk:
            raise RuntimeError()  # TODO

        return await crud.Delete(
            self.__table__.name
        ).where(self.__table__.pk.field == pk).do()


class Rows:

    def __init__(self, rows, fields=None):
        pass

    @property
    def fields(self):
        pass

    @property
    def values(self):
        pass


def load(results, model, use_tdict):

    if not results:
        return _empty(results, model, use_tdict)

    if use_tdict:
        return utils.formattdict(results)
    return _load_to_model(results, model)


def _empty(results, model, use_tdict):
    if isinstance(results, dict):
        if use_tdict:
            return utils.Tdict()
        return model()
    if isinstance(results, (list, tuple)):
        if use_tdict:
            return [utils.Tdict()]
        return FetchResult()

    raise ValueError()


def _load_to_model(results, model):

    # TODO func field
    def _do(results, model):
        model = model()
        for key, value in results.items():
            model.set_value(key, value, is_loader=True)
        return model

    if isinstance(results, dict):
        return _do(results, model)
    if isinstance(results, (list, tuple)):
        return FetchResult([_do(r, model) for r in results])

    raise ValueError()


class FetchResult(list):

    def __repr__(self):
        pass

    __str__ = __repr__

    def __iter__(self):
        """ for x in self """
        pass

    def __getitem__(self, idx):
        """ self[key] """
        pass

    def __contains__(self, value):
        """ value in self, value not in self """
        pass


class ExecResults:
    def __init__(self, affected, last_id):
        self.affected = affected
        self.last_id = last_id

    def __repr__(self):
        pass

    __str__ = __repr__
