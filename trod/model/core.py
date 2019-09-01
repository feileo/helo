import warnings
from collections import OrderedDict as OD

from . import table
from .. import types, errors, utils


def with_metaclass(meta, *bases):

    class MetaClass(type):

        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)

        @classmethod
        def __prepare__(cls, name, this_bases):
            return meta.__prepare__(name, bases)

    return type.__new__(MetaClass, 'temporary_class', (), {})


def with_table(m):
    return m.__table__


def with_fields(m):
    return m.__field__


class ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        if name in ("ModelBase", "Model"):
            return type.__new__(cls, name, bases, attrs)

        def prepare(name, attrs):
            bound = attrs.pop("__db__", None)
            table_name = attrs.pop("__table__", None)

            if not table_name:
                table_name = name.lower()
                warnings.warn(
                    f"Did not give the table name, use the model name `{table_name}`",
                    errors.ProgrammingWarning
                )

            table_fields, model_names = OD(), {}
            primary = utils.Tdict(auto=False, field=None, begin=None)
            for attr in attrs.copy():
                if primary.field and attr == primary.field.name:
                    raise errors.DuplicateFieldNameError(f"Duplicate field name `{attr}`")

                field = attrs.pop(attr)
                if isinstance(field, types.__impl__.FieldBase):
                    field.name = field.name or attr
                    if getattr(field, 'primary_key', None):
                        if primary.field is not None:
                            raise errors.DuplicatePKError(
                                f"Duplicate primary key found for field {field.name}"
                            )
                        primary.field = field
                        if getattr(field, "auto", False):
                            primary.auto = True
                            primary.begin = int(field.auto)
                            if field.name != table.Table.AIPK:
                                warnings.warn(
                                    "The field name of AUTO_INCREMENT primary key is suggested \
                                    to use `id` instead of {field.name}",
                                    errors.ProgrammingWarning
                                )

                    model_names[attr] = field.name
                    table_fields[field.name] = field
                elif attr not in table.Table.META:
                    raise errors.InvalidFieldType(f"Invalid model field {attr}")

            if not primary.field:
                raise errors.NoPKError(
                    f"Primary key not found for table `{table_name}`"
                )

            indexes = attrs.pop("__indexes__", ())
            if not isinstance(types.SEQUENCE):
                raise TypeError("")
            for index in indexes:
                if not isinstance(index, types.__impl__.IndexBase):
                    raise errors.InvalidFieldType()

            attrs["__field__"] = model_names
            attrs["__table__"] = table.Table(
                database=bound, name=table_name,
                fields=table_fields, primary=primary, indexes=tuple(indexes),
                charset=attrs.pop("__charset__", None),
                comment=attrs.pop("__comment__", None),
            )
            return attrs

        attrs = prepare(name, attrs)
        return type.__new__(cls, name, bases, attrs)

    @classmethod
    def __getattr__(cls, name):
        fn = with_fields(cls).get(name)
        if fn:
            return str(with_table(cls).fields_dict[fn])

        if name in with_table(cls).META:
            return with_table(cls).__metaattr__(name)

        raise AttributeError(
            f'{cls.__name__} class does not have `{name}` attribute.'
        )

    @classmethod
    def __setattr__(cls, *_args):
        raise errors.ModelSetAttrError(
            f"'{cls.__name__}' class not allow set attribute")


class ModelBase(metaclass=ModelMeta):

    def __init__(self, **kwargs):
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self):
        return "<Model: {0}> for table `{1}`".format(
            self.__class__.__name__, with_table(self).name
        )

    def __str__(self):
        return with_table(self).primary_key.field.name

    def __hash__(self):
        return hash(with_table(self).name)

    def __setattr__(self, name, value):
        self.__setmodel__(name, value)

    def __getattr__(self, name):
        try:
            return self.__dict__[name]
        except KeyError:
            if name in with_fields(self):
                return None
            raise AttributeError(
                f"'{self.__class__}' object has no attribute '{name}'"
            )

    # def __delattr__(self, name):
    #     pass

    # async def __iter__(self):
    #     return iter(await self.select().all())

    # def __getitem__(self, key):
    #     return self.get_by_id(key)

    # def __setitem__(self, key, value):
    #     self.set_by_id(key, value)

    # def __delitem__(self, key):
    #     self.delete_by_id(key)

    # def __contains__(self, key):
    #     try:
    #         self.get_by_id(key)
    #     except self.DoesNotExist:
    #         return False
    #     else:
    #         return True

    # def __len__(self):
    #     return self.select().count()

    def __bool__(self):
        return True

    def __isaipk__(self, name):
        pk = with_table(self).primary
        return pk.auto and name == pk.field.name

    def __setmodel__(self, name, value, __load=False):
        fn = with_fields(self).get(name)
        if not fn:
            raise errors.SetNoAttrError(name)

        if not __load and self.__isaipk__(fn):
            raise errors.ModifyAutoPkError()

        f = with_table(self).fields_dict[fn]
        try:
            value = f.py_value(f)
        except (ValueError, TypeError):
            raise errors.SetInvalidColumnsValueError()

        self.__dict__[name] = value

    @property
    def __self__(self):
        values = OD()
        for n, v in self.__dict__.items():
            values[with_fields(self)[n]] = v

        return values


class Api:

    @classmethod
    async def create_table(cls, m, **options):
        """ Do create table """

        return await with_table(m).create(**options)

    @classmethod
    async def drop_table(cls, m, **options):
        """ Do drop table """

        return await with_table(m).drop(**options)

    @classmethod
    def alter(cls, m):

        return with_table(m).show()

    @classmethod
    def show(cls, m):

        return with_table(m).alter()

    @classmethod
    async def get(cls, m, _id):

        return await table.Select(
            m, with_table(m).columns
        ).where(
            with_table(m).fields_dict[with_table(m).primary.name] == _id
        ).first()

    @classmethod
    async def get_many(cls, m, ids, columns=None):

        columns = columns or with_table(m).columns

        return await table.Select(
            m, columns
        ).where(
            with_table(m).fields_dict[with_table(m).primary.name].in_(ids)
        ).all()

    @classmethod
    def add(cls, m, instance):

        rows = Rows(instance.__self__)
        return table.Insert(with_table(m), rows)

    @classmethod
    def add_many(cls, m, instances):

        rows = Rows([instance.__self__ for instance in instances])
        return table.Insert(with_table(m), rows)

    @classmethod
    def select(cls, m, *columns, distinct=False):

        columns = columns or with_table(m).columns
        return table.Select(m, columns, distinct=distinct)

    @classmethod
    def _get_default_row(cls, m):

        insert_data = OD()
        for col in with_table(m).fields_dict:
            if m.__isaipk__(col):
                continue
            insert_data[col] = col.default() if callable(col.default) else col.default

        return insert_data

    @classmethod
    def _gen_insert_row(cls, m, row_data):

        insert_data = {}
        for c, v in row_data.items():
            if isinstance(c, types.__impl__.FieldBase):
                c = c.name
            if m.__isaipk__(c):
                raise errors.ModifyAutoPkError()
            insert_data[c] = v

        cleaned_data = cls._get_default_row(m)
        for col in cleaned_data:
            v = insert_data.pop(col, None)
            f = with_table(m).fields_dict[col]
            if v is None and not f.null:
                raise errors.InvalidColumnsVlaueError()
            cleaned_data[col] = f.db_type(v)

        if insert_data:
            for c in insert_data:
                raise errors.NoSuchColumnError(c)

        return cleaned_data

    @classmethod
    @utils.argschecker(row=dict)
    def insert(cls, m, row):
        """
        # Using keyword arguments:
        zaizee_id = Person.insert(first='zaizee', last='cat').execute()

        # Using column: value mappings:
        Note.insert({
        Note.person_id: zaizee_id,
        Note.content: 'meeeeowwww',
        Note.timestamp: datetime.datetime.now()}).execute()
        """

        if not row:
            raise ValueError("No data to insert.")

        cleaned_data = cls._gen_insert_row(m, row)
        return table.Insert(with_table(m), Rows(cleaned_data))

    @classmethod
    @utils.argschecker(rows=types.SEQUENCE, columns=types.SEQUENCE)
    def insert_many(cls, m, rows, columns=None):
        """
        people = [
            {'first': 'Bob', 'last': 'Foo'},
            {'first': 'Herb', 'last': 'Bar'},
            {'first': 'Nuggie', 'last': 'Bar'}]

        # Inserting multiple rows returns the ID of the last-inserted row.
        last_id = Person.insert(people).execute()

        # We can also specify row tuples, so long as we tell Peewee which
        # columns the tuple values correspond to:
        people = [
            ('Bob', 'Foo'),
            ('Herb', 'Bar'),
            ('Nuggie', 'Bar')]
        Person.insert(people, columns=[Person.first, Person.last]).execute()
        """
        if not rows:
            raise ValueError("No data to insert.")

        if columns:
            for c in columns:
                if c not in with_table(m).fields_dict:
                    raise errors.NoSuchColumnError(c)

        cleaned_rows = []
        for row in rows:
            if isinstance(row, types.SEQUENCE):
                if not columns:
                    raise ValueError("Bulk insert must specify columns.")
                row = dict(zip(row, columns))
            elif not isinstance(row, dict):
                raise ValueError()

            cleaned_rows.append(cls._gen_insert_row(m, row))

        return table.Insert(with_table(m).name, Rows(cleaned_rows))

    @classmethod
    def update(cls, m, **values):

        return table.Update(with_table(m), values)

    @classmethod
    def delete(cls, m):

        return table.Delete(with_table(m))

    @classmethod
    def replace(cls, m, **values):

        return table.Replace(with_table(m), Rows(values))

    @classmethod
    async def save(cls, mo):
        """ save mo """

        row = Rows(cls._gen_insert_row(mo, mo.__self__))
        result = await table.Replace(with_table(mo).name, row).do()
        mo.__setmodel__(with_table(mo).name, result.last_id, __load=True)
        return result

    @classmethod
    async def remove(cls, mo):
        """ delete mo """

        primary = mo.__getattr__(with_table(mo).primary.field.name)
        if not primary:
            raise RuntimeError()

        return await table.Delete(
            with_table(mo).name
        ).where(with_table(mo).primary.field == primary).do()


class Rows:

    def __init__(self, rows):

        if isinstance(rows, dict):
            self._columns = list(rows.keys())
            self._values = [tuple(rows.values())]
        elif isinstance(rows, list):
            self._columns = list(rows[0].keys())
            self._values = [tuple(r.values()) for r in rows]

    @property
    def columns(self):
        return ["``".join(c) for c in self._columns]

    @property
    def values(self):
        return self._values

    @property
    def spec(self):
        return ["'%s'"] * len(self._columns)


class Loader:
    pass


# def load(results, model, use_tdict):

#     if not results:
#         return _empty(results, model, use_tdict)

#     if use_tdict:
#         return utils.formattdict(results)
#     return _load_to_model(results, model)


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
