import warnings
from collections import OrderedDict

from trod import types, errors, utils
from trod.model import tables


class ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        if name in ("Model", "TrodModel"):
            return type.__new__(cls, name, bases, attrs)

        def __prepare__(name, attrs):
            bound = attrs.pop("__db__", None)
            table_name = attrs.pop("__table__", None)
            if not table_name:
                warnings.warn(
                    f"Did not give the table name, use the model name `{name}`",
                    errors.ProgrammingWarning
                )
            table_name = name.lower()

            fields = OrderedDict()
            primary = utils.Tdict(auto=False, field=None, ai=None)

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
                        if field.ai:
                            primary.auto = True
                            primary.ai = int(field.ai)
                            if field.name != tables.Table.AIPK:
                                warnings.warn(
                                    "The field name of AUTO_INCREMENT primary key is suggested \
                                    to use `id` instead of {field.name}",
                                    errors.ProgrammingWarning
                                )

                    fields[field.name] = field
                elif attr not in tables.Table.DEFAULT:
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

            attrs['__table__'] = tables.Table(
                database=bound, name=table_name,
                fields=fields, primary=primary, indexes=tuple(indexes),
                charset=attrs.pop('__charset__', None),
                comment=attrs.pop('__comment__', None),
            )
            return attrs

        attrs = __prepare__(name, attrs)
        return type.__new__(cls, name, bases, attrs)

    # def __getattr__(cls, key):
    #     try:
    #         value = getattr(cls, key)
    #     except AttributeError:
    #         if key in cls.__table__.fields_dict:
    #             value = cls.__table__.fields_dict[key]
    #         else:
    #             raise AttributeError(
    #                 f"'{cls.__name__}' class does not have `{key}` attribute"
    #             )
    #     return value

    def __setattr__(cls, _key, _value):
        raise errors.ModelSetAttrError(
            f"'{cls.__name__}' class not allow set attribute")


class Model(metaclass=ModelMeta):

    def __init__(self, **kwargs):
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    # def __repr__(self):
    #     return "<{0}(table '{1}': {2})>".format(
    #         self.__class__.__name__, self.__table__.name, self.__table__.comment
    #     )

    # __str__ = __repr__

    def __hash__(self):
        pass

    # def __getattribute__(self, name):
    #     pass

    def __getattr__(self, key):
        # attr 和 field name 不一样的情况
        try:
            return self.__dict__[key]
        except KeyError:
            if key == self.__table__.primary.field.name or key in self.__table__.fields_dict:
                value = None
            else:
                raise AttributeError(
                    f"'{self.__class__.__name__}' object has no attribute '{key}'"
                )
            return value

    def __setattr__(self, key, value, is_loader=False):
        # attr 和 field name 不一样的情况
        if self.__table__.primary.auto is True:
            if key == self.__table__.primary.field.name:
                raise errors.ModifyAutoPkError(
                    'AUTO_INCREMENT table not allowed modify primary key'
                )
        if not is_loader and (key not in self.__table__.fields_dict):
            raise AttributeError(
                f"'{self.__class.__name__}' object not allowed set attribute '{key}'"
            )

        self.__dict__[key] = value

    def __delattr__(self, name):
        pass

    @property
    def __self__(self):
        # attr 和 field name 在这里做转换
        fields = [f for f in self.__table__.fields_dict]
        values = OrderedDict()
        for f in fields:
            v = self.__getattr__(f.name)
            if v is None:
                v = f.default() if callable(f.default) else f.default
            if v is None and not f.null:
                raise errors.InvalidColumnsVlaueError()
            values[f.name] = v
        return values


def with_table(m):
    return m.__table__


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

        return await tables.Select(
            m, with_table(m).columns
        ).where(
            with_table(m).fields_dict[with_table(m).primary.name] == _id
        ).first()

    @classmethod
    async def get_many(cls, m, ids, columns=None):

        columns = columns or with_table(m).columns

        return await tables.Select(
            m, columns
        ).where(
            with_table(m).fields_dict[with_table(m).primary.name].in_(ids)
        ).all()

    @classmethod
    def add(cls, m, instance):

        rows = Rows(instance.__self__)
        return tables.Insert(with_table(m), rows)

    @classmethod
    def add_many(cls, m, instances):

        rows = Rows([instance.__self__ for instance in instances])
        return tables.Insert(with_table(m), rows)

    @classmethod
    def select(cls, m, *columns, distinct=False):

        columns = columns or with_table(m).columns
        return tables.Select(m, columns, distinct=distinct)

    @classmethod
    def _get_default_insert(cls, m):
        insert_data = OrderedDict()
        for col in with_table(m).fields_dict:
            if cls._isai(m, col):
                continue
            insert_data[col] = col.default() if callable(col.default) else col.default
        return insert_data

    @classmethod
    def _isai(cls, m, col):
        return col == with_table(m).primary_key.field.name

    @classmethod
    def _generate_insert_row(cls, m, row):

        insert_data = {}
        for c, v in row.items():
            if isinstance(c, types.__impl__.FieldBase):
                c = c.name
            if cls._isai(m, c):
                raise errors.ModifyAutoPkError()
            insert_data[c] = v

        cleaned_data = cls._get_default_insert(m)
        for col in cleaned_data:
            v = insert_data.pop(col, None)
            f = m.__tbale__.fields_dict[col]
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

        cleaned_data = cls._generate_insert_row(m, row)
        return tables.Insert(with_table(m), Rows(cleaned_data))

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

            cleaned_rows.append(cls._generate_insert_row(m, row))

        return tables.Insert(with_table(m).name, Rows(cleaned_rows))

    @classmethod
    def update(cls, m, **values):

        return tables.Update(with_table(m), values)

    @classmethod
    def delete(cls, m):

        return tables.Delete(with_table(m))

    @classmethod
    def replace(cls, m, **values):

        return tables.Replace(with_table(m), Rows(values))

    @classmethod
    async def save(cls, mo):
        """ save mo """

        row = Rows(cls._generate_insert_row(mo, mo.__self__))
        result = await tables.Replace(with_table(mo).name, row).do()
        mo.__setattr__(with_table(mo).name, result.last_id)
        return result

    @classmethod
    async def remove(cls, mo):
        """ delete mo """

        primary = mo.__getattr__(with_table(mo).primary.field.name)
        if not primary:
            raise RuntimeError()

        return await tables.Delete(
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
