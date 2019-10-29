"""
    trod.model._impl
    ~~~~~~~~~~~~~

    Implements the model core.
"""
from __future__ import annotations

import warnings
from typing import Any, Dict, Optional, List, Union, Tuple, Type

from .. import db, util, err
from ..g import _helper as gh, SQL, SEQUENCE, ENCODINGS, and_, RT
from ..types._impl import Id, IdList, FieldBase, IndexBase


class Table(gh.Node):

    __slots__ = (
        "db", "name", "fields_dict", "fields", "primary",
        "indexes", "auto_increment", "engine", "charset",
        "comment",
    )

    AIPK = 'id'
    META = util.tdict(
        __db__=None,
        __table__=None,
        __indexes__=None,
        __auto_increment__=1,
        __engine__='InnoDB',
        __charset__=ENCODINGS.utf8,
        __comment__='',
    )

    def __init__(
        self,
        database: Optional[str],
        name: str,
        fields: Dict[str, FieldBase],
        primary: util.tdict,
        indexes: Optional[List[IndexBase]] = None,
        engine: Optional[str] = None,
        charset: Optional[str] = None,
        comment: Optional[str] = None
    ) -> None:
        self.db = database
        self.name = name
        self.fields_dict = fields
        self.fields = list(fields.values())
        self.primary = primary
        self.indexes = indexes
        self.auto_increment = primary.begin or self.META.__auto_increment__
        self.engine = engine or self.META.__engine__
        self.charset = charset or self.META.__charset__
        self.comment = comment or self.META.__comment__

    @property
    def table_name(self) -> str:
        if self.db:
            return f"`{self.db}`.`{self.name}`"
        return f"`{self.name}`"

    def __repr__(self) -> str:
        return f"<Table {self.table_name}>"

    def __str__(self) -> str:
        return self.name

    def __metaattr__(self, name: str) -> Any:
        attr = name.strip("__")
        if attr == "table":
            return self.name
        return getattr(self, attr)

    def __sql__(self, ctx: gh.Context):
        ctx.literal(self.table_name)
        return ctx


class ModelType(type):

    def __new__(cls, name: str, bases: tuple, attrs: dict) -> type:

        def __prepare__(name, attrs: dict):

            bound = attrs.pop("__db__", None)
            table_name = attrs.pop("__table__", None)
            if not table_name:
                table_name = name.lower()
                warnings.warn(
                    "Did not give the table name, "
                    f"use the model name `{table_name}`",
                    err.ProgrammingWarning
                )

            model_fields, field_names = {}, {}
            primary = util.tdict(auto=False, field=None, begin=None)
            for attr in attrs.copy():
                if primary.field and attr == primary.field.name:
                    raise err.DuplicateFieldNameError(
                        f"Duplicate field name `{attr}`"
                    )

                field = attrs[attr]
                if isinstance(field, FieldBase):
                    field.name = field.name or attr
                    if getattr(field, 'primary_key', None):
                        if primary.field is not None:
                            raise err.DuplicatePKError(
                                "Duplicate primary key found for field "
                                f"{field.name}"
                            )
                        primary.field = field
                        if getattr(field, "auto", False):
                            primary.auto = True
                            primary.begin = int(field.auto)
                            if field.name != Table.AIPK:
                                warnings.warn(
                                    "The field name of AUTO_INCREMENT "
                                    "primary key is suggested to use "
                                    f"`id` instead of {field.name}",
                                    err.ProgrammingWarning
                                )

                    model_fields[attr] = field
                    field_names[field.name] = attr
                    attrs.pop(attr)
                elif attr not in Table.META:
                    if not (attr.endswith('__') and attr.endswith('__', 0, 2)):
                        raise err.InvalidFieldType(
                            f"Invalid model field {attr}"
                        )

            if not primary.field:
                raise err.NoPKError(
                    f"Primary key not found for table `{table_name}`"
                )

            indexes = attrs.pop("__indexes__", [])
            if not isinstance(indexes, SEQUENCE):
                raise TypeError("")
            for index in indexes:
                if not isinstance(index, IndexBase):
                    raise err.InvalidFieldType()

            attrs["__names__"] = field_names
            attrs["__table__"] = Table(
                database=bound, name=table_name,
                fields=model_fields,
                primary=primary, indexes=list(indexes),
                charset=attrs.pop("__charset__", None),
                comment=attrs.pop("__comment__", None),
            )

            return attrs

        if name not in ("ModelBase", "Model"):
            attrs = __prepare__(name, attrs)

        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, name: str) -> Any:
        attr = cls.__table__.field_dict.get(name)
        if attr:
            return attr

        if name in cls.__table__.META:
            return cls.__table__.__metaattr__(name)

        raise AttributeError(
            f"'{cls.__name__}' class does not have attribute '{name}'."
        )

    def __setattr__(cls, *_args: Any) -> None:
        raise err.ModelSetAttrError(
            f"Model '{cls.__name__}' class not allow set attribute")

    def __repr__(cls) -> str:
        return f"trod.model.{cls.__name__}"

    def __str__(cls) -> str:
        return cls.__name__

    def __delattr__(cls, name: str) -> None:
        raise err.ProgrammingError()

    def __aiter__(cls) -> Select:
        return Api.select(cls)  # type: ignore


class ModelBase:

    def __init__(self, **kwargs: Any) -> None:
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} object>"

    def __str__(self) -> str:
        return self.__table__.name

    def __hash__(self) -> int:
        return hash(self.__table__.name)

    def __setattr__(self, name: str, value: Any) -> None:
        self.__setmodel__(name, value)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__dict__[name]
        except KeyError:
            if name in self.__table__.field_dict:
                return None
            raise AttributeError(
                f"'{self.__class__}' object has no attribute '{name}'"
            )

    def __bool__(self) -> bool:
        return bool(self.__dict__)

    def __setmodel__(
            self, name: str, value: Any, __load: bool = False
    ) -> None:
        f = self.__table__.field_dict.get(name)
        if not f:
            raise err.SetNoAttrError(name)

        if not __load and (f.primary_key and f.auto):
            raise err.ModifyAutoPkError()

        try:
            value = f.py_value(value)
        except (ValueError, TypeError):
            raise err.SetInvalidColumnsValueError()

        self.__dict__[name] = value

    @property
    def __self__(self) -> Dict[str, Any]:
        values = {}
        for n, v in self.__dict__.items():
            values[self.__table__.field_dict[n].name] = v
        return values

    def __sql__(self, ctx):
        return ctx


class Model(gh.with_metaclass(ModelType, ModelBase)):  # type:ignore

    @classmethod
    async def create(cls, **options: Any) -> db.ExecResult:
        return await Api.create_table(cls, **options)

    @classmethod
    async def drop(cls, **options: Any) -> db.ExecResult:
        return await Api.drop_table(cls, **options)

    @classmethod
    def alter(cls) -> Alter:
        return Api.alter(cls)

    @classmethod
    def show(cls) -> Show:
        return Api.show(cls)

    @classmethod
    async def get(cls, _id: Id) -> Union[db.FetchResult, None]:
        return await Api.get(cls, _id)

    # model
    #
    @classmethod
    async def mget(
            cls, ids: IdList, columns: Optional[List[FieldBase]] = None
    ) -> db.FetchResult:
        return await Api.get_many(cls, ids, columns=columns)

    @classmethod
    async def add(cls, instance: Model) -> db.ExecResult:
        return await Api.add(cls, instance)

    @classmethod
    async def madd(cls, instances: List[Model]) -> db.ExecResult:
        return await Api.add_many(cls, instances)

    @classmethod
    async def set(cls, _id: Id, **values: Any) -> db.ExecResult:
        return await Api.set(cls, _id, values)

    # direct
    #
    @classmethod
    def select(cls, *columns: FieldBase) -> Select:
        return Api.select(cls, *columns)

    @classmethod
    def insert(
            cls, data: Optional[Dict[str, Any]] = None, **ists: Any
    ) -> Insert:
        return Api.insert(cls, data or ists)

    @classmethod
    def minsert(
            cls,
            rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
            columns: Optional[List[FieldBase]] = None
    ) -> Insert:
        return Api.insert_many(cls, rows, columns=columns)

    @classmethod
    def update(cls, **values: Any) -> Update:
        return Api.update(cls, values)

    @classmethod
    def delete(cls) -> Delete:
        return Api.delete(cls)

    @classmethod
    def replace(cls, **values: Any) -> Replace:
        return Api.replace(cls, values)

    @classmethod
    def mreplace(cls, **values: Any) -> Replace:
        return Api.replace_many(cls, values)

    # instance
    #
    async def save(self) -> None:
        await Api.save(self)

    async def remove(self) -> bool:
        await Api.remove(self)
        return True


def with_table(m: Union[Type[Model], Model]) -> Table:
    try:
        return m.__table__
    except AttributeError:
        raise err.ProgrammingError("Must be ModelType")


class Api:

    # table

    @classmethod
    async def create_table(
        cls, m: Type[Model], **options: Any
    ) -> db.ExecResult:
        """ Do create table """

        return await Create(with_table(m), **options).do()

    @classmethod
    async def drop_table(
        cls, m: Type[Model], **options: Any
    ) -> db.ExecResult:
        """ Do drop table """

        return await Drop(with_table(m), **options).do()

    @classmethod
    def alter(cls, m: Type[Model]) -> Alter:

        return Alter(with_table(m))

    @classmethod
    def show(cls, m: Type[Model]) -> Show:

        return Show(with_table(m))

    # simple

    @classmethod
    async def get(
        cls, m: Type[Model], _id: Id
    ) -> Union[db.FetchResult, None]:
        return await Select(
            with_table(m).fields, m
        ).where(
            with_table(m).primary.field == _id
        ).first()

    @classmethod
    async def get_many(
        cls,
        m: Type[Model],
        ids: IdList,
        columns: Optional[List[FieldBase]] = None
    ) -> db.FetchResult:

        columns = columns or with_table(m).fields

        return await Select(
            columns, m
        ).where(
            with_table(m).primary.field.in_(ids)
        ).all()

    @classmethod
    async def set(
        cls,
        m: Type[Model],
        _id: Id,
        values: Any
    ) -> db.ExecResult:

        return Update(
            with_table(m), values
        ).where(with_table(m).primary.field == _id).do()

    @classmethod
    async def add(cls, m: Type[Model], instance: Model) -> db.ExecResult:

        row = Values(instance.__self__)
        return await Insert(with_table(m), row).do()

    @classmethod
    async def add_many(
        cls, m: Type[Model], instances: List[Model]
    ) -> db.ExecResult:

        rows = Values([instance.__self__ for instance in instances])
        return await Insert(with_table(m), rows).do()

    # statement

    @classmethod
    def select(
        cls, m: Type[Model], *columns: FieldBase
    ) -> Select:

        columns = columns or with_table(m).fields  # type: ignore
        return Select(list(columns), m)

    @classmethod
    @util.argschecker(row=dict)
    def insert(
        cls, m: Type[Model], row: Dict[str, Any]
    ) -> Insert:
        """
        # Using keyword arguments:
        >>> zaizee_id = Person.insert(first='zaizee', last='cat').do()

        # Using value mappings:
        >>> Person.insert({
            'first': 'zsizee',
            'last': 'meeeeowwww',
            'timestamp': datetime.datetime.now()
            }).do()

        or column value mappings:
        >>> Person.insert({
            Person.first: 'zsizee',
            Person.last: 'meeeeowwww',
            Person.timestamp: datetime.datetime.now()
            }).do()
        """

        if not row:
            raise ValueError("No data to insert.")

        cleaned_data = cls._gen_insert_row(m, row)
        return Insert(with_table(m), Values(cleaned_data))

    @classmethod
    @util.argschecker(rows=SEQUENCE, columns=SEQUENCE)
    def insert_many(
        cls,
        m: Type[Model],
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[FieldBase]] = None
    ) -> Insert:
        """
        >>> people = [
            {'first': 'Bob', 'last': 'Foo'},
            {'first': 'Herb', 'last': 'Bar'},
            {'first': 'Nuggie', 'last': 'Bar'}]

        # Inserting multiple
        >>> result = Person.insert(people).do()

        # We can also specify row tuples, so long as we tell Peewee which
        # columns the tuple values correspond to:
        >>> people = [
            ('Bob', 'Foo'),
            ('Herb', 'Bar'),
            ('Nuggie', 'Bar')]
        >>> Person.insert(people, columns=[Person.first, Person.last]).do()
        """
        if not rows:
            raise ValueError("No data to insert.")

        if columns:
            for c in columns:
                if not isinstance(c, FieldBase):
                    raise TypeError("Use field")

                if c.name not in m.__names__:
                    raise err.NoSuchColumnError(c)

        cleaned_rows = []
        for row in rows:
            if isinstance(row, SEQUENCE):
                if not columns:
                    raise ValueError("Bulk insert must specify columns.")
                row = dict(zip(row, columns))
                if len(row) != len(columns):
                    raise ValueError()
            elif not isinstance(row, dict):
                raise ValueError()

            cleaned_rows.append(cls._gen_insert_row(m, row))  # type:ignore

        return Insert(with_table(m), Values(cleaned_rows))

    @classmethod
    def update(cls, m: Type[Model], values: Any) -> Update:

        return Update(with_table(m), values)

    @classmethod
    def delete(cls, m: Type[Model]) -> Delete:

        return Delete(with_table(m))

    @classmethod
    def replace(cls, m: Type[Model], values: Any) -> Replace:

        return Replace(with_table(m), Values(values))

    @classmethod
    def replace_many(cls, m: Type[Model], values: Any) -> Replace:

        return Replace(with_table(m), Values(values))

    # model

    @classmethod
    async def save(cls, mo: Model) -> db.ExecResult:
        """ save mo """

        row = Values(cls._gen_insert_row(mo, mo.__self__))  # type: ignore
        result = await Replace(with_table(mo), row).do()
        mo.__setmodel__(
            mo.__names__[with_table(mo).primary.field.name],
            result.last_id,
            True
        )
        return result

    @classmethod
    async def remove(cls, mo: Model) -> db.ExecResult:
        """ delete mo """

        primary = getattr(mo, mo.__names__[with_table(mo).primary.field.name])
        if not primary:
            raise RuntimeError()

        return await Delete(
            with_table(mo)
        ).where(with_table(mo).primary.field == primary).do()

    @classmethod
    def _get_default_row(cls, m: Type[Model]) -> Dict[str, Any]:

        insert_data = {}  # type: Dict[str, Any]

        for f in with_table(m).fields:
            if f.primary_key and f.auto:  # type:ignore
                continue
            default = f.default() if callable(f.default) else f.default
            if isinstance(default, SQL):
                continue
            insert_data[f.name] = default   # type: ignore

        return insert_data

    @classmethod
    def _gen_insert_row(
            cls, m: Type[Model], row_data: Dict[str, Any]
    ) -> Dict[str, Any]:

        cleaned_data = {}        # type: Dict[str, Any]
        for col in row_data:
            try:
                f = with_table(m).fields_dict[col]
            except KeyError:
                raise err.NoSuchColumnError()
            cleaned_data[f.name] = row_data[col]  # type: ignore

        insert_data = cls._get_default_row(m)
        for col in insert_data:
            v = cleaned_data.pop(col, None)
            f = with_table(m).fields_dict[m.__names__[col]]
            if v is None and not f.null:
                raise err.InvalidColumnsVlaueError()
            try:
                insert_data[col] = f.db_value(v)
            except ValueError:
                raise err.InvalidColumnsVlaueError()

        if cleaned_data:
            for c in cleaned_data:
                raise err.NoSuchColumnError(c)

        return insert_data


class Values(gh.Node):

    def __init__(
        self, rows: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:

        if isinstance(rows, dict):
            self._columns = list(rows.keys())
            self._values = [tuple(rows.values())]
        elif isinstance(rows, list):
            self._columns = list(rows[0].keys())
            self._values = [tuple(r.values()) for r in rows]

    @property
    def columns(self) -> List[gh.Node]:
        return [SQL("``".join(c)) for c in self._columns]

    @property
    def params(self) -> List[gh.Node]:
        return [SQL("%s")] * len(self._columns)

    def __sql__(self, ctx: gh.Context):

        ctx.sql(gh.CommaNodeList(self.columns))

        ctx.literal(
            f" VALUES "
        ).sql(
            self.params
        ).values(self._values)

        return ctx


class QueryBase:

    __slots__ = ('_state',)

    def __init__(self):
        self._state = util.tdict()

    def __repr__(self) -> str:
        return repr(self.__query__())

    def __str__(self) -> str:
        return str(self.__query__())

    def __query__(self) -> gh.Query:
        return gh.Context().parse(self).query()

    @property
    def query(self) -> gh.Query:
        table = getattr(self, '_table', None)
        if not table and hasattr(self, '_model'):
            table = with_table(self._model)
        self._state.db = getattr(table, 'db', None)
        return self.__query__()


class WriteQuery(gh.Node, QueryBase):

    async def do(self) -> Any:
        self.query.r = False
        return await db.execute(self.query, **self._state)

    def __sql__(self, ctx: gh.Context) -> gh.Context:
        raise NotImplementedError


class Select(gh.Node, QueryBase):

    __slots__ = (
        '_model', '_select', '_from', '_join', '_where', '_group_by',
        '_having', '_window', '_order_by', '_limit', '_offset',
        '_rowtype'
    )

    def __init__(
            self, columns: List[FieldBase], model: Type[Model]
    ) -> None:

        self._model = model
        self._select = columns
        self._from = with_table(model)
        self._join = None
        self._where = None
        self._group_by = None
        self._having = None
        self._window = None
        self._order_by = None
        self._limit = None
        self._offset = None
        self._rowtype = RT.MODEL
        super().__init__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        pass

    def __getitem__(self, _id):
        pass

    def __contains__(self, key):
        pass

    def __len__(self):
        pass

    def __reversed__(self):
        pass

    def __bool__(self):
        pass

    def join(self, dest, join_type="", on=None):
        raise NotImplementedError

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = and_(*filters)
        return self

    def group_by(self, *columns):
        if not columns:
            raise ValueError("Group by clause cannot be empty")

        self._group_by = columns
        return self

    def having(self, *filters):
        if not filters:
            raise ValueError("Having clause cannot be empty")

        self._having = and_(*filters)
        return self

    def window(self):
        raise NotImplementedError

    def order_by(self, *columns):
        if not columns:
            raise ValueError("Order by clause cannot be empty")

        self._order_by = columns
        return self

    def limit(self, limit=1000, offset=0):
        self._limit = limit
        self._offset = offset
        return self

    def tdicts(self):
        self._rowtype = RT.TDICT
        return self

    def tuples(self):
        self._rowtype = RT.TUPLE
        return self

    async def all(
        self, rowtype=None
    ) -> Union[None, db.FetchResult, tuple, util.tdict]:
        self.query.r = True
        if rowtype and rowtype not in RT.values():
            raise ValueError(f"Unsupported rowtype {rowtype}")
        if rowtype:
            self._rowtype = rowtype
        if self._rowtype == RT.TUPLE:
            self._state.tdict = False
        return Loader(
            await db.execute(self.query, **self._state),
            self._model, self._rowtype
        ).do()

    async def first(self, rowtype=None):
        self.limit(1)
        self._state.rows = 1
        await self.all(rowtype)

    async def get(self):
        pass

    async def rows(self, rows, start=0, rowtype=None):
        self.limit(rows, start)
        if rows <= 0:
            raise ValueError()
        if rows == 1:
            self._state.rows = 1
        await self.all(rowtype)

    async def paginate(self, page, size=20, rowtype=None):
        if page < 0 or size <= 0:
            raise ValueError()
        if page > 0:
            page -= 1
        if size == 1:
            self._state.rows = 1
        self._limit = size
        self._offset = page * size
        await self.all(rowtype)

    async def scalar(self):
        return await self.all()[0]

    async def count(self):
        pass

    async def exist(self):
        pass

    def __sql__(self, ctx: gh.Context):
        ctx.literal("SELECT ").sql(gh.CommaNodeList(self._select))  # type: ignore
        ctx.literal(f" FROM ").sql(self._from)

        if self._where:
            ctx.literal(" WHERE ").sql(self._where)

        if self._group_by:
            ctx.literal(" GROUP BY ").sql(gh.CommaNodeList(self._group_by))

        if self._having:
            ctx.literal(f" HAVING ").sql(self._having)

        if self._window:
            ctx.literal(" WINDOW ")
            ctx.sql(gh.CommaNodeList(self._window))

        if self._order_by:
            ctx.literal(" ORDER BY ").sql(gh.CommaNodeList(self._order_by))

        if self._limit:
            ctx.literal(f" LIMIT {self._limit} ")

        if self._offset:
            ctx.literal(f" OFFSET {self._offset}")

        return ctx


class Insert(WriteQuery):

    __slots__ = ('_table', '_values', '_select')

    def __init__(self, table: Table, values: Values, many=False):
        self._table = table
        self._values = values
        self._select = None
        if many:
            self._state.many = True
        super().__init__()

    def select(self, *columns):
        raise NotImplementedError

    def __sql__(self, ctx: gh.Context):
        ctx.literal(
            f"INSERT INTO "
        ).sql(self._table)

        ctx.sql(self._values)

        return ctx


class Replace(WriteQuery):

    __slots__ = ('_table', '_values', '_select')

    def __init__(self, table: Table, values: Values):
        self._table = table
        self._values = values
        self._select = None
        super().__init__()

    def select(self, *columns):
        raise NotImplementedError

    def __sql__(self, ctx):
        ctx.literal(
            f"REPLACE INTO "
        ).sql(self._table)

        ctx.sql(self._values)

        return ctx


class Update(WriteQuery):

    __slots__ = ('_table', '_update', '_where')

    def __init__(self, table: Table, update: Dict[str, Any]):
        self._table = table
        self._update = update
        self._where = None
        super().__init__()

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = and_(*filters)
        return self

    def __sql__(self, ctx: gh.Context):
        ctx.literal(
            "UPDATE "
        ).sql(self._table)

        ctx.literal(
            " SET "
        ).sql(
            gh.CommaNodeList([SQL(f'{c}=%s') for c in self._update])
        ).values(list(self._update.values()))

        if self._where:
            ctx.literal(
                " WHERE "
            ).sql(
                self._where
            )

        return ctx


class Delete(WriteQuery):

    __slots__ = ('_table', '_where', '_limit', '_force')

    def __init__(self, table: Table, force=False):

        self._table = table
        self._where = None
        self._limit = None
        self._force = force
        super().__init__()

    def where(self, *filters):
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = and_(*filters)
        return self

    def limit(self, row_count):
        self._limit = row_count
        return self

    def __sql__(self, ctx: gh.Context):
        ctx.literal(f"DELETE FROM").sql(self._table)
        if self._where:
            ctx.literal(
                " WHERE "
            ).sql(self._where)
        elif not self._force:
            raise RuntimeError()
        if self._limit:
            ctx.literal(f" LIMIT {self._limit}")

        return ctx


class Join(gh.Node):

    def __sql__(self, ctx):
        return ctx


class Create(WriteQuery):

    __slots__ = ('_table', '_options')

    def __init__(self, table: Table, **options: Any) -> None:
        self._table = table
        self._options = options
        super().__init__()

    def __sql__(self, ctx: gh.Context) -> gh.Context:

        ctx.literal('CREATE ')
        if self._options.get('temporary'):
            ctx.literal('TEMPORARY ')
        ctx.literal('TABLE ')
        if self._options.get('safe', True):
            ctx.literal('IF NOT EXISTS ')
        ctx.sql(self._table)

        defs = [f.__def__() for f in self._table.fields]  # type: List[gh.Node]
        defs.append(SQL(f"PRIMARY KEY (`{self._table.primary.field.name}`)"))
        if self._table.indexes:
            defs.extend([i.__def__() for i in self._table.indexes])

        ctx.sql(
            gh.EnclosedNodeList(defs)
        ).literal(
            f"ENGINE={self._table.engine} "
            f"AUTO_INCREMENT={self._table.auto_increment} "
            f"DEFAULT CHARSET={self._table.charset} "
            f"COMMENT='{self._table.comment}'"
        )

        return ctx


class Drop(Create):

    def __sql__(self, ctx: gh.Context) -> gh.Context:
        ctx.literal('DROP TABLE ')
        if self._options.get('safe'):
            ctx.literal(' IF NOT EXISTS ')
        ctx.sql(self._table)
        return ctx


class Show:

    __slots__ = ("_table",)

    __fetch__ = True

    def __init__(self, table: Table):
        self._table = table

    def __repr__(self):
        return f"<Class Show> for table `{self._table.table_name}`"

    __str__ = __repr__

    async def create_syntax(self):
        return (await db.execute(
            gh.Query(
                f"SHOW CREATE TABLE {self._table.table_name};",
                read=True
            ),
            rows=1
        ))['Create Table']

    async def columns(self):
        return await db.execute(
            gh.Query(
                f"SHOW FULL COLUMNS FROM {self._table.table_name};"
            ))

    async def indexes(self):
        return await db.execute(
            gh.Query(
                f"SHOW INDEX FROM {self._table.table_name};")
        )

    async def engine(self):
        pass


class Alter(WriteQuery):

    __slots__ = ('_table',)

    def __init__(self, table):
        self._table = table
        super().__init__()

    def add(self):
        pass

    def drop(self):
        pass

    def modify(self):
        pass

    def change(self):
        pass

    def after(self):
        pass

    def first(self):
        pass

    def __sql__(self, ctx):
        return ctx


class Loader:

    def __init__(self, ori, model, rt):
        self._ori = ori
        self._model = model
        self._rt = rt

    def do(self):
        """
        1. model:
            - 1: None | {} -> Model
            -!1: [] | [{}, {}] -> [model1, model2]

        2. tdict:
            - 1: None | {}
            -!1 [] | [{}, {}]

        3. tuple:
            - 1: None | (xx, xx)
            -!1: () | [(xx, xx), ...]
        """
        if self._rt == RT.MODEL:
            if isinstance(self._ori, list):
                for i in range(len(self._ori)):
                    mobj = self._to_model(self._ori[i])
                    self._ori[i] = mobj or self._ori[i]

            elif self._ori:
                self._ori = self._to_model(self._ori) or self._ori
        return self._ori

    def _to_model(self, row):

        model = self._model()
        for name, value in row.items():
            try:
                model.__setmodel__(name, value, __load=True)
            except Exception:
                return None
        return model
