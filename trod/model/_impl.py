"""
    trod.model._impl
    ~~~~~~~~~~~~~~~~

    Implements the model core.
"""

from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Any, Dict, Optional, List, Union, Tuple, Type

from .. import db, util, err
from .._helper import (
    Query,
    SQL,
    Node,
    Context,
    parse,
    CommaNodeList,
    EnclosedNodeList,
    with_metaclass,
)
from ..types._impl import (
    SEQUENCE,
    ENCODING,
    Id,
    IdList,
    FieldBase,
    IndexBase,
    FS,
)


ROWTYPE = util.tdict(
    MODEL=1,
    TDICT=2,
    TUPLE=3,
)
DEFAULT_ROWTYPE = ROWTYPE.MODEL

JOINTYPE = util.tdict(
    INNER='INNER',
    LEFT_OUTER='LEFT OUTER',
    RIGHT_OUTER='RIGHT OUTER',
    FULL='FULL',
)

_BUILTIN_NAMES = ("ModelBase", "Model")


class Table(Node):

    __slots__ = (
        "db", "name", "fields_dict", "fields", "primary",
        "indexes", "auto_increment", "engine", "charset",
        "comment",
    )

    AIPK = 'id'
    META = util.tdict(
        __db__=None,
        __tablename__=None,
        __indexes__=None,
        __auto_increment__=1,
        __engine__='InnoDB',
        __charset__=ENCODING.utf8,
        __comment__='',
    )

    def __init__(
        self,
        database: Optional[str],
        name: str,
        fields_dict: Dict[str, FieldBase],
        primary: util.tdict,
        indexes: Optional[Union[Tuple[IndexBase, ...], List[IndexBase]]] = None,
        engine: Optional[str] = None,
        charset: Optional[str] = None,
        comment: Optional[str] = None
    ) -> None:
        self.db = database
        self.name = name
        self.fields_dict = fields_dict
        self.fields = list(fields_dict.values())
        self.primary = primary
        self.indexes = indexes
        self.auto_increment = primary.begin or self.META.__auto_increment__
        self.engine = engine or self.META.__engine__
        self.charset = charset or self.META.__charset__
        self.comment = comment or self.META.__comment__

        if not self.primary.field:
            raise err.NoPKError(
                f"Primary key not found for table {self.table_name}"
            )

    def __repr__(self) -> str:
        return f"<Table {self.table_name}>"

    def __str__(self) -> str:
        return self.name

    def __metaattr__(self, name: str) -> Any:
        attr = name.strip("__")
        if attr == "tablename":
            return self.name
        return getattr(self, attr)

    def __sql__(self, ctx: Context):
        ctx.literal(self.table_name)
        return ctx

    @property
    def table_name(self) -> str:
        if self.db:
            return f"`{self.db}`.`{self.name}`"
        return f"`{self.name}`"


class ModelType(type):
    """ Model metaclass.
    TODO: Should be optimized
    """

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: dict) -> type:

        def __prepare__():

            table_name = attrs.pop("__tablename__", name.lower())

            model_fields, model_attrs = {}, {}
            for attr in attrs.copy():
                field = attrs[attr]
                if isinstance(field, FieldBase):
                    field.name = field.name or attr
                    model_fields[attr] = field
                    model_attrs[field.name] = attr
                    attrs.pop(attr)

            indexes = attrs.pop("__indexes__", [])
            if indexes and not isinstance(indexes, (tuple, list)):
                raise TypeError('__indexes__ type must be `tuple` or `list`')
            for index in indexes:
                if not isinstance(index, IndexBase):
                    raise err.InvalidFieldType()

            bound = attrs.pop("__db__", None)
            engine = attrs.pop("__engine__", None)
            charset = attrs.pop("__charset__", None)
            comment = attrs.pop("__comment__", None)

            base_table = None
            base_type = bases[0] if bases else None
            if base_type:
                base_table = deepcopy(base_type.__table__)
                if base_table:
                    base_names = deepcopy(base_type.__attrs__)
                    base_names.update(model_attrs)
                    model_attrs = base_names

                    bound = bound or base_table.db
                    base_table.fields_dict.update(model_fields)
                    model_fields = base_table.fields_dict

                    indexes = indexes or base_table.indexes
                    engine = engine or base_table.engine
                    charset = charset or base_table.charset
                    comment = comment or base_table.comment

            primary = util.tdict(auto=False, field=None, attr=None, begin=None)
            for attr_name, field in model_fields.items():
                if getattr(field, 'primary_key', None):
                    if primary.field is not None:
                        raise err.DuplicatePKError(
                            "Duplicate primary key found for field "
                            f"{field.name}"
                        )
                    primary.field = field
                    primary.attr = attr_name
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

            table_meta = Table(
                database=bound,
                name=table_name,
                fields_dict=model_fields,
                primary=primary,
                indexes=indexes,
                engine=engine,
                charset=charset,
                comment=comment,
            )
            attrs["__attrs__"] = model_attrs
            attrs["__table__"] = table_meta

            return attrs

        attrs['__table__'] = None
        if name not in _BUILTIN_NAMES:
            attrs = __prepare__()

        return type.__new__(cls, name, bases, attrs)

    def __getattr__(cls, name: str) -> Any:
        if name in cls.__table__.fields_dict:
            return cls.__table__.fields_dict[name]

        if name in cls.__table__.META:
            return cls.__table__.__metaattr__(name)

        raise AttributeError(
            f"'{cls.__name__}' class does not have attribute '{name}'"
        )

    def __setattr__(cls, *_args: Any) -> None:
        raise err.NotAllowedError(
            f"Model '{cls.__name__}' class not allow set attribute")

    def __delattr__(cls, name: str) -> None:
        raise err.NotAllowedError(
            f"Model '{cls.__name__}' class not allow delete attribute"
        )

    def __repr__(cls) -> str:
        return f"Model<{cls.__name__}>"

    def __str__(cls) -> str:
        return cls.__name__

    def __aiter__(cls) -> Select:
        return Api.select(cls)  # type: ignore

    def __getitem__(cls, _id: Id) -> Model:
        raise NotImplementedError

    def __contains__(cls, _id: Id) -> bool:
        raise NotImplementedError


def for_table(m: Union[Type[Model], Model]) -> Table:
    try:
        return m.__table__
    except AttributeError:
        raise err.ProgrammingError("Must be ModelType")


def for_attrs(m: Union[Type[Model], Model]) -> dict:
    try:
        return m.__attrs__
    except AttributeError:
        raise err.ProgrammingError("Must be ModelType")


class ModelBase:
    """Model Base Class"""

    def __init__(self, **kwargs: Any) -> None:
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self) -> str:
        id_ = getattr(self, self.__table__.primary.attr, None)
        return f"<{self.__class__.__name__} object> at {id_}"

    __str__ = __repr__

    def __hash__(self) -> int:
        return hash(self.__table__.name)

    def __setattr__(self, name: str, value: Any) -> None:
        self.__setmodel__(name, value)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__dict__[name]
        except KeyError:
            if name in self.__table__.fields_dict:
                return None
            raise AttributeError(
                f"'{self.__class__}' object has no attribute '{name}'"
            )

    def __bool__(self) -> bool:
        return bool(self.__dict__)

    def __setmodel__(
            self, name: str, value: Any, __load__: bool = False
    ) -> None:
        f = self.__table__.fields_dict.get(name)
        if not f:
            raise err.NotAllowedError(
                f"{self.__class__.__name__} object not allowed "
                f"set attribute '{name}'"
            )

        if not __load__:
            if getattr(f, 'primary_key', None) and getattr(f, 'auto', None):
                raise err.NotAllowedError(
                    f"auto field '{f.name}' not allowed to set"
                )

        value = f.py_value(value)

        self.__dict__[name] = value

    @property
    def __self__(self) -> Dict[str, Any]:
        return deepcopy(self.__dict__)


class Model(with_metaclass(ModelType, ModelBase)):  # type: ignore
    """Model API"""

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
    async def get(
        cls,
        _id: Id,
        rowtype: Optional[int] = None
    ) -> Union[Model, util.tdict, None, Tuple[Any, ...]]:
        if not _id:
            return None
        return await Api.get(cls, _id, rowtype=rowtype)

    @classmethod
    async def mget(
        cls,
        ids: IdList,
        columns: Optional[List[FieldBase]] = None,
        rowtype: Optional[int] = None
    ) -> db.FetchResult:
        if not ids:
            raise ValueError("No ids to mget")
        return await Api.get_many(cls, ids, columns=columns, rowtype=rowtype)

    @classmethod
    async def add(
        cls,
        row: Union[Model, Dict[str, Any]]
    ) -> db.ExecResult:
        if row is None:
            raise ValueError("No data to add")
        return await Api.add(cls, row)

    @classmethod
    async def madd(
        cls,
        rows: Union[List[Model], List[Dict[str, Any]]]
    ) -> db.ExecResult:
        if not rows:
            raise ValueError("No data to madd")
        return await Api.add_many(cls, rows)

    @classmethod
    async def set(cls, _id: Id, **values: Any) -> db.ExecResult:
        if not (_id and values):
            raise ValueError('No _id or values to set')
        return await Api.set(cls, _id, values)

    @classmethod
    def select(cls, *columns: FieldBase) -> Select:
        return Api.select(cls, *columns)

    @classmethod
    def insert(
        cls, __row: Optional[Dict[str, Any]] = None, **values: Any
    ) -> Insert:
        row = __row or values
        if not row:
            raise ValueError("No data to insert")
        return Api.insert(cls, row)

    @classmethod
    def minsert(
        cls,
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[FieldBase]] = None
    ) -> Insert:
        if not rows:
            raise ValueError("No data to minsert {}")
        return Api.insert_many(cls, rows, columns=columns)

    @classmethod
    def update(cls, **values: Any) -> Update:
        if not values:
            raise ValueError("No data to update")
        return Api.update(cls, values)

    @classmethod
    def delete(cls) -> Delete:
        return Api.delete(cls)

    @classmethod
    def replace(
        cls, __row: Optional[Dict[str, Any]] = None, **values: Any
    ) -> Replace:
        row = __row or values
        if not row:
            raise ValueError("No data to replace")
        return Api.replace(cls, row)

    @classmethod
    def mreplace(
        cls,
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[FieldBase]] = None
    ) -> Replace:
        if not rows:
            raise ValueError("No data to mreplace")
        return Api.replace_many(cls, rows, columns=columns)

    async def save(self) -> db.ExecResult:
        return await Api.save(self)

    async def remove(self) -> db.ExecResult:
        return await Api.remove(self)


class Api:
    """Implementation of the Model API"""

    @classmethod
    async def create_table(
        cls, m: Type[Model], **options: Any
    ) -> db.ExecResult:
        """ Do create table """

        if m.__name__ in _BUILTIN_NAMES:
            raise err.NotAllowedError(f"{m.__name__} is a built-in model name")

        return await Create(for_table(m), **options).do()

    @classmethod
    async def drop_table(
        cls, m: Type[Model], **options: Any
    ) -> db.ExecResult:
        """ Do drop table """

        if m.__name__ in _BUILTIN_NAMES:
            raise err.NotAllowedError(f"{m.__name__} is a built-in model name")

        return await Drop(for_table(m), **options).do()

    @classmethod
    def alter(cls, m: Type[Model]) -> Alter:

        return Alter(for_table(m))

    @classmethod
    def show(cls, m: Type[Model]) -> Show:

        return Show(for_table(m))

    @classmethod
    async def get(
        cls,
        m: Type[Model],
        _id: Id,
        rowtype: Optional[int] = None
    ) -> Union[Model, util.tdict, None, Tuple[Any, ...]]:

        table = for_table(m)

        return await Select(
            table.fields, m
        ).where(
            table.primary.field == _id
        ).get(rowtype)

    @classmethod
    @util.argschecker(ids=SEQUENCE)
    async def get_many(
        cls,
        m: Type[Model],
        ids: IdList,
        columns: Optional[List[FieldBase]] = None,
        rowtype: Optional[int] = None
    ) -> db.FetchResult:

        table = for_table(m)
        columns = columns or table.fields

        return await Select(
            columns, m
        ).where(
            table.primary.field.in_(ids)
        ).all(rowtype)

    @classmethod
    @util.argschecker(row=(Model, dict), nullable=False)
    async def add(
        cls,
        m: Type[Model],
        row: Union[Model, Dict[str, Any]]
    ) -> db.ExecResult:

        addrow = cls._gen_insert_row(
            m, row.__self__ if isinstance(row, m) else row
        )
        return await Insert(for_table(m), Values(addrow)).do()

    @classmethod
    @util.argschecker(rows=list, nullable=False)
    async def add_many(
        cls,
        m: Type[Model],
        rows: Union[List[Model], List[Dict[str, Any]]]
    ) -> db.ExecResult:

        addrows = []
        for row in rows:
            if isinstance(row, m):
                addrows.append(cls._gen_insert_row(m, row.__self__))
            elif isinstance(row, dict):
                addrows.append(cls._gen_insert_row(m, row))
            else:
                raise ValueError(f"Invalid data {row!r} to add")

        return await Insert(for_table(m), Values(addrows), many=True).do()

    @classmethod
    @util.argschecker(values=dict, nullable=False)
    async def set(
        cls,
        m: Type[Model],
        _id: Id,
        values: Any
    ) -> db.ExecResult:

        table = for_table(m)
        return await Update(
            table, values
        ).where(table.primary.field == _id).do()

    @classmethod
    def select(
        cls, m: Type[Model], *columns: FieldBase
    ) -> Select:

        columns = columns or for_table(m).fields  # type: ignore
        # columns = columns or [SQL('*')]
        return Select(list(columns), m)

    @classmethod
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
        """

        toinsert = cls._gen_insert_row(m, row.copy())
        return Insert(for_table(m), Values(toinsert))

    @classmethod
    @util.argschecker(rows=SEQUENCE)
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
        normalize_rows = cls._normalize_rows(m, rows, columns)
        return Insert(for_table(m), Values(normalize_rows), many=True)

    @classmethod
    def update(cls, m: Type[Model], values: Dict[str, Any]) -> Update:

        return Update(for_table(m), values)

    @classmethod
    def delete(cls, m: Type[Model]) -> Delete:

        return Delete(for_table(m))

    @classmethod
    def replace(cls, m: Type[Model], row: Dict[str, Any]) -> Replace:

        toreplace = cls._gen_insert_row(m, row, for_replace=True)
        return Replace(for_table(m), Values(toreplace))

    @classmethod
    def replace_many(
        cls,
        m: Type[Model],
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[FieldBase]] = None
    ) -> Replace:

        normalize_rows = cls._normalize_rows(m, rows, columns, for_replace=True)
        return Replace(for_table(m), Values(normalize_rows), many=True)

    @classmethod
    async def save(cls, mo: Model) -> db.ExecResult:
        """ save model object """

        has_id = False
        pk_attr = for_table(mo).primary.attr
        if pk_attr in mo.__self__:
            has_id = True

        row = Values(
            cls._gen_insert_row(mo, mo.__self__, for_replace=has_id)
        )
        result = await Replace(for_table(mo), row).do()
        mo.__setmodel__(
            name=pk_attr,
            value=result.last_id,
            __load__=True
        )
        return result

    @classmethod
    async def remove(cls, mo: Model) -> db.ExecResult:
        """ delete model object"""

        table = for_table(mo)
        primary_value = getattr(mo, table.primary.attr, None)
        if not primary_value:
            raise RuntimeError("Remove object has no primary key value")

        return await Delete(
            table
        ).where(table.primary.field == primary_value).do()

    @classmethod
    @util.argschecker(row_data=dict, nullable=False)
    def _gen_insert_row(
        cls,
        m: Type[Model],
        row_data: Dict[str, Any],
        for_replace: bool = False
    ) -> Dict[str, Any]:

        toinserts = {}
        for name, field in for_table(m).fields_dict.items():
            # Primary key fields should not be included when not for_replace
            if name == for_table(m).primary.attr and not for_replace:
                continue

            value = row_data.pop(name, None)
            # if value is None, to get default
            if value is None:
                default = field.default() if callable(field.default) else field.default
                if isinstance(default, SQL):
                    continue
                value = default
            if value is None and not field.null:
                if not for_replace:
                    raise err.InvalidColumnValue(
                        f"Invalid data(None) for not null attribute {name}"
                    )
            try:
                toinserts[field.name] = value
            except (ValueError, TypeError):
                raise ValueError(f'Invalid data({value}) for {name}')

        for attr in row_data:
            if not for_replace and attr == for_table(m).primary.attr:
                raise err.NotAllowedError(
                    f"Auto field {attr!r} not allowed to set"
                )
            raise ValueError(f"'{m!r}' has no attribute {attr}")

        return toinserts

    @classmethod
    def _normalize_rows(
        cls,
        m: Type[Model],
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[FieldBase]] = None,
        for_replace: bool = False,
    ) -> List[Dict[str, Any]]:

        cleaned_rows = []  # type:List[Dict[str, Any]]

        if columns:
            if not isinstance(columns, list):
                raise ValueError("Specify columns must be list")
            mattrs = for_attrs(m)
            for c in columns:
                if not isinstance(c, FieldBase):
                    raise TypeError(f"Invalid type of columns element {c}")

                if c.name not in mattrs:
                    raise ValueError(f"{m!r} has no attribute {c.name}")
                c = mattrs[c.name]

            for row in rows:
                if not isinstance(row, SEQUENCE):
                    raise ValueError(f"Invalid data {row!r} for specify columns")
                row = dict(zip(columns, row))  # type:ignore
                if len(row) != len(columns):
                    raise ValueError("No enough data for columns")

                cleaned_rows.append(cls._gen_insert_row(m, row, for_replace))
        else:
            cleaned_rows = [cls._gen_insert_row(m, r, for_replace) for r in rows]

        return cleaned_rows


class Values(Node):

    __slots__ = ("_columns", "_params", "_values")

    def __init__(
        self, rows: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:

        if isinstance(rows, dict):
            columns = list(rows.keys())
            self._values = tuple(rows.values())
        elif isinstance(rows, list):
            columns = list(rows[0].keys())
            self._values = tuple([tuple(r.values()) for r in rows])
        else:
            raise ValueError("Invalid data unpack to values")

        self._columns, self._params = [], []  # type: List[Node], List[Node]
        for col in columns:
            self._columns.append(SQL(col.join("``")))
            self._params.append(SQL("%s"))

    def __sql__(self, ctx: Context) -> Context:

        ctx.literal(
            ' '
        ).sql(EnclosedNodeList(self._columns))

        ctx.literal(
            " VALUES "
        ).sql(
            EnclosedNodeList(self._params)
        ).values(self._values)

        return ctx


class QueryBase(Node):

    __slots__ = ('_props',)

    def __init__(self) -> None:
        self._props = util.tdict()

    def __repr__(self) -> str:
        return repr(self.__query__)

    def __str__(self) -> str:
        return str(self.__query__)

    @property
    def __query__(self) -> Query:
        return parse(self)

    @property
    def query(self) -> Query:
        table = getattr(self, '_table', None)
        if not table and hasattr(self, '_model'):
            table = for_table(getattr(self, '_model'))
        self._props.db = getattr(table, 'db', None)
        return self.__query__

    def __sql__(self, ctx: Context) -> Context:
        raise NotImplementedError


class WriteQuery(QueryBase):

    async def do(self) -> Any:
        self.query.r = False
        return await db.execute(self.query, **self._props)

    def __sql__(self, ctx: Context) -> Context:
        raise NotImplementedError


class Select(QueryBase):

    __slots__ = (
        '_model', '_select', '_table', '_join', '_where',
        '_group_by', '_having', '_window', '_order_by',
        '_limit', '_offset', '_rowtype', '_irange',
    )
    _single = 1

    def __init__(
            self, columns: Union[List[FieldBase], List[SQL]], model: Type[Model]
    ) -> None:

        super().__init__()
        self._select = columns
        self._model = model
        self._table = for_table(model)
        self._join = None
        self._where = None
        self._group_by = None
        self._having = None
        self._window = None
        self._order_by = None
        self._limit = None      # type: Optional[int]
        self._offset = None     # type: Optional[int]
        self._irange = None     # type: Optional[slice]
        self._rowtype = DEFAULT_ROWTYPE

    async def __genrow__(self) -> Optional[Model]:
        if self._offset is None:
            if self._irange:
                self._offset = self._irange.start
            else:
                self._offset = 0
        else:
            if self._irange and self._irange.step:
                self._offset += self._irange.step
            else:
                self._offset += 1

        if self._irange and self._irange.stop:
            if self._offset >= self._irange.stop:  # type:ignore
                return None
        ret = await self.limit(self._single).first()
        return ret  # type: ignore

    async def __anext__(self) -> Optional[Model]:
        v = await self.__genrow__()
        if not v:
            raise StopAsyncIteration
        return v

    def __aiter__(self) -> Select:
        return self

    def __getitem__(self, _range: slice) -> Select:
        if isinstance(_range, slice):
            if _range.start is None or _range.stop is None:
                raise ValueError(
                    'Iter range must have both a start and end-point.'
                )
            if _range.start < 0 or _range.stop < 0 or _range.start >= _range.stop:
                raise ValueError(f"Invalid range slice({_range})")

            if _range.step is not None and _range.step <= 0:
                raise ValueError(f"Invalid slice step {_range.step}")

            self._irange = _range  # type: ignore
        else:
            raise TypeError("Range type must be slice")
        return self

    def join(
        self, dest: Any,
        join_type: str = JOINTYPE.INNER,
        on: Optional[str] = None
    ) -> Select:
        raise NotImplementedError

    def where(self, *filters: Node) -> Select:
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = util.and_(*filters)
        return self

    def group_by(self, *columns: FieldBase) -> Select:
        if not columns:
            raise ValueError("Group by clause cannot be empty")
        for f in columns:
            if not isinstance(f, Node):
                raise TypeError(
                    f"Invalid type for {self._model!r} group_by"
                )

        self._group_by = columns  # type: ignore
        return self

    def having(self, *filters: Node) -> Select:
        if not filters:
            raise ValueError("Having clause cannot be empty")

        self._having = util.and_(*filters)
        return self

    def window(self) -> Select:
        raise NotImplementedError

    def order_by(self, *columns: FieldBase):
        if not columns:
            raise ValueError("Order by clause cannot be empty")
        for f in columns:
            if not isinstance(f, Node):
                raise TypeError(
                    f"Invalid type for {self._model!r} order_by"
                )

        self._order_by = columns  # type: ignore
        return self

    def limit(self, limit: int = 1000) -> Select:
        self._limit = limit
        return self

    def offset(self, offset: Optional[int] = 0) -> Select:
        if self._limit is None:
            raise err.ProgrammingError("Offset clause has no limit")
        self._offset = offset
        return self

    def tdicts(self) -> Select:
        self._rowtype = ROWTYPE.TDICT
        return self

    def tuples(self) -> Select:
        self._rowtype = ROWTYPE.TUPLE
        return self

    async def all(
        self, rowtype: Optional[int] = None
    ) -> Any:
        self.query.r = True
        if rowtype and rowtype not in ROWTYPE.values():
            raise ValueError(f"Unsupported rowtype {rowtype}")
        if rowtype:
            self._rowtype = rowtype
        if self._rowtype == ROWTYPE.TUPLE:
            self._props.tdict = False

        return Loader(
            await db.execute(self.query, **self._props),
            self._model, self._rowtype
        ).do()

    async def first(
        self, rowtype: Optional[int] = None
    ) -> Union[None, util.tdict, Tuple[Any, ...], Model]:
        self.limit(1)
        self._props.rows = self._single
        return await self.all(rowtype)

    async def get(
        self, rowtype: Optional[int] = None
    ) -> Union[None, util.tdict, Tuple[Any, ...], Model]:
        self._props.rows = self._single
        return await self.all(rowtype)

    async def rows(
        self,
        rows: int,
        start: int = 0,
        rowtype: Optional[int] = None
    ) -> db.FetchResult:
        self.limit(rows).offset(start)
        if rows <= 0:
            raise ValueError(f"Invalid select rows: {rows}")
        return await self.all(rowtype)

    async def paginate(
        self,
        page: int,
        size: int = 20,
        rowtype: Optional[int] = None
    ) -> db.FetchResult:
        if page < 0 or size <= 0:
            raise ValueError("Invalid page or size")
        if page > 0:
            page -= 1
        self._limit = size
        self._offset = page * size
        return await self.all(rowtype)

    async def scalar(self, as_tuple=False) -> Union[int, Tuple[int, ...]]:
        row = await self.tuples().first()
        return row[0] if row and not as_tuple else row  # type: ignore

    async def count(self) -> int:
        self._select = [FS.COUNT(SQL('1'))]
        return await self.scalar()  # type: ignore

    async def exist(self) -> bool:
        return bool(await self.limit(1).scalar())

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal(
            "SELECT "
        ).sql(CommaNodeList(self._select))  # type: ignore
        ctx.literal(
            " FROM "
        ).sql(self._table)

        if self._where:
            ctx.literal(" WHERE ").sql(self._where)

        if self._group_by:
            ctx.literal(
                " GROUP BY "
            ).sql(CommaNodeList(self._group_by))

        if self._having:
            ctx.literal(f" HAVING ").sql(self._having)

        if self._window:
            ctx.literal(" WINDOW ")
            ctx.sql(CommaNodeList(self._window))

        if self._order_by:
            ctx.literal(
                " ORDER BY "
            ).sql(CommaNodeList(self._order_by))

        if self._limit is not None:
            ctx.literal(f" LIMIT {self._limit}")

        if self._offset is not None:
            ctx.literal(f" OFFSET {self._offset}")

        return ctx


class Insert(WriteQuery):

    __slots__ = ('_table', '_values', '_select')

    def __init__(
        self, table: Table, values: Values, many: bool = False
    ) -> None:
        super().__init__()
        self._table = table
        self._values = values
        self._select = None
        if many:
            self._props.many = True

    def select(self, *columns: Node) -> Insert:
        raise NotImplementedError

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal(
            "INSERT INTO "
        ).sql(self._table)

        ctx.sql(self._values)

        return ctx


class Replace(Insert):

    def select(self, *columns: Node) -> Replace:
        raise NotImplementedError

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal(
            "REPLACE INTO "
        ).sql(self._table)

        ctx.sql(self._values)

        return ctx


class Update(WriteQuery):

    __slots__ = ('_table', '_update', '_where')

    def __init__(self, table: Table, update: Dict[str, Any]) -> None:
        super().__init__()
        self._table = table
        self._update = update
        self._where = None

    def where(self, *filters: Node) -> Update:
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = util.and_(*filters)
        return self

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal(
            "UPDATE "
        ).sql(self._table)

        ctx.literal(
            " SET "
        ).sql(
            CommaNodeList([SQL(f'`{c}`=%s') for c in self._update])
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

    def __init__(self, table: Table, force: bool = False) -> None:

        self._table = table
        self._where = None
        self._limit = None  # type: Optional[int]
        self._force = force
        super().__init__()

    def where(self, *filters: Node) -> Delete:
        if not filters:
            raise ValueError("Where clause cannot be empty")

        self._where = util.and_(*filters)
        return self

    def limit(self, row_count: int) -> Delete:
        self._limit = row_count
        return self

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal(f"DELETE FROM ").sql(self._table)
        if self._where:
            ctx.literal(
                " WHERE "
            ).sql(self._where)
        elif not self._force:
            raise err.DangerousOperation(
                "Delete is too dangerous as no where clause"
            )
        if self._limit is not None:
            ctx.literal(f" LIMIT {self._limit}")

        return ctx


class Join(Node):

    def __init__(self) -> None:
        raise NotImplementedError

    def __sql__(self, ctx: Context) -> Context:
        return ctx


class Create(WriteQuery):

    __slots__ = ('_table', '_options')

    def __init__(self, table: Table, **options: Any) -> None:
        self._table = table
        self._options = options
        super().__init__()

    def __sql__(self, ctx: Context) -> Context:

        ctx.literal('CREATE ')
        if self._options.get('temporary'):
            ctx.literal('TEMPORARY ')
        ctx.literal('TABLE ')
        if self._options.get('safe', True):
            ctx.literal('IF NOT EXISTS ')
        ctx.sql(self._table)

        defs = [f.__def__() for f in self._table.fields]  # type: List[Node]
        defs.append(SQL(f"PRIMARY KEY ({self._table.primary.field.column})"))
        # TODO: add unique and index
        if self._table.indexes:
            defs.extend([i.__def__() for i in self._table.indexes])

        ctx.sql(
            EnclosedNodeList(defs)
        ).literal(
            f"ENGINE={self._table.engine} "
            f"AUTO_INCREMENT={self._table.auto_increment} "
            f"DEFAULT CHARSET={self._table.charset} "
            f"COMMENT='{self._table.comment}'"
        )

        return ctx


class Drop(Create):

    def __sql__(self, ctx: Context) -> Context:
        ctx.literal('DROP TABLE ').sql(self._table)
        return ctx


class Show(QueryBase):

    __slots__ = ("_table", "_key")

    _options = {
        "create": "SHOW CREATE TABLE ",
        "columns": "SHOW FULL COLUMNS FROM ",
        "indexes": "SHOW INDEX FROM ",
    }

    def __init__(self, table: Table) -> None:
        super().__init__()
        self._table = table
        self._key = None  # type: Optional[str]

    def __repr__(self) -> str:
        return f"<Show object> for table {self._table.table_name}"

    __str__ = __repr__

    async def create_syntax(self) -> Optional[util.tdict]:
        self._key = "create"
        self._props.rows = 1
        return (
            await db.execute(self.query, **self._props)
        ).get("Create Table")

    async def columns(self) -> db.FetchResult:
        self._key = "columns"
        return await db.execute(self.query, **self._props)

    async def indexes(self) -> db.FetchResult:
        self._key = "indexes"
        return await db.execute(self.query, **self._props)

    def __sql__(self, ctx: Context) -> Context:
        if self._key is not None:
            ctx.literal(self._options[self._key]).sql(self._table)
        return ctx


class Alter(WriteQuery):

    __slots__ = ('_table',)

    def __init__(self, table: Table) -> None:
        super().__init__()
        raise NotImplementedError

    # def add(self):
    #     pass

    # def drop(self):
    #     pass

    # def modify(self):
    #     pass

    # def change(self):
    #     pass

    # def after(self):
    #     pass

    # def first(self):
    #     pass

    def __sql__(self, ctx: Context) -> Context:
        return ctx


class Loader:

    def __init__(
        self,
        data: Union[None, db.FetchResult, Tuple[Any, ...], util.tdict],
        model: Type[Model],
        rowtype: int
    ) -> None:
        self._data = data
        self._model = model
        self._rowtype = rowtype

    def do(self) -> Union[None, db.FetchResult, util.tdict, Model, Tuple[Any, ...]]:

        if self._rowtype == ROWTYPE.MODEL:
            if isinstance(self._data, db.FetchResult):
                for i in range(self._data.count):
                    mobj = self._as_model(self._data[i])
                    self._data[i] = mobj or self._data[i]

            elif self._data:
                self._data = self._as_model(self._data) or self._data  # type: ignore

        if isinstance(self._data, dict):
            mattrs = for_attrs(self._model)
            for key in self._data.copy():
                if key not in mattrs.values():
                    self._data[mattrs.get(key, key)] = self._data.pop(key)
        return self._data

    def _as_model(self, row: util.tdict) -> Optional[Model]:

        model = self._model()
        mattrs = for_attrs(self._model)
        for name, value in row.items():
            name = mattrs.get(name)
            if not name:
                return None
            try:
                model.__setmodel__(name, value, __load__=True)
            except Exception:  # pylint: disable=broad-except
                return None
        return model
