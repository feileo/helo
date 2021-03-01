from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional, List, Union, Tuple

from .. import db
from .. import util
from .. import err
from .. import _sql
from ..types import core as ttype
from . import core
from . import typ as mtype


class Model(metaclass=mtype.ModelType):
    """From Model defining your model is easy

    >>> import helo
    >>>
    >>> db = helo.Helo()
    >>>
    >>> class User(db.Model):
    ...     id = helo.Auto()
    ...     nickname = helo.VarChar(length=45)
    ...     password = helo.VarChar(length=100)
    """

    __db__ = None  # type: db.Database

    def __init__(self, **kwargs: Any) -> None:
        for attr in kwargs:
            setattr(self, attr, kwargs[attr])

    def __repr__(self) -> str:
        pk = getattr(self, self.__table__.primary.attr, None)
        return f"<{self.__class__.__name__} object {pk}>"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.__dict__})"

    def __hash__(self) -> int:
        return hash(self.__table__)

    def __eq__(self, other: Any) -> bool:
        return self.__dict__ == other.__dict__

    def __setattr__(self, name: str, value: Any) -> None:
        f = self.__table__.fields_dict.get(name)
        if not f:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

        value = f.py_value(value)
        self.__dict__[name] = value

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__dict__[name]
        except KeyError:
            if name in self.__table__.fields_dict:
                return None
            joined = self.__dict__.get("__join__")
            if joined and name in joined:
                return joined[name]
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    def __bool__(self) -> bool:
        return bool(self.__dict__)

    @property
    def __self__(self) -> Dict[str, Any]:
        return deepcopy(self.__dict__)

    @classmethod
    async def create(cls, **options: Dict[str, Any]) -> db.ExeResult:
        """Create a table in the database from the model"""

        return await create_table(cls, **options)

    @classmethod
    async def drop(cls, **options: Dict[str, Any]) -> db.ExeResult:
        """Drop a table in the database from the model"""

        return await drop_table(cls, **options)

    #
    # Simple API for short
    #
    @classmethod
    async def get(
        cls,
        by: Union[ttype.ID, ttype.Expression]
    ) -> Union[None, util.adict, Model]:
        """Getting a row by the primary key
        or simple query expression

        >>> user = await User.get(1)
        >>> user
        <User objetc 1>
        >>> user.nickname
        'at7h'
        >>>
        >>> user = await User.get(User.nickname=='at7h')
        >>> user
        <User objetc 1>
        >>> user.nickname
        'at7h'
        """

        if not by:
            return None
        return await get(cls, by)

    @classmethod
    async def mget(
        cls,
        by: Union[List[ttype.ID], ttype.Expression],
        *,
        columns: Optional[List[ttype.Column]] = None,
    ) -> Union[List[util.adict], List[Model]]:
        """Getting rows by the primary key list
        or simple query expression

        >>> await User.mget([1, 2, 3])
        [<User object 1>, <User object 2>, <User object 3>]
        """

        if not by:
            raise ValueError("no condition to mget")
        return await get_many(cls, by, columns=columns or ())

    @classmethod
    async def add(
        cls,
        __row: Optional[Dict[str, Any]] = None,
        **values: Any
    ) -> ttype.ID:
        """Adding a row, simple and shortcut of ``insert``

        # Using keyword arguments:
        >>> await User.add(nickname='at7h', password='7777')
        1

        # Using values dict:
        >>> await User.add({'nickname': 'at7h', 'password': '777'})
        1
        """

        row = __row or values
        if not row:
            raise ValueError("no data to add")
        return await add(cls, row)

    @classmethod
    async def madd(
        cls,
        rows: Union[List[Dict[str, Any]], List[Model]]
    ) -> int:
        """Adding multiple, simple and shortcut of ``minsert``

        # Using values dict list:
        >>> users = [
        ...    {'nickname': 'at7h', 'password': '777'}
        ...    {'nickname': 'mebo', 'password': '666'}]
        >>> await User.madd(users)
        2

        # Adding User object list:
        >>> users = [User(**u) for u in users]
        >>> await User.madd(users)
        2
        """

        if not rows:
            raise ValueError("no data to madd")
        return await add_many(cls, rows)

    # @classmethod
    # async def set(cls, pk: ttype.ID, **values: Any) -> int:
    #     """Setting the value of a row with the primary key

    #     >>> user = await User.get(1)
    #     >>> user.password
    #     777
    #     >>> await User.set(1, password='888')
    #     1
    #     >>> user = await User.get(1)
    #     >>> user.password
    #     888
    #     """

    #     if not values:
    #         raise ValueError("no 'pk' or 'values' to set")
    #     return await set(cls, pk, values)

    # API that translates directly from SQL statements(DQL, DML).
    # You have to explicitly execute them via methods like `do()`.
    @classmethod
    def select(cls, *columns: ttype.Column) -> core.Select:
        """Select Query, see ``Select``"""

        return select(cls, *columns)

    @classmethod
    def insert(
        cls, __row: Optional[Dict[str, Any]] = None, **values: Any
    ) -> core.Insert:
        """Inserting a row

        # Using keyword arguments:
        >>> await User.insert(nickname='at7h', password='777').do()
        ExeResult(affected: 1, last_id: 1)

        # Using values dict list:
        >>> await User.insert({
        ...     'nickname': 'at7h',
        ...     'password': '777',
        ... }).do()
        ExeResult(affected: 1, last_id: 1)
        """

        row = __row or values
        if not row:
            raise ValueError("no data to insert")
        return insert(cls, row)

    @classmethod
    def minsert(
        cls,
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[ttype.Field]] = None
    ) -> core.Insert:
        """Inserting multiple

        # Using values dict list:
        >>> users = [
        ...    {'nickname': 'Bob', 'password': '666'},
        ...    {'nickname': 'Her', 'password: '777'},
        ...    {'nickname': 'Nug', 'password': '888'}]

        >>> result = await User.insert(users).do()

        # We can also specify row tuples
        # columns the tuple values correspond to:
        >>> users = [
        ...    ('Bob', '666'),
        ...    ('Her', '777'),
        ...    ('Nug', '888')]
        >>> result = await User.insert(
        ...    users, columns=[User.nickname, User.password]
        ... ).do()
        """

        if not rows:
            raise ValueError("no data to minsert {}")
        return insert_many(cls, rows, columns=columns)

    @classmethod
    def insert_from(
        cls, from_: core.Select, columns: List[ttype.Field]
    ) -> core.Insert:
        """Inserting from select clause

        >>> select = Employee.Select(
        ...     Employee.id, Employee.name
        ... ).where(Employee.id < 10)
        >>>
        >>> User.insert_from(select, [User.id, User.name]).do()
        """

        if not columns:
            raise ValueError("insert_from must specify columns")
        return insert(cls, list(columns), from_select=from_)

    @classmethod
    def update(cls, **values: Any) -> core.Update:
        """Updating record

        >>> await User.update(
        ...    password='888').where(User.id == 1
        ... ).do()
        ExeResult(affected: 1, last_id: 0)
        """
        if not values:
            raise ValueError("no data to update")
        return update(cls, values)

    @classmethod
    def delete(cls) -> core.Delete:
        """Deleting record

        >>> await User.delete().where(User.id == 1).do()
        ExeResult(affected: 1, last_id: 0)
        """
        return delete(cls)

    @classmethod
    def replace(
        cls, __row: Optional[Dict[str, Any]] = None, **values: Any
    ) -> core.Replace:
        """MySQL REPLACE, similar to ``insert``"""

        row = __row or values
        if not row:
            raise ValueError("no data to replace")
        return replace(cls, row)

    @classmethod
    def mreplace(
        cls,
        rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
        columns: Optional[List[ttype.Field]] = None
    ) -> core.Replace:
        """MySQL REPLACE, similar to ``minsert``"""

        if not rows:
            raise ValueError("no data to mreplace")
        return replace_many(cls, rows, columns=columns)

    # instance

    async def save(self) -> ttype.ID:
        """Write objects in memory to database

        >>> user = User(nickname='at7h',password='777')
        >>> await user.save()
        1
        """
        return await save(self)

    async def remove(self) -> int:
        """Removing a row

        >>> user = await User.get(1)
        >>> await user.remove()
        1
        >>> await User.get(1)
        None
        """
        return await remove(self)


async def create_table(
    m: mtype.ModelType, **options: Dict[str, Any]
) -> db.ExeResult:
    if m.__name__ == mtype.BUILTIN_MODEL_NAME:
        raise err.NotAllowedOperation(f"{m.__name__} is built-in model name")

    return await core.Create(m, **options).do()


async def drop_table(
    m: mtype.ModelType, **options: Dict[str, Any]
) -> db.ExeResult:
    if m.__name__ == mtype.BUILTIN_MODEL_NAME:
        raise err.NotAllowedOperation(f"{m.__name__} is built-in model name")

    return await core.Drop(m, **options).do()


async def get(
    m: mtype.ModelType, by: Union[ttype.ID, ttype.Expression],
) -> Union[None, util.adict, Model]:
    if not isinstance(by, ttype.Expression):
        where = m.__table__.primary.field == by
    else:
        where = by
    return (
        await core.Select([m]).where(where).get()
    )


@util.argschecker(by=(ttype.SEQUENCE, ttype.Expression))
async def get_many(
    m: mtype.ModelType,
    by: Union[List[ttype.ID], ttype.Expression],
    columns: Union[Tuple[ttype.Column, ...], List[ttype.Column]],
) -> Union[List[util.adict], List[Model]]:
    where = by
    if isinstance(where, ttype.SEQUENCE):
        where = m.__table__.primary.field.in_(by)
    return await (
        core.Select([m], columns=columns).where(where).all()  # type: ignore
    )


@util.argschecker(row=dict, nullable=False)
async def add(
    m: mtype.ModelType,
    row: Dict[str, Any]
) -> ttype.ID:
    addrow = _gen_insert_row(m, row)
    return (
        await core.Insert(m, addrow).do()
    ).last_id


@util.argschecker(rows=list, nullable=False)
async def add_many(
    m: mtype.ModelType,
    rows: Union[List[Dict[str, Any]], List[Model]]
) -> int:
    addrows = []
    for row in rows:
        if isinstance(row, Model):
            addrows.append(_gen_insert_row(m, row.__self__))
        elif isinstance(row, dict):
            addrows.append(_gen_insert_row(m, row))
        else:
            raise ValueError(f"invalid data {row!r} to add")

    return (
        await core.Insert(m, addrows, many=True).do()
    ).affected


# @util.argschecker(values=dict, nullable=False)
# async def set(
#     m: mtype.ModelType,
#     pk: ttype.ID,
#     values: Any
# ) -> int:
#     table = m.__table__
#     values = _normalize_update_values(m, values)
#     return (
#         await core.Update(
#             m, values
#         ).where(
#             table.primary.field == pk
#         ).do()
#     ).affected


def select(
    m: mtype.ModelType, *columns: ttype.Column
) -> core.Select:
    return core.Select([m], columns=columns)


def insert(
    m: mtype.ModelType,
    row: Union[Dict[str, Any], List[ttype.Field]],
    from_select: Optional[core.Select] = None
) -> core.Insert:
    if isinstance(row, dict):
        toinsert = _gen_insert_row(m, row.copy())
        return core.Insert(m, toinsert)

    if from_select is None:
        raise ValueError('`from_select` cannot be None')

    return core.Insert(m, row).from_(from_select)


@util.argschecker(rows=ttype.SEQUENCE)
def insert_many(
    m: mtype.ModelType,
    rows: List[Union[Dict[str, Any], Tuple[Dict[str, Any], ...]]],
    columns: Optional[List[ttype.Field]] = None
) -> core.Insert:
    normalize_rows = _normalize_insert_rows(m, rows, columns)
    return core.Insert(m, normalize_rows, many=True)


def update(m: mtype.ModelType, values: Dict[str, Any]) -> core.Update:
    values = _normalize_update_values(m, values)
    return core.Update(m, values)


def delete(m: mtype.ModelType) -> core.Delete:
    return core.Delete(m)


def replace(m: mtype.ModelType, row: Dict[str, Any]) -> core.Replace:
    toreplace = _gen_insert_row(m, row, for_replace=True)
    return core.Replace(m, toreplace)


def replace_many(
    m: mtype.ModelType,
    rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
    columns: Optional[List[ttype.Field]] = None
) -> core.Replace:
    normalize_rows = _normalize_insert_rows(m, rows, columns, for_replace=True)
    return core.Replace(m, normalize_rows, many=True)


async def save(mo: Model) -> ttype.ID:
    has_id = False
    pk_attr = mo.__table__.primary.attr
    if pk_attr in mo.__self__:
        has_id = True

    row = _gen_insert_row(mo, mo.__self__, for_replace=has_id)
    if getattr(mo, '__saved__', False):
        result = await core.Update(mo.__class__, row).do()
    else:
        result = await core.Replace(mo.__class__, row).do()
    mo.__setattr__(pk_attr, result.last_id)
    mo.__dict__['__saved__'] = True
    return result.last_id


async def remove(mo: Model) -> int:
    table = mo.__table__
    primary_value = getattr(mo, table.primary.attr, None)
    if not primary_value:
        raise RuntimeError("remove object has no primary key value")

    ret = await core.Delete(
        mo.__class__
    ).where(
        table.primary.field == primary_value
    ).do()
    return ret.affected


@util.argschecker(row_data=dict, nullable=False)
def _gen_insert_row(
    m: mtype.ModelType,
    row_data: Dict[str, Any],
    for_replace: bool = False
) -> Dict[str, Any]:
    toinserts = {}
    table = m.__table__
    for name, field in table.fields_dict.items():
        # Primary key fields should not be included when not for_replace
        # if name == table.primary.attr and not for_replace:
        #     continue

        value = row_data.pop(name, None)
        # if value is None, to get default
        if value is None:
            if name == table.primary.attr and table.primary.auto:
                continue
            if hasattr(field, 'default'):
                default = field.default() if callable(field.default) else field.default
                if isinstance(default, _sql.SQL):
                    continue
                value = default
        if value is None and not field.null:
            if not for_replace:
                raise ValueError(
                    f"invalid data(None) for not null attribute {name}"
                )
        try:
            toinserts[field.name] = field.db_value(value)
        except (ValueError, TypeError):
            raise ValueError(f'invalid data({value}) for {name}')

    for attr in row_data:
        # if attr == table.primary.attr and for_replace:
        #     raise err.NotAllowedOperation(
        #         f"auto field {attr!r} not allowed to set"
        #     )
        raise ValueError(f"'{m!r}' has no attribute {attr}")

    return toinserts


def _normalize_insert_rows(
    m: mtype.ModelType,
    rows: List[Union[Dict[str, Any], Tuple[Any, ...]]],
    columns: Optional[List[ttype.Field]] = None,
    for_replace: bool = False,
) -> List[Dict[str, Any]]:
    cleaned_rows = []  # type: List[Dict[str, Any]]

    if columns:
        if not isinstance(columns, list):
            raise ValueError("specify columns must be list")
        mattrs = m.__attrs__
        for c in columns:
            if not isinstance(c, ttype.Field):
                raise TypeError(f"invalid type of columns element {c}")

            if c.name not in mattrs:
                raise ValueError(f"{m!r} has no attribute {c.name}")
            c = mattrs[c.name]

        for row in rows:
            if not isinstance(row, ttype.SEQUENCE):
                raise ValueError(f"invalid data {row!r} for specify columns")
            row = dict(zip(columns, row))  # type: ignore
            if len(row) != len(columns):
                raise ValueError("no enough data for columns")

            cleaned_rows.append(_gen_insert_row(m, row, for_replace))
    else:
        cleaned_rows = [_gen_insert_row(m, r, for_replace) for r in rows]
    return cleaned_rows


def _normalize_update_values(
    m: mtype.ModelType, values: Dict[str, Any]
) -> Dict[str, Any]:
    table = m.__table__
    normalized_values = {}  # type: Dict[str, Any]
    for attr in values:
        f = table.fields_dict.get(attr)
        if f is None:
            raise ValueError(f"'{m!r}' has no attribute {attr}")
        v = values[attr]
        if not isinstance(v, _sql.ClauseElement):
            v = f.db_value(v)
        normalized_values[f.name] = v
    return normalized_values
