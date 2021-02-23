from __future__ import annotations

from functools import wraps
from typing import Any, Dict, Optional, List, Union, Tuple, Callable

from . import typ as mtype
from .. import db
from .. import util
from .. import err
from .. import _sql
from ..types import core as ttype, Func


# class AssignmentList(_sql.ClauseElement):

#     __slots__ = ('_data_dict',)

#     # _VSM = "`{col}` = {val}"

#     def __init__(self, data: Dict[str, Any]) -> None:
#         self._data_dict = data

#     def __sql__(self, ctx: _sql.Context) -> _sql.Context:
#         # values, params = [], []
#         # for col, value in self._data_dict.items():
#         # if isinstance(value, ttype.Field):
#         #     values.append(_sql.SQL(
#         #         self._VSM.format(
#         #             col=col,
#         #             val="`{}`.`{}`".format(
#         #                 value.__table__.name,
#         #                 value.name)
#         #         )
#         #     ))
#         # elif isinstance(value, ttype.Expression):
#         #     query = _sql.query(value)
#         #     values.append(_sql.SQL(
#         #         self._VSM.format(
#         #             col=col,
#         #             val=query.sql
#         #         )
#         #     ))
#         #     params.append(query.params)
#         # else:
#         #     values.append(_sql.SQL(
#         #         self._VSM.format(col=col, val='%s')
#         #     ))
#         #     params.append(value)

#         ctx.sql(
#             _sql.CommaClauseElements([
#                 AssignmentPair(c, v) for c, v in self._data_dict.items()
#             ])
#         )
#         # if params:
#         #     ctx.values(params)

#         return ctx


class AssignmentPair(_sql.ClauseElement):

    __slots__ = ('_name', '_value')

    def __init__(self, name: str, value: Any) -> None:
        self._name = name
        self._value = value

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.sql(
            _sql.EscapedElement(self._name)
        ).literal(
            " = "
        )
        if isinstance(self._value, ttype.Field):
            ctx.sql(
                self._value
            )
        elif isinstance(self._value, ttype.Expression):
            # query = _sql.query(self._value)
            ctx.sql(
                self._value
            )
            # ctx.literal(
            #     query.sql
            # ).values(
            #     query.params
            # )
        else:
            ctx.sql(
                _sql.Value(self._value)
            )
        return ctx


class SelectedColumns(_sql.ClauseElement):

    __slots__ = ("columns", "from1", "from2", "_for_all")

    def __init__(
        self,
        columns: List[_sql.ClauseElement],
        from1: ttype.Table,
        from2: Optional[ttype.Table] = None
    ) -> None:
        self._for_all = not columns
        self.columns = columns
        if self._for_all:
            self.columns.append(_sql.SQL("*"))
        self.from1 = from1
        self.from2 = from2

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        # if not self.from2 or self._for_all:
        if self._for_all:
            ctx.sql(_sql.CommaClauseElements(self.columns))
            return ctx

        for idx, col in enumerate(self.columns):
            if isinstance(col, ttype.Field):
                t = col.__table__
                if t is self.from1:
                    continue

                if self.from2 is None or t is not self.from2:
                    raise ValueError("xxxx")

                self.columns[idx] = ttype.Alias(
                    col, f"{ctx.sources[t.name]}.{col.name}"
                )
        ctx.sql(_sql.CommaClauseElements(self.columns))
        return ctx


class ValuesMatch(_sql.ClauseElement):

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
            raise ValueError("invalid data unpack to values")

        self._columns, self._params = [], []  # type: List[_sql.ClauseElement], List[_sql.ClauseElement]
        for col in columns:
            self._columns.append(_sql.SQL(col.join("``")))
            self._params.append(_sql.SQL("%s"))

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            " "
        ).sql(
            _sql.EnclosedClauseElements(self._columns)
        )
        ctx.literal(
            " VALUES "
        ).sql(
            _sql.EnclosedClauseElements(self._params)
        ).values(
            self._values
        )
        return ctx


class Join(_sql.ClauseElement):

    __slots__ = ('lt', 'rt', 'join_type', '_on')

    def __init__(
        self,
        lt: ttype.Table,
        rt: ttype.Table,
        join_type: str = mtype.JOINTYPE.INNER,
        on: Optional[ttype.Expression] = None
    ):
        self.lt = lt
        self.rt = rt
        self.join_type = join_type
        self._on = on

    def on(self, expr: ttype.Expression):
        self._on = expr
        return self

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        with ctx(params=True):
            ctx.sql(
                self.lt
            ).literal(
                f' {self.join_type} JOIN '
            ).sql(
                self.rt
            )
            if self._on is not None:
                ctx.literal(' ON ').sql(self._on)
        return ctx


class Fetcher(_sql.ClauseElement):

    __slots__ = ('_model', '_props', '_aliases', '_sources')
    __r__ = True

    def __init__(self, model: mtype.ModelType) -> None:
        self._model = model
        self._props = util.adict()
        self._aliases = {}  # type: Dict[str, Any]

    def __repr__(self) -> str:
        return repr(self.query())

    def __str__(self) -> str:
        return str(self.query())

    async def __do__(
        self, **props: Any
    ) -> Union[None, util.adict, List[util.adict]]:
        database = self._model.__db__
        if database is None:
            raise err.UnconnectedError(
                "Database is not connected yet, "
                "please call `connect` before"
            )

        if props:
            self._props.update(props)

        return await database.execute(self.query(), **self._props)

    def query(self) -> _sql.Query:
        ctx = _sql.Context.from_clause(
            self, is_mysql=self._model.__db__.scheme == "mysql"
        )
        self._aliases = ctx.aliases
        self._sources = ctx.sources
        q = ctx.query()
        q.r = self.__r__
        return q


class Executor(Fetcher):

    __slots__ = ()
    __r__ = False

    async def do(self) -> db.ExeResult:
        return await self.__do__()


class Select(Fetcher):

    __slots__ = (
        '_columns', '_froms', '_where', '_group_by', '_having',
        '_order_by', '_limit', '_offset', '_inline_model', '_wrap'
    )
    _SINGLE = 1

    def __init__(
        self,
        froms: List[mtype.ModelType],
        *,
        columns: Union[Tuple[ttype.Column, ...], List[ttype.Column]] = (),
    ) -> None:
        if not froms:
            raise ValueError

        if len(froms) > 2:
            raise ValueError

        super().__init__(froms[0])

        self._columns = list(columns)
        self._froms = [m.__table__ for m in froms]
        self._where = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._limit = None     # type: Optional[int]
        self._offset = None    # type: Optional[int]
        self._inline_model = None if len(froms) < 2 else froms[1]
        self._wrap = self._model.__rowtype__ == mtype.ROWTYPE.MODEL

    def adict(self) -> Select:
        self._wrap = False
        return self

    def join(
        self,
        target: mtype.ModelType,
        join_type: str = mtype.JOINTYPE.INNER,
        on: Optional[ttype.Expression] = None
    ) -> Select:
        if self._model.__db__ is not target.__db__:
            raise ValueError(
                "illegal join in different database "
                f"{self._model.__db__} and {target.__db__}"
            )

        if len(self._froms) > 1:
            raise ValueError()

        lt = self._froms.pop()
        rt = target.__table__
        self._froms.append(Join(lt, rt, join_type, on))  # type: ignore
        self._inline_model = target
        return self

    def where(self, *filters: _sql.ClauseElement) -> Select:
        self._where = util.and_(*filters) or None
        return self

    def group_by(self, *columns: ttype.Column) -> Select:
        if not columns:
            raise ValueError("group by clause cannot be empty")
        for f in columns:
            if not isinstance(f, ttype.Column):
                raise TypeError(
                    f"invalid value '{f}' for group_by field"
                )

        self._group_by = columns  # type: ignore
        return self

    def having(self, *filters: _sql.ClauseElement) -> Select:
        self._having = util.and_(*filters) or None
        return self

    def order_by(self, *columns: ttype.Column):
        if not columns:
            raise ValueError("order by clause cannot be empty")
        for f in columns:
            if not isinstance(f, ttype.Column):
                raise TypeError(
                    f"invalid value '{f}' for order_by field")

        self._order_by = columns  # type: ignore
        return self

    def limit(self, limit: int = 500) -> Select:
        self._limit = limit
        return self

    def offset(self, offset: Optional[int] = 0) -> Select:
        if self._limit is None:
            raise err.NotAllowedOperation("offset clause has no limit")
        self._offset = offset
        return self

    async def get(self) -> Union[None, util.adict, mtype.Model]:
        return await self.__do__(rows=self._SINGLE)

    async def first(self) -> Union[None, util.adict, mtype.Model]:
        self.limit(self._SINGLE)
        return await self.__do__(rows=self._SINGLE)

    async def rows(
        self,
        rows: int,
        start: int = 0,
    ) -> Union[List[util.adict], List[mtype.Model]]:
        self.limit(rows).offset(start)
        if rows <= 0:
            raise ValueError(f"invalid select rows: {rows}")
        return await self.__do__()

    async def paginate(
        self,
        page: int,
        size: int = 20,
    ) -> Union[List[util.adict], List[mtype.Model]]:
        if page < 0 or size <= 0:
            raise ValueError("invalid page or size")
        if page > 0:
            page -= 1
        self._limit = size
        self._offset = page * size
        return await self.__do__()

    def all(self) -> Union[List[util.adict], List[mtype.Model]]:
        return self.__do__()

    async def scalar(self) -> Union[int, Dict[str, int]]:
        row = await super().__do__()
        if not row:
            return 0
        return tuple(row.values())[0] if len(row) == 1 else row

    async def count(self) -> int:
        self._columns = [Func.COUNT(_sql.SQL('1'))]  # type: ignore
        return await self.scalar()  # type: ignore

    async def exist(self) -> bool:
        return bool(await self.limit(self._SINGLE).count())

    async def __do__(self, **props) -> Any:
        return Loader(**{
            "data": await super().__do__(**props),
            "model": self._model,
            "inline_model": self._inline_model,
            "aliases": self._aliases,
            "sources": self._sources,
            "wrap": self._wrap
        })()

    async def __aiter__(self) -> Any:
        database = self._model.__db__
        if database is None:
            raise err.UnconnectedError(
                "Database is not connected yet, "
                "please call `connect` before"
            )

        query = self.query()
        async for row in database.iterate(query):
            yield Loader(**{
                "data": row,
                "model": self._model,
                "inline_model": self._inline_model,
                "aliases": self._aliases,
                "sources": self._sources,
                "wrap": self._wrap,
            })()

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.props.assigning = True

        sources = []
        for source in self._froms:
            if isinstance(source, ttype.Table):
                sources.append(source.name)
            else:
                sources.extend([source.lt.name, source.rt.name])
        ctx.source(sources)

        ctx.literal(
            "SELECT "
        ).sql(
            SelectedColumns(
                self._columns,
                self._model.__table__,
                self._inline_model.__table__ if self._inline_model else None
            )
        ).literal(
            " FROM "
        ).sql(
            _sql.CommaClauseElements(self._froms)
        )

        if self._where:
            ctx.literal(" WHERE ").sql(self._where)

        if self._group_by:
            ctx.literal(
                " GROUP BY "
            ).sql(_sql.CommaClauseElements(self._group_by))

        if self._having:
            ctx.literal(" HAVING ").sql(self._having)

        if self._order_by:
            ctx.literal(
                " ORDER BY "
            ).sql(_sql.CommaClauseElements(self._order_by))

        if self._limit is not None:
            ctx.literal(f" LIMIT {self._limit}")

        if self._offset is not None:
            ctx.literal(f" OFFSET {self._offset}")
        return ctx


class Insert(Executor):

    __slots__ = ('_values', '_from')

    def __init__(
        self,
        model: mtype.ModelType,
        values: Union[Dict[str, Any], List[ttype.Field]],
        many: bool = False
    ) -> None:
        super().__init__(model)
        self._values = values
        self._from = None  # type: Optional[Select]
        if many:
            self._props.many = True

    def from_(self, select: Select) -> Insert:
        if not isinstance(select, Select):
            raise TypeError(
                'from select clause must be `Select` object')
        self._from = select
        return self

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            "INSERT INTO "
        ).sql(
            self._model.__table__
        )

        if isinstance(self._values, dict):
            ctx.sql(
                ValuesMatch(self._values)
            )
        elif isinstance(self._values, list):
            for i, f in enumerate(self._values):
                if isinstance(f, str):
                    self._values[i] = _sql.EscapedElement(f)
            ctx.literal(
                ' '
            ).sql(
                _sql.EnclosedClauseElements(self._values)
            )
        if self._from:
            ctx.literal(' ').sql(self._from)

        return ctx


class Replace(Executor):

    __slots__ = ('_values', '_from')

    def __init__(
        self,
        model: mtype.ModelType,
        values: Union[Dict[str, Any]],
        many: bool = False
    ) -> None:
        super().__init__(model)
        self._values = values
        if many:
            self._props.many = True

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            "REPLACE INTO "
        ).sql(self._model.__table__)

        ctx.sql(ValuesMatch(self._values))
        return ctx


class Update(Executor):

    __slots__ = ('_values', '_from', '_where')

    def __init__(
        self, model: mtype.ModelType, values: Dict[str, Any]
    ) -> None:
        super().__init__(model)
        self._values = values
        self._from = None  # type: Optional[ttype.Table]
        self._where = None

    def from_(self, source: mtype.ModelType) -> Update:
        self._from = source.__table__
        return self

    def where(self, *filters: ttype.Column) -> Update:
        self._where = util.and_(*filters) or None
        return self

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            "UPDATE "
        ).sql(
            self._model.__table__
        ).literal(
            " SET "
        )

        if self._from is not None:
            ctx.props.assigning = True
            ctx.source([self._from.name])

        ctx.sql(
            _sql.CommaClauseElements([
                AssignmentPair(c, v) for c, v in self._values.items()
            ])
        )

        if self._from is not None:
            ctx.literal(" FROM ").sql(self._from)

        if self._where is not None:
            ctx.literal(" WHERE ").sql(self._where)
        return ctx


class Delete(Executor):

    __slots__ = ('_where', '_limit', '_force')

    def __init__(self, model: mtype.ModelType, force: bool = False) -> None:
        super().__init__(model)
        self._where = None
        self._limit = None  # type: Optional[int]
        self._force = force

    def where(self, *filters: ttype.Column) -> Delete:
        self._where = util.and_(*filters) or None
        return self

    def limit(self, row_count: int) -> Delete:
        self._limit = row_count
        return self

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            "DELETE FROM "
        ).sql(
            self._model.__table__
        )
        if self._where:
            ctx.literal(
                " WHERE "
            ).sql(self._where)
        elif not self._force:
            raise err.DangerousOperation(
                "delete is too dangerous as no where clause"
            )
        if self._limit is not None:
            ctx.literal(f" LIMIT {self._limit}")

        return ctx


# class Show(Fetcher):

#     __slots__ = ("_key",)

#     _options = {
#         "create": "SHOW CREATE TABLE ",
#         "columns": "SHOW FULL COLUMNS FROM ",
#         "indexes": "SHOW INDEX FROM ",
#     }

#     def __init__(self, model: mtype.ModelType) -> None:
#         super().__init__(model)
#         self._key = None  # type: Optional[str]

#     def __repr__(self) -> str:
#         return f"<Show object for {self._model.__table__!r}>"

#     __str__ = __repr__

#     async def create_syntax(self) -> Optional[util.adict]:
#         self._key = "create"
#         return (await self.__do__(rows=1)).get("Create Table")

#     async def columns(self) -> List[Any]:
#         self._key = "columns"
#         return await self.__do__()

#     async def indexes(self) -> List[Any]:
#         self._key = "indexes"
#         return await self.__do__()

#     def __sql__(self, ctx: _sql.Context) -> _sql.Context:
#         if self._key is not None:
#             ctx.literal(
#                 self._options[self._key]
#             ).sql(
#                 self._model.__table__
#             )
#         return ctx


class Create(Executor):

    __slots__ = ('_options',)

    def __init__(
        self, model: mtype.ModelType, **options: Dict[str, Any]
    ) -> None:
        super().__init__(model)
        self._options = options

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.props.update({
            "safe": self._options.get('safe', True),
            "temporary": self._options.get('temporary', False),
        })
        ctx.sql(self._model.__table__.__ddl__())
        return ctx


class Drop(Create):

    __slots__ = ()

    def __sql__(self, ctx: _sql.Context) -> _sql.Context:
        ctx.literal(
            'DROP TABLE '
        ).sql(
            self._model.__table__
        )
        return ctx


class Loader:

    __slots__ = (
        '_data', '_model_cls', '_inline_model_cls', '_wrap', '_mattrs',
        '_mfields', '_aliases', '_imattrs', '_imfields', '_sources'
    )

    def __init__(
        self,
        data: Union[None, util.adict, List[util.adict]],
        model: mtype.ModelType,
        aliases: Dict[str, str],
        sources: Dict[str, str],
        wrap: bool,
        inline_model: Optional[mtype.ModelType] = None,
    ) -> None:
        self._data = data
        self._model_cls = model
        self._wrap = wrap
        self._aliases = aliases
        self._sources = sources
        self._mattrs = self._model_cls.__attrs__
        self._mfields = self._model_cls.__table__.fields_dict

        self._inline_model_cls = inline_model
        if inline_model:
            self._imattrs = inline_model.__attrs__
            self._imfields = inline_model.__table__.fields_dict

    def adict(self) -> Any:
        self._wrap = False
        return self()

    def __call__(self) -> Any:
        print(self._data)
        print(self._aliases)
        print(self._sources)
        if not self._data:
            return self._data

        if isinstance(self._data, list):
            if self._wrap is True:
                for i in range(len(self._data)):
                    self._data[i] = self._to_model(self._data[i])
            else:
                for i in range(len(self._data)):
                    self._data[i] = self._convert_type(self._data[i])

        elif isinstance(self._data, dict):
            if self._wrap is True:
                self._data = self._to_model(self._data)
            else:
                self._data = self._convert_type(self._data)

        return self._data

    def _convert_type(
        self, row: util.adict
    ) -> util.adict:
        table_name = self._model_cls.__table__.name
        for name, value in row.copy().items():
            aname = self._aliases.get(name, name)
            if '.' in aname:
                table_name, aname = aname.split(".")

            for_join = table_name != self._model_cls.__table__.name
            if not for_join:
                rname = self._mattrs.get(aname)
                f = self._mfields.get(rname)
            else:
                rname = self._imattrs.get(aname)
                f = self._imfields.get(rname)

            # if name not in self._mattrs.values():
            #     rname = self._mattrs.get(aname, aname)
            #     row[rname] = row.pop(name)
            #     name = rname

            if f and not isinstance(value, f.py_type):
                row[name] = f.py_value(value)
        return row

    def _to_model(self, row: util.adict) -> Optional[mtype.Model]:
        model = self._model_cls()
        join_model = None
        for name, value in row.items():
            attr_name, _, join = self._get_field(name)
            if not join:
                setattr(model, attr_name, value)
            else:
                if join_model is None:
                    join_model = self._inline_model_cls()
                setattr(join_model, attr_name, value)

        if join_model:
            model.__dict__["__join__"] = {
                self._inline_model_cls.__name__.lower(): join_model
            }
        return model

    def _get_field(self, name: str) -> Tuple[str, ttype.Field, bool]:
        table_name = self._model_cls.__table__.name

        name = self._aliases.get(name, name)
        if "." in name:
            table_alias, name = name.split(".")
            table_name = self._sources.get(table_alias)

        for_join = table_name != self._model_cls.__table__.name
        attr_name = self._mattrs.get(name)
        if for_join or attr_name is None:
            for_join = True
            attr_name = self._imattrs.get(name)

        if not attr_name:
            raise

        if for_join is False:
            field = self._mfields.get(attr_name)
            if field and field.__table__ is self._model_cls.__table__:
                return attr_name, field, for_join

        for_join = True
        attr_name = self._imattrs.get(name)
        field = self._imfields.get(attr_name)
        return attr_name, field, for_join
