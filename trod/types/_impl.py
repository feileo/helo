"""
    trod.types._impl
    ~~~~~~~~~~~~~~~~
"""
from __future__ import annotations

import calendar
import datetime
import decimal
import time
import uuid
import warnings
from typing import Any, Optional, Union, Callable, List, Tuple

from .. import utils, errors
from ..g import _helper as gh, SQL, SEQUENCE, ENCODINGS


Id = Union[int, str]
IdList = List[Union[int, str]]


class ColumnBase(gh.Node):

    __slots__ = ()

    OPERATOR = utils.Tdict(
        AND='AND',
        OR='OR',
        ADD='+',
        SUB='-',
        MUL='*',
        DIV='/',
        BIN_AND='&',
        BIN_OR='|',
        XOR='#',
        MOD='%',
        EQ='=',
        LT='<',
        LTE='<=',
        GT='>',
        GTE='>=',
        NE='!=',
        IN='IN',
        NOT_IN='NOT IN',
        IS='IS',
        IS_NOT='IS NOT',
        LIKE='LIKE BINARY',
        ILIKE='LIKE',
        EXISTS='EXISTS',
        NEXISTS='NOT EXISTS',
        BETWEEN='BETWEEN',
        NBETWEEN='NOT BETWEEN',
        REGEXP='REGEXP BINARY',
        IREGEXP='REGEXP',
        BITWISE_NEGATION='~',
        CONCAT='||',
    )

    def __and__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.AND, rhs)

    def __rand__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.AND, self)

    def __or__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.OR, rhs)

    def __ror__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.OR, self)

    def __add__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.ADD, rhs)

    def __radd__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.ADD, self)

    def __sub__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.SUB, rhs)

    def __rsub__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.SUB, self)

    def __mul__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.MUL, rhs)

    def __rmul__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.MUL, self)

    def __div__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.DIV, rhs)

    def __rdiv__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.DIV, self)

    __truediv__ = __div__
    __rtruediv__ = __rdiv__

    def __xor__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.XOR, rhs)

    def __rxor__(self, lhs: Any) -> Expression:
        return Expression(lhs, self.OPERATOR.XOR, self)

    def __eq__(self, rhs: Any) -> Expression:  # type:ignore
        op = self.OPERATOR.IS if rhs is None else self.OPERATOR.EQ
        return Expression(self, op, rhs)

    def __ne__(self, rhs: Any) -> Expression:  # type:ignore
        op = self.OPERATOR.IS_NOT if rhs is None else self.OPERATOR.NE
        return Expression(self, op, rhs)

    def __lt__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.LT, rhs)

    def __le__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.LTE, rhs)

    def __gt__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.GT, rhs)

    def __ge__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.GTE, rhs)

    def __lshift__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.IN, rhs)

    def __rshift__(self, rhs: Any):
        return Expression(self, self.OPERATOR.IS, rhs)

    def __mod__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.LIKE, rhs)

    def __pow__(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.ILIKE, rhs)

    def __getitem__(self, item: Any) -> Expression:
        if isinstance(item, slice):
            if item.start is None or item.stop is None:
                raise ValueError(
                    'BETWEEN range must have both a start and end-point.'
                )
            return self.between(item.start, item.stop)
        return self == item

    def concat(self, rhs: Any) -> StrExpression:
        return StrExpression(self, self.OPERATOR.CONCAT, rhs)

    def binand(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.BIN_AND, rhs)

    def binor(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.BIN_OR, rhs)

    def in_(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.IN, rhs)

    def nin_(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.NOT_IN, rhs)

    def exists(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.EXISTS, rhs)

    def nexists(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.NEXISTS, rhs)

    def isnull(self, is_null: bool = True) -> Expression:
        op = self.OPERATOR.IS if is_null else self.OPERATOR.IS_NOT
        return Expression(self, op, None)

    def regexp(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.REGEXP, rhs)

    def iregexp(self, rhs: Any) -> Expression:
        return Expression(self, self.OPERATOR.IREGEXP, rhs)

    def like(self, rhs: Any, i: bool = True) -> Expression:
        if i:
            return Expression(self, self.OPERATOR.ILIKE, rhs)
        return Expression(self, self.OPERATOR.LIKE, rhs)

    def contains(self, rhs: Any, i: bool = False) -> Expression:
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"%{rhs}%")
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}%")

    def startswith(self, rhs: Any, i: bool = False) -> Expression:
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"{rhs}%")
        return Expression(self, self.OPERATOR.ILIKE, f"{rhs}%")

    def endswith(self, rhs: Any, i: bool = False) -> Expression:
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"%{rhs}")
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}")

    def between(self, low: Any, hig: Any) -> Expression:
        return Expression(
            self, self.OPERATOR.BETWEEN, gh.NodeList([low, self.OPERATOR.AND, hig])
        )

    def nbetween(self, low: Any, hig: Any) -> Expression:
        return Expression(
            self, self.OPERATOR.NBETWEEN, gh.NodeList([low, self.OPERATOR.AND, hig])
        )

    def asc(self) -> Ordering:
        return Ordering(self, "ASC")

    def desc(self) -> Ordering:
        return Ordering(self, "DESC")

    def as_(self, alias: str) -> gh.Node:
        if alias:
            return Alias(self, alias)
        return self


class Ordering(gh.Node):

    def __init__(self, node: gh.Node, key: str) -> None:
        self.node = node
        self.key = key

    def __sql__(self, ctx: gh.Context):
        ctx.sql(self.node).literal(f" {self.key} ")
        return ctx


class Alias(gh.Node):

    @utils.argschecker(alias=str, nullable=False)
    def __init__(self, node: gh.Node, alias: str) -> None:
        self.node = node
        self.alias = alias

    def __sql__(self, ctx: gh.Context):
        ctx.sql(self.node).literal(" AS {self.alias} ")
        return ctx


class Expression(ColumnBase):

    __slots__ = ('lhs', 'op', 'rhs', 'parens')

    def __init__(
        self, lhs: Any, op: str, rhs: Any, parens: bool = True
    ) -> None:
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.parens = parens

    def __sql__(self, ctx: gh.Context):
        overrides = {'parens': self.parens, 'params': True}

        if isinstance(self.lhs, FieldBase):
            overrides['converter'] = self.lhs.db_value
        elif isinstance(self.rhs, FieldBase):
            overrides['converter'] = self.rhs.db_value

        if self.op in (self.OPERATOR.IN,
                       self.OPERATOR.NOT_IN,
                       self.OPERATOR.EXISTS,
                       self.OPERATOR.NEXISTS):
            if not isinstance(self.rhs, SEQUENCE):
                raise TypeError(
                    f"Invalid values for operator '{self.op}', "
                    f"expected {SEQUENCE}")
            self.rhs = tuple(self.rhs)
            overrides['nesting'] = True

        with ctx(**overrides):
            ctx.sql(
                self.lhs
            ).literal(
                self.op
            ).sql(self.rhs)

        return ctx


class StrExpression(Expression):

    def __add__(self, rhs: Any) -> StrExpression:
        return self.concat(rhs)

    def __radd__(self, lhs: Any) -> StrExpression:
        return StrExpression(lhs, self.OPERATOR.CONCAT, self)


class DDL:

    __slots__ = ('defi',)
    __types__ = utils.Tdict(
        sit='{type}',
        wlt='{type}({length})',
        wdt='{type}({length}, {float_length})',
    )

    def __init__(self, field: FieldBase) -> None:

        defi = gh.NodeList([SQL(field.column), self.parse_type(field)])

        ops = self.parse_options(field)
        if ops.unsigned:
            defi.append(SQL("unsigned"))
        if ops.encoding:
            defi.append(SQL(f"CHARACTER SET {ops.encoding}"))
        if ops.zerofill:
            defi.append(SQL("zerofill"))
        defi.append(SQL("NULL" if ops.allow_null else "NOT NULL"))
        if ops.default:
            defi.append(self.parse_default(ops.auto, ops.default, ops.adapt))
        if ops.comment:
            defi.append(SQL(f"COMMENT '{ops.comment}'"))

        self.defi = defi

    def parse_type(self, field: FieldBase) -> SQL:
        type_render = {'type': field.db_type}

        type_tpl = self.__types__.sit
        length = getattr(field, 'length', None)

        if length:
            type_render['length'] = length
            type_tpl = self.__types__.wlt
        if isinstance(length, tuple) and len(length) == 2:
            type_render['length'] = length[0]
            float_length = length[1]
            if float_length:
                type_render['float_length'] = float_length
                type_tpl = self.__types__.wdt

        return SQL(type_tpl.format(**type_render))

    def parse_options(self, field: FieldBase) -> utils.Tdict:
        return utils.Tdict(
            auto=getattr(field, 'auto', None),
            unsigned=getattr(field, 'unsigned', None),
            zerofill=getattr(field, 'zerofill', None),
            encoding=getattr(field, 'encoding', None),
            allow_null=field.null,
            default=field.default,
            comment=field.comment,
            adapt=field.to_str,
        )

    def parse_default(
            self, auto: bool, default: Any, adapt: Callable
    ) -> SQL:

        if auto:
            return SQL("AUTO_INCREMENT")
        if default:
            if isinstance(default, SQL):
                default = f"DEFAULT {default.sql}"
            elif callable(default):
                default = f"DEFAULT NULL"
            else:
                default = adapt(default)
                default = f"DEFAULT '{default}'"
        else:
            default = "DEFAULT NULL"

        return SQL(default)


class FieldBase(ColumnBase):

    __slots__ = ('null', 'default', 'comment', 'name', '_seqnum')

    py_type = None  # type: Any
    db_type = None  # type: Any

    _field_counter = 0

    @utils.argschecker(null=bool, comment=str)
    def __init__(
        self,
        null: bool,
        default: Any,
        comment: str,
        name: Optional[str] = None
    ) -> None:

        if default:
            if not isinstance(default, (self.py_type, SQL)) and not callable(default):
                raise TypeError(
                    f"Invalid {self.__class__.__name__} default value ({default})"
                )

        self.null = null
        self.default = default
        self.comment = comment
        self.name = name

        FieldBase._field_counter += 1
        self._seqnum = FieldBase._field_counter
        self._custom_wain()

    def __def__(self) -> gh.NodeList:
        return DDL(self).defi

    def __repr__(self) -> str:
        ddl_def = gh.Context().parse(self).query().sql
        ispk = getattr(self, 'primary_key', False)
        extra = ""
        if ispk:
            extra = " [PRIMARY KEY]"
            if getattr(self, 'auto', False):
                extra = " [PRIMARY KEY, AUTO_INCREMENT]"
        return f"types.{self.__class__.__name__}({ddl_def}{extra})"

    def __str__(self) -> str:
        return f"types.{self.__class__.__name__}({self.name})"

    def __hash__(self) -> int:
        if self.name:
            return hash(self.name)
        raise errors.NoColumnNameError()

    def _custom_wain(self) -> None:
        if not self.null and self.default is None:
            if not getattr(self, 'primary_key', False):
                warnings.warn(
                    'Not to give default value for '
                    f'NOT NULL field {self.__class__.__name__}'
                )

    @property
    def seqnum(self) -> int:
        return self._seqnum

    @property
    def column(self) -> str:
        if self.name:
            return f'`{self.name}`'
        raise errors.NoColumnNameError()

    def adapt(self, value: Any) -> Any:
        return value

    def to_str(self, value: Any) -> str:
        if value is None:
            raise ValueError()
        return str(self.db_value(value))

    def py_value(self, value: Any) -> Any:
        return value if value is None else self.adapt(value)

    def db_value(self, value: Any) -> Any:
        return value if value is None else self.adapt(value)

    def __sql__(self, ctx):
        ctx.literal(self.column)
        return ctx


class Tinyint(FieldBase):

    __slots__ = ('length', 'unsigned', 'zerofill')

    py_type = int
    db_type = 'tinyint'
    default_length = 4

    def __init__(
        self,
        length: int = default_length,
        unsigned: bool = False,
        zerofill: bool = False,
        null: bool = True,
        default: Optional[Union[int, SQL, Callable]] = None,
        comment: str = '',
        name: Optional[str] = None
    ) -> None:
        self.length = length
        self.unsigned = unsigned
        self.zerofill = zerofill
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def adapt(self, value: Any) -> int:
        return self.py_type(value)


class Smallint(Tinyint):

    __slots__ = ()

    db_type = 'smallint'
    default_length = 6


class Int(Tinyint):

    __slots__ = ('primary_key', 'auto')

    db_type = 'int'
    default_length = 11

    def __init__(
            self,
            length: int = default_length,
            unsigned: bool = False,
            zerofill: bool = False,
            primary_key: bool = False,
            auto: bool = False,
            null: bool = True,
            default: Optional[Union[int, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        self.primary_key = primary_key
        self.auto = auto
        if self.primary_key is True:
            if null or default:
                raise errors.ProgrammingError("Primary key field not allow null")
            if default:
                raise errors.ProgrammingError("Primary key field not allow set default")
        elif self.auto:
            raise errors.ProgrammingError(
                "'AUTO_INCREMENT' cannot be set for non-primary key fields",
            )

        super().__init__(
            length=length, unsigned=unsigned, zerofill=zerofill,
            null=null, default=default, comment=comment, name=name
        )


class Bigint(Int):

    __slots__ = ()

    db_type = 'bigint'
    default_length = 20


class Auto(Int):

    __slots__ = ()
    default_length = 11

    def __init__(
            self,
            length: int = default_length,
            unsigned: bool = False,
            zerofill: bool = False,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        super().__init__(
            length=length, unsigned=unsigned, zerofill=zerofill,
            primary_key=True, auto=True,
            null=False, default=None, comment=comment, name=name
        )


class BigAuto(Auto):

    __slots__ = ()
    db_type = 'bigint'
    default_length = 20


class Bool(FieldBase):

    __slots__ = ()

    py_type = bool
    db_type = 'bool'

    def __init__(
            self,
            null: bool = True,
            default: Optional[Union[bool, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def adapt(self, value: Any) -> bool:
        return self.py_type(value)

    def to_str(self, value: Any) -> str:
        if self.py_value(value):
            return "1"
        return "0"


class Float(FieldBase):

    __slots__ = ('length', 'unsigned',)

    py_type = float  # type: Any
    db_type = 'float'

    def __init__(
            self,
            length: Optional[Union[int, tuple]] = None,
            unsigned: bool = False,
            null: bool = True,
            default: Optional[Union[float, int, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        if not length or isinstance(length, int):
            self.length = length
        else:
            if isinstance(length, SEQUENCE) and len(length) == 2:
                self.length = tuple(length)
            else:
                raise TypeError(f"Invalid `Float` length type({length})")
        self.unsigned = unsigned
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def adapt(self, value: Any) -> float:
        return self.py_type(value)


class Double(Float):

    __slots__ = ()

    db_type = 'double'


class Decimal(Float):

    __slots__ = ('length', 'unsigned', 'max_digits', 'decimal_places',
                 'auto_round', 'rounding')

    py_type = decimal.Decimal
    db_type = 'decimal'
    default_md = (10, 5)

    def __init__(
            self,
            length: tuple = default_md,
            unsigned: bool = False,
            null: bool = True,
            auto_round: bool = False,
            rounding: Optional[str] = None,
            default: Optional[Union[str, float, decimal.Decimal, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        if not isinstance(length, tuple) or len(length) != 2:
            raise TypeError(f"`Decimal` length type must be tuple or list ")
        self.length = tuple(length)
        self.unsigned = unsigned
        self.max_digits = self.length[0]
        self.decimal_places = self.length[1]
        self.auto_round = auto_round
        self.rounding = rounding or decimal.DefaultContext.rounding
        super().__init__(  # type: ignore
            null=null, default=default, comment=comment, name=name
        )

    def db_value(self, value: Any) -> Optional[decimal.Decimal]:
        if not value:
            return value if value is None else self.py_type(0)
        if self.auto_round:
            exp = self.py_type(10) ** (-self.decimal_places)
            rounding = self.rounding
            return self.py_type(str(value)).quantize(exp, rounding=rounding)
        return value

    def py_value(self, value: Any) -> Optional[decimal.Decimal]:
        if value is not None:
            if isinstance(value, self.py_type):
                return value
            return self.py_type(str(value))
        return None


class Text(FieldBase):

    __slots__ = ('encoding',)

    py_type = str
    db_type = 'text'

    def __init__(
            self,
            encoding: Optional[str] = None,
            null: bool = True,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        if encoding not in ENCODINGS:
            raise ValueError(f"Unsupported encoding '{encoding}'")
        self.encoding = encoding
        super().__init__(
            null=null, default=None, comment=comment, name=name
        )

    def __add__(self, other: Any) -> StrExpression:
        return StrExpression(self, self.OPERATOR.CONCAT, other)

    def __radd__(self, other: Any) -> StrExpression:
        return StrExpression(other, self.OPERATOR.CONCAT, self)

    def _custom_wain(self) -> None:
        pass

    def adapt(self, value: Any) -> str:
        return self.py_type(value)


class Char(FieldBase):

    __slots__ = ('length', 'encoding',)

    py_type = str
    db_type = 'char'

    def __init__(
            self,
            length: int = 255,
            encoding: str = None,
            null: bool = True,
            default: Optional[Union[str, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        self.length = length
        self.encoding = encoding
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __add__(self, other: Any) -> StrExpression:
        return StrExpression(self, self.OPERATOR.CONCAT, other)

    def __radd__(self, other: Any) -> StrExpression:
        return StrExpression(other, self.OPERATOR.CONCAT, self)

    def adapt(self, value: Any) -> str:
        return self.py_type(value)


class VarChar(Char):

    __slots__ = ()

    db_type = 'varchar'


class UUID(FieldBase):

    __slots__ = ("primary_key",)

    py_type = uuid.UUID
    db_type = "varchar(40)"

    def __init__(
            self,
            primary_key: bool = False,
            default: Optional[Union[str, uuid.UUID, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        self.primary_key = primary_key
        super().__init__(
            null=False, default=default, comment=comment, name=name
        )

    def db_value(self, value: Any) -> str:
        if isinstance(value, str) and len(value) == 32:
            return value
        if isinstance(value, bytes) and len(value) == 16:
            value = self.py_type(bytes=value)

        if isinstance(value, self.py_type):
            return value.hex
        try:
            return self.py_type(value).hex
        except Exception:
            return value

    def py_value(self, value: Any) -> Optional[uuid.UUID]:
        if isinstance(value, self.py_type):
            return value
        return self.py_value(value) if value is not None else None


def format_datetime(
        value: str,
        formats: Union[List[str], tuple],
        extractor: Optional[Callable] = None
) -> Any:
    extractor = extractor or (lambda x: x)
    for fmt in formats:
        try:
            return extractor(datetime.datetime.strptime(value, fmt))
        except ValueError:
            pass
    return value


def simple_datetime(value: str) -> Union[datetime.datetime, str]:
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return value


def dt_strftime(value: Any, formats: Union[List[str], tuple]) -> str:
    if hasattr(value, 'strftime'):
        for fmt in formats:
            try:
                return value.strftime(fmt)
            except (TypeError, ValueError):
                pass
    return value


class Date(FieldBase):

    py_type = datetime.datetime
    db_type = 'date'

    formats = (
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
    )

    def __init__(
            self,
            formats: Optional[Union[List[str], tuple, str]] = None,
            null: bool = True,
            default: Optional[Union[datetime.datetime, datetime.date, str, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        if formats is not None:
            self.formats = formats  # type: ignore
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __call__(self, *_args: Any, **_kwargs: Any) -> datetime.date:
        return datetime.datetime.now().date()

    def adapt(self, value: Any) -> Optional[datetime.date]:
        if value and isinstance(value, str):
            value = format_datetime(value, self.formats, lambda x: x.date())
        elif value and isinstance(value, datetime.datetime):
            value = value.date()
        return value

    def to_str(self, value: Any) -> str:
        return dt_strftime(self.db_value(value), self.formats)


class Time(Date):

    __slots__ = ()

    db_type = 'time'

    formats = (  # type: ignore
        '%H:%M:%S.%f',
        '%H:%M:%S',
        '%H:%M',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
    )

    def __call__(self, *_args: Any, **_kwargs: Any) -> datetime.time:  # type: ignore
        return datetime.datetime.now().time()

    def adapt(self, value: Any) -> Optional[datetime.time]:  # type:ignore
        if value:
            if isinstance(value, str):
                value = format_datetime(value, self.formats, lambda x: x.time())  # type: ignore
            elif isinstance(value, datetime.datetime):
                value = value.time()
        if value is not None and isinstance(value, datetime.timedelta):
            return (datetime.datetime.min + value).time()
        return value


class DateTime(Date):

    __slots__ = ()

    db_type = 'datetime'

    formats = (
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    )

    def __call__(self, *args: Any, **kwargs: Any) -> datetime.datetime:
        return datetime.datetime.now()

    def adapt(self, value: Any) -> Optional[datetime.datetime]:  # type: ignore
        if value and isinstance(value, str):
            return format_datetime(value, self.formats)
        return value


class Timestamp(FieldBase):

    __slots__ = ('utc',)

    py_type = datetime.datetime
    db_type = 'timestamp'

    def __init__(
            self,
            utc: bool = False,
            null: bool = True,
            default: Optional[Union[datetime.datetime, str, SQL, Callable]] = None,
            comment: str = '',
            name: Optional[str] = None
    ) -> None:
        self.utc = utc
        if not default:
            default = datetime.datetime.utcnow if self.utc else datetime.datetime.now
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def _custom_wain(self) -> None:
        pass

    def db_value(self, value: Union[datetime.datetime, int]) -> Optional[int]:
        if value is None:
            return value
        if not isinstance(value, datetime.datetime):
            if isinstance(value, datetime.date):
                value = datetime.datetime(value.year, value.month, value.day)
            else:
                return int(round(value))

        if self.utc:
            timestamp = calendar.timegm(value.utctimetuple())  # type: ignore
        else:
            timestamp = time.mktime(value.timetuple())  # type: ignore

        return int(round(timestamp))

    def py_value(self, value: Any) -> Optional[datetime.datetime]:
        if value is not None and isinstance(value, (int, float)):
            if self.utc:
                value = datetime.datetime.utcfromtimestamp(value)
            else:
                value = datetime.datetime.fromtimestamp(value)
        return value


class Func(gh.Node):

    __slots__ = ('_func', '_node')
    _supported = (
        "SUM",
        "AVG",
        "MAX",
        "MIN",
        "COUNT",
    )

    def __init__(self, func: str, node: ColumnBase) -> None:
        self._func = func
        self._node = node

    def __getattr__(self, func: str):
        if func.upper() not in self._supported:
            raise RuntimeError(f"Not supported func: {func}")

        def decorator(*args, **kwargs):
            return Func(func, *args, **kwargs)

        return decorator

    def as_(self, alias: str) -> str:
        return Alias(self, alias)

    def __sql__(self, ctx: gh.Context):
        ctx.literal(self._func)
        with ctx(parens=True):
            ctx.sql(self._node)
        return ctx


FS = Func("", None)  # type: ignore


class IndexBase(gh.Node):

    __slots__ = ('fields', 'comment', 'name', '_seqnum')
    __type__ = None  # type: SQL

    _field_counter = 0

    def __init__(
            self,
            name: str,
            fields: Union[str, List[str], Tuple[str, ...]],
            comment: Optional[str] = None
    ) -> None:
        self.name = name
        self.comment = comment

        if not isinstance(fields, SEQUENCE):
            fields = [fields]  # type: ignore

        self.fields = []  # type: List[str]
        for f in fields:
            if not isinstance(f, str):
                raise TypeError()
            self.fields.append(f"`{f}`")

        IndexBase._field_counter += 1
        self._seqnum = IndexBase._field_counter

    @property
    def seqnum(self) -> int:
        return self._seqnum

    def __def__(self) -> gh.NodeList:
        return gh.NodeList([
            self.__type__,
            SQL(f'`{self.name}`'),
            gh.EnclosedNodeList(self.fields),  # type: ignore
            SQL(f"COMMENT '{self.comment}'"),
        ])

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        ddl_def = gh.Context().parse(self).query().sql
        return f"types.{self.__class__.__name__}({ddl_def})"

    def __str__(self) -> str:
        return self.name

    def __sql__(self, ctx: gh.Context):
        ctx.literal(f'`{self.name}`')
        return ctx


class Key(IndexBase):

    __slots__ = ()
    __type__ = SQL("KEY")


class UKey(IndexBase):

    __slots__ = ()
    __type__ = SQL("UNIQUE KEY")