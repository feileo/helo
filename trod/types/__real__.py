import calendar
import datetime
import decimal
import time
import uuid
import warnings
from abc import ABC

from .. import errors, utils


SEQUENCE = (list, tuple)

ENCODINGS = utils.Tdict(
    utf8="utf8",
    utf16="utf16",
    utf32="utf32",
    utf8mb4="utf8mb4",
    gbk="gbk",
    gb2312="gb2312",
)


class ColumnBase:

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

    def __and__(self, rhs):
        return Expression(self, self.OPERATOR.AND, rhs)

    def __rand__(self, lhs):
        return Expression(lhs, self.OPERATOR.AND, self)

    def __or__(self, rhs):
        return Expression(self, self.OPERATOR.OR, rhs)

    def __ror__(self, lhs):
        return Expression(lhs, self.OPERATOR.OR, self)

    def __add__(self, rhs):
        return Expression(self, self.OPERATOR.ADD, rhs)

    def __radd__(self, lhs):
        return Expression(lhs, self.OPERATOR.ADD, self)

    def __sub__(self, rhs):
        return Expression(self, self.OPERATOR.SUB, rhs)

    def __rsub__(self, lhs):
        return Expression(lhs, self.OPERATOR.SUB, self)

    def __mul__(self, rhs):
        return Expression(self, self.OPERATOR.MUL, rhs)

    def __rmul__(self, lhs):
        return Expression(lhs, self.OPERATOR.MUL, self)

    def __div__(self, rhs):
        return Expression(self, self.OPERATOR.DIV, rhs)

    def __rdiv__(self, lhs):
        return Expression(lhs, self.OPERATOR.DIV, self)

    __truediv__ = __div__
    __rtruediv__ = __rdiv__

    def __xor__(self, rhs):
        return Expression(self, self.OPERATOR.XOR, rhs)

    def __rxor__(self, lhs):
        return Expression(lhs, self.OPERATOR.XOR, self)

    def __eq__(self, rhs):
        op = self.OPERATOR.IS if rhs is None else self.OPERATOR.EQ
        return Expression(self, op, rhs)

    def __ne__(self, rhs):
        op = self.OPERATOR.IS_NOT if rhs is None else self.OPERATOR.NE
        return Expression(self, op, rhs)

    def __lt__(self, rhs):
        return Expression(self, self.OPERATOR.LT, rhs)

    def __le__(self, rhs):
        return Expression(self, self.OPERATOR.LTE, rhs)

    def __gt__(self, rhs):
        return Expression(self, self.OPERATOR.GT, rhs)

    def __ge__(self, rhs):
        return Expression(self, self.OPERATOR.GTE, rhs)

    def __lshift__(self, rhs):
        return Expression(self, self.OPERATOR.IN, rhs)

    def __rshift__(self, rhs):
        return Expression(self, self.OPERATOR.IS, rhs)

    def __mod__(self, rhs):
        return Expression(self, self.OPERATOR.LIKE, rhs)

    def __pow__(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, rhs)

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.start is None or item.stop is None:
                raise ValueError(
                    'BETWEEN range must have both a start and end-point.'
                )
            return self.between(item.start, item.stop)
        return self == item

    @property
    def __sname__(self):
        name = getattr(self, "name")
        if name:
            return f"`{name}`"
        raise errors.NoColumnNameError()

    def concat(self, rhs):
        return StrExpression(self, self.OPERATOR.CONCAT, rhs)

    def binand(self, rhs):
        return Expression(self, self.OPERATOR.BIN_AND, rhs)

    def binor(self, rhs):
        return Expression(self, self.OPERATOR.BIN_OR, rhs)

    def in_(self, rhs):
        return Expression(self, self.OPERATOR.IN, rhs)

    def nin_(self, rhs):
        return Expression(self, self.OPERATOR.NOT_IN, rhs)

    def exists(self, rhs):
        return Expression(self, self.OPERATOR.EXISTS, rhs)

    def nexists(self, rhs):
        return Expression(self, self.OPERATOR.NEXISTS, rhs)

    def isnull(self, is_null=True):
        op = self.OPERATOR.IS if is_null else self.OPERATOR.IS_NOT
        return Expression(self, op, None)

    def regexp(self, rhs):
        return Expression(self, self.OPERATOR.REGEXP, rhs)

    def iregexp(self, rhs):
        return Expression(self, self.OPERATOR.IREGEXP, rhs)

    def like(self, rhs, i=True):
        if i:
            return Expression(self, self.OPERATOR.ILIKE, rhs)
        return Expression(self, self.OPERATOR.LIKE, rhs)

    def contains(self, rhs, i=False):
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"%{rhs}%")
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}%")

    def startswith(self, rhs, i=False):
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"{rhs}%")
        return Expression(self, self.OPERATOR.ILIKE, f"{rhs}%")

    def endswith(self, rhs, i=False):
        if not i:
            return Expression(self, self.OPERATOR.LIKE, f"%{rhs}")
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}")

    def between(self, low, hig):
        return Expression(
            self, self.OPERATOR.BETWEEN, NodesComper([low, self.OPERATOR.AND, hig])
        )

    def nbetween(self, low, hig):
        return Expression(
            self, self.OPERATOR.NBETWEEN, NodesComper([low, self.OPERATOR.AND, hig])
        )

    def asc(self):
        return _Ordering(self, ASC)

    def desc(self):
        return _Ordering(self, DESC)

    def as_(self, alias):
        return _Alias(self, alias)


class _Ordering:

    def __init__(self, f, k):
        self.f = f
        self.k = k

    def __repr__(self):
        return self.__sname__

    __str__ = __repr__

    @property
    def __sname__(self):
        return f"{self.f.__sname__} {self.k}"


class _Alias:

    @utils.argschecker(alias=str, nullable=False)
    def __init__(self, field, alias):
        self.f = field
        self.a = alias

    def __repr__(self):
        return self.__sname__

    __str__ = __repr__

    @property
    def __sname__(self):
        return f"{self.f.__sname__} AS `{self.a}`"


class SQL:

    __slots__ = ('_sql',)

    @utils.argschecker(sql=str)
    def __init__(self, sql):
        self._sql = sql

    def __repr__(self):
        return self.sql

    __str__ = __repr__

    @property
    def sql(self):
        return self._sql


class NodesComper:

    __slots__ = ('nodes', 'glue', 'parens', '_sql')

    @utils.argschecker(nodes=list)
    def __init__(self, nodes=None, glue=' ', parens=False):
        self.nodes = nodes or []
        self.glue = glue
        self.parens = parens
        self._sql = []

    def __enter__(self):
        if self.parens:
            self._sql.append('(')

    def __exit__(self, *_args):
        if self.parens:
            self._sql.append(')')
        self._sql = ''.join(self._sql)

    def append(self, node):
        if isinstance(node, list):
            self.nodes.extend(node)
        elif isinstance(node, NodesComper):
            self.nodes.append(node.nodes)
        else:
            self.nodes.append(node)
        return self

    def pop(self):
        return self.nodes.pop()

    def complete(self):
        with self:
            nl = []
            for n in self.nodes:
                if n is not None:
                    if isinstance(n, SQL):
                        nl.append(n.sql)
                    else:
                        nl.append(n)
            self._sql.append(self.glue.join(nl))
        return SQL(self._sql)


class Query:

    __slots__ = ('_sql', '_values',)

    @utils.argschecker(
        sql=SQL, values=SEQUENCE
    )
    def __init__(self, sql, values):
        self._sql = sql
        self._values = values

    def __str__(self):
        return f"Query{self.query}"

    __repr__ = __str__

    @property
    def sql(self):
        return self._sql.sql

    @sql.setter
    @utils.argschecker(sql=SQL)
    def sql(self, sql):
        self._sql = sql

    @property
    def values(self):
        return tuple(self._values)

    @values.setter
    @utils.argschecker(values=SEQUENCE)
    def values(self, values):
        self._values = values

    @property
    def query(self):
        return self.sql, self.values

    @classmethod
    def isquery(cls, obj):
        return hasattr(obj, "__sql__") and hasattr(obj, "__params__")


class Expression(ColumnBase, Query):

    __slots__ = ('lhs', 'op', 'rhs')

    def __init__(self, lhs, op, rhs):

        super().__init__(SQL(""), [])
        self.op = op
        self.lhs, self.rhs = self._adapt(lhs, rhs, op)

    def _add_values(self, value, nesting=False):
        current_values = list(self.values)
        if isinstance(value, SEQUENCE):
            if not nesting:
                current_values.extend(value)
                self.values = current_values
                return
        current_values.append(value)
        self.values = current_values

    def _adapt(self, lhs, rhs, op):

        def converter(hs):
            if isinstance(hs, ColumnBase):
                hs = hs.__sname__
                if hs is None:
                    raise errors.NoColumnNameError()
            elif isinstance(hs, Expression):
                if hs.values:
                    self._add_values(hs.values)
                hs = NodesComper([hs.sql], parens=True).complete().sql
            elif self.isquery(hs):
                self._add_values(hs.__params__)
                hs = hs.__sql__
            elif isinstance(hs, NodesComper):
                hs = hs.complete().sql
            else:
                if op in (self.OPERATOR.IN, self.OPERATOR.NOT_IN, self.OPERATOR.EXISTS,
                          self.OPERATOR.NEXISTS):
                    if not isinstance(hs, SEQUENCE):
                        raise TypeError(
                            f"Invalid values for operator '{op}', expected {SEQUENCE}")
                    hs = tuple(hs)
                self._add_values(hs, nesting=True)
                hs = "'%s'"
            return hs

        lhs = converter(lhs)
        rhs = converter(rhs)
        self.sql = NodesComper([lhs, op, rhs]).complete()
        return lhs, rhs


class StrExpression(Expression):

    def __add__(self, rhs):
        return self.concat(rhs)

    def __radd__(self, lhs):
        return StrExpression(lhs, self.OPERATOR.CONCAT, self)


class Syntax:

    __slots__ = ('defi',)
    __types__ = utils.Tdict(
        sit='{type}',
        wlt='{type}({length})',
        wdt='{type}({length},{float_length})',
    )

    def __init__(self, field):

        defi = NodesComper([field.__sname__, self.parse_type(field)])

        ops = self.parse_options(field)
        if ops.unsigned:
            defi.append(SQL("unsigned"))
        if ops.encoding:
            defi.append(SQL(f"CHARACTER SET {ops.encoding}"))
        if ops.zerofill:
            defi.append(SQL("zerofill"))
        defi.append(SQL("NULL") if ops.allow_null else SQL("NOT NULL"))
        if ops.default:
            defi.append(self.parse_default(ops.auto, ops.default, ops.adapt))
        if ops.comment:
            defi.append(SQL(f"COMMENT '{ops.comment}'"))

        self.defi = defi.complete().sql

    def parse_type(self, field):
        type_render = {'type': field.db_type}

        type_tpl = self.__types__.sit
        length = getattr(field, 'length', None)

        if length:
            type_render['length'] = length
            type_tpl = self.__types__.wlt
        if isinstance(length, tuple) and len(length) == 2:
            float_length = length[1]
            if float_length:
                type_render['float_length'] = float_length
                type_tpl = self.__types__.wdt

        return SQL(type_tpl.format(**type_render))

    def parse_options(self, field):
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

    def parse_default(self, auto, default, adapt):

        if auto:
            return SQL("AUTO_INCREMENT")
        if default:
            default = default if not callable(default) else default()
            if isinstance(default, SQL):
                default = f"DEFAULT {default.sql}"
            else:
                default = adapt(default)
                default = f"DEFAULT '{default}'"
        else:
            default = "DEFAULT NULL"
        return SQL(default)


class FieldBase(ColumnBase):

    __slots__ = ('null', 'default', 'comment', 'name', '_seqnum')

    py_type = None
    db_type = None

    _field_counter = 0

    @utils.argschecker(null=bool, comment=str)
    def __init__(self, null, default, comment, name=None):
        """
        :params default str|callbale object
        """

        if default:
            if isinstance(self.py_type, SEQUENCE):
                py_type = list(self.py_type)
                py_type.append(SQL)
                py_type = tuple(py_type)
            else:
                py_type = (self.py_type, SQL)
            if not isinstance(default, py_type) and not callable(default):
                raise TypeError(
                    f"Invalid {self.__class__.__name__} default value ({default})"
                )

        self.null = null
        self.default = default
        self.comment = comment
        self.name = name

        FieldBase._field_counter += 1
        self._seqnum = FieldBase._field_counter
        self.custom_wain()
        super().__init__()

    @property
    def __def__(self):
        return Syntax(self).defi

    def __repr__(self):
        isprimary_key = getattr(self, 'primary_key', False)
        extra = ""
        if isprimary_key:
            extra = " [PRIMARY KEY]"
            if getattr(self, 'auto', False):
                extra = " [PRIMARY KEY, AUTO_INCREMENT]"
        return f"types.{self.__class__.__name__}({self.__def__}{extra})"

    __str__ = __repr__

    def __hash__(self):
        if self.name:
            return hash(self.name)
        raise errors.NoColumnNameError()

    def custom_wain(self):
        if not self.null and not self.default:
            if not getattr(self, 'primary_key', False):
                warnings.warn(
                    f'Not to give default value for NOT NULL field {self.__class__.__name__}'
                )

    @property
    def seqnum(self):
        return self._seqnum

    def adapt(self, value):
        return value

    def to_str(self, value):
        if value is None:
            return value
        return str(self.db_value(value))

    def py_value(self, value):
        return value if value is None else self.adapt(value)

    def db_value(self, value):
        return value if value is None else self.adapt(value)


class Tinyint(FieldBase):

    __slots__ = ('length', 'unsigned', 'zerofill')

    py_type = int
    db_type = 'tinyint'

    def __init__(self,
                 length=11,
                 unsigned=False,
                 zerofill=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        self.length = length
        self.unsigned = unsigned
        self.zerofill = zerofill
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def adapt(self, value):
        return self.py_type(value)


class Smallint(Tinyint):

    __slots__ = ()

    db_type = 'smallint'


class Int(Tinyint):

    __slots__ = ('primary_key', 'auto',)

    db_type = 'int'

    def __init__(self,
                 length=11,
                 unsigned=False,
                 zerofill=False,
                 primary_key=False,
                 auto=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
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


class Auto(Int):

    __slots__ = ()

    def __init__(self,
                 length=11,
                 unsigned=False,
                 zerofill=False,
                 comment='',
                 name=None):
        super().__init__(
            length=length, unsigned=unsigned, zerofill=zerofill,
            primary_key=True, auto=True,
            null=False, default=None, comment=comment, name=name
        )


class BigAuto(Auto):

    __slots__ = ()

    db_type = 'bigint'


class UUID(FieldBase):

    __slots__ = ()

    py_type = uuid.UUID
    db_type = "varchar(40)"

    def db_value(self, value):
        if isinstance(value, str) and len(value) == 32:
            return value
        elif isinstance(value, bytes) and len(value) == 16:
            value = self.py_type(bytes=value)

        if isinstance(value, self.py_type):
            return value.hex
        try:
            return self.py_type(value).hex
        except Exception:
            return value

    def py_value(self, value):
        if isinstance(value, self.py_type):
            return value
        return self.py_value(value) if value is not None else None


class Bool(FieldBase):

    __slots__ = ()

    py_type = bool
    db_type = 'bool'

    def __init__(self,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def adapt(self, value):
        return self.py_type(value)

    def to_str(self, value):
        if self.py_value(value):
            return "1"
        return "0"


class Float(FieldBase):

    __slots__ = ('length', 'unsigned',)

    py_type = float
    db_type = 'float'

    def __init__(self,
                 length=None,
                 unsigned=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
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

    def adapt(self, value):
        return self.py_type(value)


class Double(Float):

    __slots__ = ()

    db_type = 'double'


class Decimal(Float):

    __slots__ = ('length', 'unsigned', 'max_digits', 'decimal_places',
                 'auto_round', 'rounding')

    db_type = 'decimal'
    py_type = decimal.Decimal
    default_md = (10, 5)

    def __init__(self,
                 length=default_md,
                 unsigned=False,
                 null=True,
                 auto_round=False,
                 rounding=None,
                 default=None,
                 comment='',
                 name=None):
        if not isinstance(length, tuple) or len(length) != 2:
            raise TypeError(f"`Decimal` length type must be tuple or list ")
        self.length = tuple(length)
        self.unsigned = unsigned
        self.max_digits = self.length[0]
        self.decimal_places = self.length[1]
        self.auto_round = auto_round
        self.rounding = rounding or decimal.DefaultContext.rounding
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def db_value(self, value):
        if not value:
            return value if value is None else self.py_type(0)
        if self.auto_round:
            exp = self.py_type(10) ** (-self.decimal_places)
            rounding = self.rounding
            return self.py_type(str(value)).quantize(exp, rounding=rounding)
        return value

    def py_value(self, value):
        if value is not None:
            if isinstance(value, self.py_type):
                return value
            return self.py_type(str(value))
        return None


class Text(FieldBase):

    __slots__ = ('encoding',)

    py_type = str
    db_type = 'text'

    def __init__(self,
                 encoding=None,
                 null=True,
                 comment='',
                 name=None):
        if encoding not in ENCODINGS:
            raise ValueError(f"Unsupported encoding '{encoding}'")
        self.encoding = encoding
        super().__init__(
            null=null, default=None, comment=comment, name=name
        )

    def __add__(self, other):
        return StrExpression(self, self.OPERATOR.CONCAT, other)

    def __radd__(self, other):
        return StrExpression(other, self.OPERATOR.CONCAT, self)

    def custom_wain(self):
        return True

    def adapt(self, value):
        return self.py_type(value)


class Char(FieldBase):

    __slots__ = ('length', 'encoding',)

    py_type = str
    db_type = 'char'

    def __init__(self,
                 length=255,
                 encoding=None,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        self.length = length
        self.encoding = encoding
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __add__(self, other):
        return StrExpression(self, self.OPERATOR.CONCAT, other)

    def __radd__(self, other):
        return StrExpression(other, self.OPERATOR.CONCAT, self)

    def adapt(self, value):
        return self.py_type(value)


class VarChar(Char):

    __slots__ = ()

    db_type = 'varchar'


def format_datetime(value, formats, extractor=None):
    extractor = extractor or (lambda x: x)
    for fmt in formats:
        try:
            return extractor(datetime.datetime.strptime(value, fmt))
        except ValueError:
            pass
    return value


def simple_datetime(value):
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return value


def dt_strftime(value, formats):
    if hasattr(value, 'strftime'):
        for fmt in formats:
            try:
                return value.strftime(fmt)
            except (TypeError, ValueError):
                pass
    return value


class Date(FieldBase):

    __slots__ = ("formats",)

    py_type = datetime.datetime
    db_type = 'date'

    formats = (
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
    )

    def __init__(self,
                 formats=None,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        if formats is not None:
            self.formats = formats
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __call__(self, *args, **kwargs):
        return datetime.datetime.now().date()

    def adapt(self, value):
        if value and isinstance(value, str):
            value = format_datetime(value, self.formats, lambda x: x.date())
        elif value and isinstance(value, datetime.datetime):
            value = value.date()
        return value

    def to_str(self, value):
        return dt_strftime(self.db_value(value), self.formats)


class Time(Date):

    __slots__ = ()

    db_type = 'time(6)'

    formats = (
        '%H:%M:%S.%f',
        '%H:%M:%S',
        '%H:%M',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
    )

    def __call__(self, *args, **kwargs):
        return datetime.datetime.now().time()

    def adapt(self, value):
        if value:
            if isinstance(value, str):
                value = format_datetime(value, self.formats, lambda x: x.time())
            elif isinstance(value, datetime.datetime):
                value = value.time()
        if value is not None and isinstance(value, datetime.timedelta):
            return (datetime.datetime.min + value).time()
        return value


class DateTime(Date):

    __slots__ = ()

    db_type = 'datetime(6)'

    formats = (
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    )

    def __call__(self, *args, **kwargs):
        return datetime.datetime.now()

    def adapt(self, value):
        if value and isinstance(value, str):
            return format_datetime(value, self.formats)
        return value


class Timestamp(FieldBase):

    __slots__ = ('utc',)

    py_type = (datetime.datetime, int)
    db_type = 'timestamp'

    def __init__(self,
                 utc=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        self.utc = utc
        if not default:
            default = datetime.datetime.utcnow if self.utc else datetime.datetime.now
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def custom_wain(self):
        return True

    def db_value(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.date):
            value = datetime.datetime(value.year, value.month, value.day)
        elif not isinstance(value, datetime.datetime):
            return int(round(value))

        if self.utc:
            timestamp = calendar.timegm(value.utctimetuple())
        else:
            timestamp = time.mktime(value.timetuple())

        return int(round(timestamp))

    def py_value(self, value):
        if value is not None and isinstance(value, (int, float)):
            if self.utc:
                value = datetime.datetime.utcfromtimestamp(value)
            else:
                value = datetime.datetime.fromtimestamp(value)
        return value


ASC = SQL("ASC")
DESC = SQL("DESC")
ON_CREATE = SQL("CURRENT_TIMESTAMP")
ON_UPDATE = SQL("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")


class Funcs:

    __slots__ = ('_f',)

    def __init__(self, f):
        self._f = f

    @classmethod
    def sum(cls, field):
        return cls(f"SUM({field.__sname__})")

    @classmethod
    def avg(cls, field):
        return cls(f"AVG({field.__sname__})")

    @classmethod
    def max(cls, field):
        return cls(f"MAX({field.__sname__})")

    @classmethod
    def min(cls, field):
        return cls(f"MIN({field.__sname__})")

    @classmethod
    def count(cls, field):
        return cls(f"COUNT({field.__sname__})")

    def __sname__(self):
        return self._f

    __repr__ = __str__ = __sname__

    def as_(self, alias):
        return _Alias(self, alias)


class IndexBase(ABC):

    __slots__ = ('fields', 'comment', 'name', '_seq_num', )

    __type__ = None

    _field_counter = 0

    def __init__(self, name, fields, comment=None):
        self.name = name
        self.comment = comment

        if not isinstance(fields, SEQUENCE):
            fields = [fields]

        self.fields = []
        for f in fields:
            if not isinstance(f, FieldBase):
                raise TypeError()
            self.fields.append(f.__sname__)

        IndexBase._field_counter += 1
        self._seq_num = IndexBase._field_counter

    def __hash__(self):
        return hash(self.name)

    @property
    def __sname__(self):
        return f"`{self.name}`"

    @property
    def __def__(self):
        fs = NodesComper(self.fields, glue=", ", parens=True).complete()
        cm = SQL(f"COMMENT '{self.comment}'")
        return NodesComper(
            [self.__type__, self.__sname__, fs, cm]).complete().sql

    def __repr__(self):
        return f"types.{self.__class__.__name__}({self.__def__})"

    __str__ = __repr__


class Key(IndexBase):

    __type__ = SQL("KEY")


class UKey(IndexBase):

    __type__ = SQL("UNIQUE KEY")
