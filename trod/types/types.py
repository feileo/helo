import datetime
import decimal
import warnings
import time
import calendar
from abc import ABC

from .. import errors, utils


SEQUENCE = (list, tuple)

ENCODINGS = {"utf8"}


class Column:

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

    def regexp(self, rhs):
        return Expression(self, self.OPERATOR.REGEXP, rhs)

    def iregexp(self, rhs):
        return Expression(self, self.OPERATOR.IREGEXP, rhs)

    def exists(self, rhs):
        return Expression(self, self.OPERATOR.EXISTS, rhs)

    def nexists(self, rhs):
        return Expression(self, self.OPERATOR.NEXISTS, rhs)

    def like(self, rhs):
        return Expression(self, self.OPERATOR.LIKE, rhs)

    def isnull(self, is_null=True):
        op = self.OPERATOR.IS if is_null else self.OPERATOR.IS_NOT
        return Expression(self, op, None)

    def contains(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}%")

    def startswith(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, f"{rhs}%")

    def endswith(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, f"%{rhs}")

    def between(self, low, hig):
        return Expression(
            self, self.OPERATOR.BETWEEN, NodeList([low, self.OPERATOR.AND, hig])
        )

    def nbetween(self, low, hig):
        return Expression(
            self, self.OPERATOR.NBETWEEN, NodeList([low, self.OPERATOR.AND, hig])
        )

    def desc(self):
        return _Desc(self)

    def asc(self):
        return _Asc()

    @utils.argschecker(alias=str, nullable=False)
    def as_(self, alias):
        return _Alias(self, alias)


class SQL:

    __slots__ = ('_sql',)

    @utils.argschecker(sql=str)
    def __init__(self, sql):
        self._sql = sql

    @property
    def sql(self):
        return self._sql


ON_CREATE = SQL("CURRENT_TIMESTAMP")
ON_UPDATE = SQL("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")


class NodeList:

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

    def append(self, nodes):
        if isinstance(nodes, list):
            self.nodes.extend(nodes)
        else:
            self.nodes.append(nodes)
        return self

    def pop(self):
        return self.nodes.pop()

    def complete(self):
        with self:
            self._sql.append(self.glue.join(
                [n.sql if isinstance(n, SQL) else n for n in self.nodes]
            ))
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
    def isself(cls, obj):
        return hasattr(obj, "sql") and hasattr(obj, "values")


class Expression(Column, Query):

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
            if isinstance(hs, FieldBase):
                hs = hs.name
                if hs is None:
                    raise errors.NoColumnNameError()
            elif isinstance(hs, Expression):
                if hs.values:
                    self._add_values(hs.values)
                hs = NodeList([hs.sql], parens=True).complete().sql
            elif self.isself(hs):
                self._add_values(hs.values)
                hs = hs.sql
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
        self.sql = NodeList([lhs, op, rhs]).complete()
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

        defi = NodeList([field.__sname__, self.parse_type(field)])

        ops = self.parse_options(field)
        if ops.unsigned:
            defi.append(SQL("unsigned"))
        if ops.encoding:
            defi.append(SQL(f"CHARACTER SET {ops.encoding}"))
        if ops.zerofill:
            defi.append(SQL("zerofill"))
        defi.append(SQL("NULL") if ops.allow_null else SQL("NOT NULL"))
        if ops.default:
            defi.append(self.parse_default(ops.ai, ops.default, ops.adapt))
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
            ai=getattr(field, 'ai', None),
            unsigned=getattr(field, 'unsigned', None),
            zerofill=getattr(field, 'zerofill', None),
            encoding=getattr(field, 'encoding', None),
            allow_null=field.null,
            default=field.default,
            comment=field.comment,
            adapt=field.to_str,
        )

    def parse_default(self, ai, default, adapt):

        if ai:
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


class FieldBase(Column):

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
        return f"types.{self.__class__.__name__}({self.__def__})"

    __str__ = __repr__

    def __hash__(self):
        pass

    def custom_wain(self):
        if not self.null and not self.default:
            warnings.warn(
                f'Not to give default value for NOT NULL field {self.__class__.__name__}'
            )

    @property
    def seqnum(self):
        return self._seqnum

    def adapt(self, value):
        return value

    def to_str(self, value):
        return str(self.adapt(value))

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

    __slots__ = ('pk', 'ai',)

    db_type = 'int'

    def __init__(self,
                 length=11,
                 unsigned=False,
                 zerofill=False,
                 pk=False,
                 ai=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        self.pk = pk
        self.ai = ai
        if self.pk is True:
            if null or default:
                warnings.warn(
                    "Not allow null or default, corrected to 'null=False, default=None'",
                    errors.ProgrammingWarning
                )
            null = False
            default = None
        elif self.ai:
            warnings.warn(
                "'AUTO_INCREMENT' cannot be set for non-primary key fields, corrected to 'ai=False'",
                errors.ProgrammingWarning
            )
            self.ai = False

        super().__init__(
            length=length, unsigned=unsigned, zerofill=zerofill,
            null=null, default=default, comment=comment, name=name
        )


class Bigint(Int):

    __slots__ = ()

    _db_type = 'bigint'


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


class Auto(FieldBase):
    pass


class BigAuto(FieldBase):
    pass


class UUID(FieldBase):
    pass


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


class Date(FieldBase):

    __slots__ = ('formats',)

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


class Func:

    def __init__(self, field):
        if isinstance(field, Expression):
            pass
        elif isinstance(field, Column):
            pass

    @utils.argschecker(alias=str, nullable=False)
    def as_(self, alias):
        return _Alias(self, alias)


class _Asc:

    def __init__(self, field):
        self.fi = field

    @property
    def __sname__(self):
        return f"{self.fi.sname} ASC"


class _Desc:

    def __init__(self, field):
        self.fi = field

    @property
    def __sname__(self):
        return f"{self.fi.sname} DESC"


class _Alias:

    def __init__(self, field, alias):
        self.fi = field
        self.alias = alias

    @property
    def __sname__(self):
        return f"{self.fi.sname} AS {self.alias}"


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
        pass

    @property
    def __sname__(self):
        return f"`{self.name}`"

    @property
    def __def__(self):
        fs = NodeList(self.fields, glue=", ", parens=True).complete()
        cm = SQL(f"COMMENT '{self.comment}'")
        return NodeList(
            [self.__type__, self.__sname__, fs, cm]).complete().sql

    def __repr__(self):
        return f"types.{self.__class__.__name__}({self.__def__})"

    __str__ = __repr__


class Key(IndexBase):

    __type__ = SQL("KEY")


class UKey(IndexBase):

    __type__ = SQL("UNIQUE KEY")
