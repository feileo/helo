import datetime
import decimal
import warnings
from collections.abc import Iterable

from trod import errors, db_ as db, utils


OPER = utils.Tdict(
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
    LIKE='LIKE',
    ILIKE='ILIKE',
    EXISTS='EXISTS',
    NEXISTS='NOT EXISTS',
    BETWEEN='BETWEEN',
    NBETWEEN='NOT BETWEEN',
    REGEXP='REGEXP',
    IREGEXP='IREGEXP',
    BITWISE_NEGATION='~'
)


class Cnt:

    def __init__(self, pair, op, encap=False):
        if not encap:
            self.pair = pair
        else:
            self.pair = [f'({p})' for p in pair]
        self.op = op

    @property
    def sql(self):
        return f"{self.pair[0]} {self.op} {self.pair[1]}"


class Expr:

    __slots__ = ('lhs', 'op', 'rhs', 'flat', 'logic')

    def __init__(self, lhs, op, rhs, flat=False, logic=False):
        self.lhs = self._hs(lhs, _l=True)
        self.op = op
        self.rhs = self._hs(rhs)
        self.flat = flat
        self.logic = logic

    def __and__(self, expr):
        if isinstance(expr, self.__class__):
            return self.__class__(self.__sql__(), OPER.AND, expr.__sql__(), logic=True)
        raise ValueError()

    def __or__(self, expr):
        if isinstance(expr, self.__class__):
            return self.__class__(self.__sql__(), OPER.OR, expr.__sql__(), logic=True)
        raise ValueError()

    def __sql__(self):
        if self.op in (OPER.IN, OPER.NOT_IN, OPER.EXISTS):
            if not isinstance(self.rhs, Iterable):
                raise ValueError(
                    f"The value of the operator {self.rhs} should be an Iterable object"
                )
            self.rhs = tuple(self.rhs)
        return Cnt((self.lhs, self.rhs), self.op, encap=self.logic).sql

    @property
    def sql(self):
        return self.__sql__()

    def _hs(self, hs, _l=False):

        if isinstance(hs, Column):
            hs = hs.name
        elif isinstance(hs, db.Doer):
            if _l:
                raise RuntimeError()
            hs = f'({hs.sql})'
        # elif isinstance(hs, self.__class__):
        #     hs = hs.sql
        return hs


class Column:

    __slots__ = ('name',)

    def _expr(op, inv=False):  # pylint: disable=all

        def wraper(self, rhs):
            if inv:
                return Expr(rhs, op, self)
            return Expr(self, op, rhs)
        return wraper

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.name}>'

    __str__ = __repr__

    __and__ = _expr(OPER.AND)
    __or__ = _expr(OPER.OR)

    __add__ = _expr(OPER.ADD)
    __sub__ = _expr(OPER.SUB)
    __mul__ = _expr(OPER.MUL)
    __div__ = __truediv__ = _expr(OPER.DIV)
    __xor__ = _expr(OPER.XOR)
    __radd__ = _expr(OPER.ADD, inv=True)
    __rsub__ = _expr(OPER.SUB, inv=True)
    __rmul__ = _expr(OPER.MUL, inv=True)
    __rdiv__ = __rtruediv__ = _expr(OPER.DIV, inv=True)
    __rand__ = _expr(OPER.AND, inv=True)
    __ror__ = _expr(OPER.OR, inv=True)
    __rxor__ = _expr(OPER.XOR, inv=True)

    def __eq__(self, rhs):
        op = OPER.IS if rhs is None else OPER.EQ
        return Expr(self, op, rhs)

    def __ne__(self, rhs):
        op = OPER.IS_NOT if rhs is None else OPER.NE
        return Expr(self, op, rhs)

    __lt__ = _expr(OPER.LT)
    __le__ = _expr(OPER.LTE)
    __gt__ = _expr(OPER.GT)
    __ge__ = _expr(OPER.GTE)
    __lshift__ = _expr(OPER.IN)
    __rshift__ = _expr(OPER.IS)
    __mod__ = _expr(OPER.LIKE)
    __pow__ = _expr(OPER.ILIKE)

    b_and = _expr(OPER.BIN_AND)
    b_or = _expr(OPER.BIN_OR)
    in_ = _expr(OPER.IN)
    nin = _expr(OPER.NOT_IN)
    regexp = _expr(OPER.REGEXP)
    iregexp = _expr(OPER.IREGEXP)
    exists = _expr(OPER.EXISTS)
    not_exists = _expr(OPER.NEXISTS)
    like = _expr(OPER.LIKE)

    def is_null(self, is_null=True):
        op = OPER.IS if is_null else OPER.IS_NOT
        return Expr(self, op, None)

    def contains(self, rhs):
        return Expr(self, OPER.ILIKE, f'%{rhs}%')

    def startswith(self, rhs):
        return Expr(self, OPER.ILIKE, f'{rhs}%')

    def endswith(self, rhs):
        return Expr(self, OPER.ILIKE, f'%{rhs}')

    def between(self, low, hig):
        return Expr(self, OPER.BETWEEN, Cnt((low, hig), OPER.AND).sql)

    def not_between(self, low, hig):
        return Expr(self, OPER.NBETWEEN, Cnt((low, hig), OPER.AND).sql)

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.start is None or item.stop is None:
                raise ValueError(
                    'BETWEEN range must have both a start and end-point.'
                )
            return self.between(item.start, item.stop)
        return self == item

    def desc(self):
        return _Desc(self)

    def asc(self):
        return _Asc()

    @utils.argschecker(alias=str, nullable=False)
    def as_(self, alias):
        return _Alias(self, alias)


class Defi:

    __slots__ = ('_sql',)

    _spaces = ' '

    def __init__(self):
        self._sql = []

    @property
    def sname(self):
        raise NotImplementedError

    @property
    def _type(self):
        raise NotImplementedError

    @property
    def _unsigned(self):
        raise NotImplementedError

    @property
    def _zerofill(self):
        raise NotImplementedError

    @property
    def _encoding(self):
        raise NotImplementedError

    @property
    def _allow_null(self):
        raise NotImplementedError

    @property
    def _default(self):
        raise NotImplementedError

    @property
    def _comment(self):
        raise NotImplementedError

    @property
    def _ai(self):
        raise NotImplementedError

    @property
    def sql(self):
        self._sql.extend([self.sname, self._type])
        if self._unsigned:
            self._sql.append(self._unsigned)
        if self._encoding:
            self._sql.append(self._encoding)
        if self._zerofill:
            self._sql.append(self._zerofill)
        self._sql.extend(self._allow_null)
        if self._default:
            self._sql.append(self._default)
        if self._comment:
            self._sql.append(self._comment)
        return self._space.join(self._sql)


class FieldBase(Column, Defi):

    __slots__ = ('null', 'default', 'comment', '_seq_num', )

    _py_type = None
    _db_type = None

    _field_counter = 0

    # TODO params checker
    def __init__(self, null, default, comment, name=None):

        if not isinstance(null, bool):
            raise ValueError(f"Unexpected `null` type: {null}")

        self.null = null
        self.default = default
        self.comment = comment

        FieldBase._field_counter += 1
        self._seq_num = FieldBase._field_counter
        super().__init__(name)

    @property
    def sname(self):
        return f'`{self.name}`'

    @property
    def _unsigned(self):
        return None

    @property
    def _zerofill(self):
        return None

    @property
    def _encoding(self):
        return None

    @property
    def _allow_null(self):
        return "NULL" if self.null else 'NOT NULL'

    @property
    def _default(self):

        default = None
        if self._ai:
            return 'AUTO_INCREMENT'
        if self.default is not None:
            try:
                default = self._py_type(default)
            except Exception:
                raise ValueError(
                    f'Except default value {self._py_type}, now got {default}'
                )
            default = f"DEFAULT '{default}'"
        else:
            if not self.null:
                warnings.warn(f'Not to give default value for NOT NULL field {self.name}')
            default = 'DEFAULT NULL'
        return default

    @property
    def _comment(self):
        if self.comment:
            return f"COMMENT '{self.comment}'"
        return None

    @property
    def _ai(self):
        return False

    @property
    def seq_num(self):
        return self._seq_num


class Tinyint(FieldBase):

    __slots__ = ('unsigned', 'length', 'zerofill')

    _py_type = int
    _db_type = 'tinyint'

    _type_tpl = '{type}({length})'

    def __init__(self,
                 length,
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
            name=name, null=null, default=default, comment=comment
        )

    @property
    def _type(self):
        return self._type_tpl.format(
            type=self._db_type, length=self.length
        )

    @property
    def _unsigned(self):
        if self.unsigned:
            return 'unsigned'
        return None

    @property
    def _zerofill(self):
        if self.zerofill:
            return 'zerofill'
        return None


class Smallint(Tinyint):

    __slots__ = ()

    _db_type = 'smallint'


class Int(Tinyint):

    __slots__ = ('pk', 'ai',)

    _db_type = 'int'

    def __init__(self,
                 length,
                 unsigned=False,
                 zerofill=False,
                 null=True,
                 pk=False,
                 ai=False,
                 default=None,
                 comment='',
                 name=None):
        self.pk = pk
        self.ai = ai
        if self.pk is True:
            if null:
                warnings.warn(
                    'Primary key is not allow null, use default',
                    errors.ProgrammingWarning
                )
            null = False
            default = None
        elif self.ai:
            warnings.warn(
                "'AUTO_INCREMENT' cannot be set for non-primary key fields, use default",
                errors.ProgrammingWarning
            )
            self.ai = False

        super().__init__(
            name=name, null=null, default=default, comment=comment,
            length=length, unsigned=unsigned, zerofill=zerofill
        )

        @property
        def _ai(self):
            return True


class Bigint(Int):

    __slots__ = ()

    _db_type = 'bigint'


class Text(FieldBase):

    __slots__ = ('encoding',)

    _py_type = str
    _db_type = 'text'
    _type_tpl = '{type}'

    def __init__(self,
                 encoding=None,
                 null=True,
                 comment='',
                 name=None):
        self.encoding = encoding
        super().__init__(
            name=name, null=null, default=None, comment=comment
        )

    @property
    def _type(self):
        return self._type_tpl.format(type=self._db_type)

    @property
    def _encoding(self):
        if not self.encoding:
            return None
        return f'CHARACTER SET {self.encoding}'

    @property
    def _default(self):
        return None


class Char(FieldBase):

    __slots__ = ('length', 'encoding',)

    _py_type = str
    _db_type = 'char'

    _type_tpl = '{type}({length})'

    def __init__(self,
                 length,
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

    @property
    def _type(self):
        return self._type_tpl.format(type=self._db_type, length=self.length)

    @property
    def _encoding(self):
        if not self.encoding:
            return None
        return f'CHARACTER SET {self.encoding}'


class VarChar(Char):

    __slots__ = ()

    _db_type = 'varchar'


class Float(FieldBase):

    __slots__ = ('length', 'unsigned',)

    _py_type = float
    _db_type = 'float'
    _type_tpl = ('{type}', '{type}({m},{d})')

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
            if isinstance(length, (tuple, list)) and len(length) == 2:
                self.length = tuple(length)
            else:
                raise ValueError(f"Invalid `Float` length: {length}")
        self.unsigned = unsigned
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    @property
    def _unsigned(self):
        if self.unsigned:
            return 'unsigned'
        return None

    @property
    def _type(self):
        if isinstance(self.length, tuple):
            return self._type_tpl[1].format(
                type=self._db_type, m=self.length[0], d=self.length[1]
            )
        return self._type_tpl[0].format(type=self._db_type)


class Double(Float):

    __slots__ = ()

    _db_type = 'double'


class Decimal(Float):

    __slots__ = ('length', 'unsigned',)

    _py_type = decimal.Decimal
    _db_type = 'decimal'

    _type_tpl = '{type}({m},{d})'

    def __init__(self,
                 length,
                 unsigned=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        if not isinstance(length, (tuple, list)) or len(length) != 2:
            raise ValueError(f"Invalid `Float` length: {length}")
        self.length = tuple(length)
        self.unsigned = unsigned
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    @property
    def _type(self):
        return self._type_tpl.format(
            type=self._db_type, m=self.length[0], d=self.length[1]
        )


class Datetime(FieldBase):

    __slots__ = ()

    _py_type = datetime.datetime
    _db_type = 'datetime'
    _format = '%Y-%m-%d %H:%M:%S'

    _type_tpl = '{type}'

    def __init__(self,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        if not isinstance(default, self._py_type):
            raise ValueError(f"Invalid Datetime default: '{default}'")
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __call__(self, *args, **kwargs):
        return datetime.now()

    @property
    def type(self):
        return self._type_tpl.format(type=self._db_type)

    @property
    def _default(self):
        if self.default:
            return "DEFAULT '{}'".format(self.default.strftime(self._format))
        return 'DEFAULT NULL'


class Timestamp(Datetime):

    __slots__ = ()

    _db_type = 'timestamp'
    _auto = utils.Tdict(
        on_create="CURRENT_TIMESTAMP",
        on_update="CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    )

    def __init__(self,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        if isinstance(default, self._py_type) or default in self._auto:
            default = default
        else:
            raise ValueError(
                f"Invalid `Timestamp` default: {default}"
            )
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    @property
    def _default(self):
        if self.default in self._auto:
            return f'DEFAULT {self._auto[self.default]}'
        elif self.default:
            return "DEFAULT '{}'".format(self.default.strftime(self._format))
        return 'DEFAULT NULL'


class Func:

    def __init__(self, field):
        if isinstance(field, Expr):
            pass
        elif isinstance(field, Column):
            pass

    @utils.argschecker(alias=str, nullable=False)
    def as_(self, alias):
        return _Alias(self, alias)

    @property
    def sname(self):
        pass

    @property
    def sql(self):
        pass


class _Asc:

    def __init__(self, field):
        self.f = field

    @property
    def sname(self):
        return f"{self.f.sname} ASC"


class _Desc:

    def __init__(self, field):
        self.f = field

    @property
    def sname(self):
        return f"{self.f.sname} DESC"


class _Alias:

    def __init__(self, field, alias):
        self.f = field
        self.alias = alias

    @property
    def sname(self, alias):
        return f"{self.f.sname} AS {self.alias}"
