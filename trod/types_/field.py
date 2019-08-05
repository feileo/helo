import datetime
import decimal
import warnings
from collections.abc import Iterable

from trod import errors, db_ as db, utils


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
        return Expression(self, self.OPERATOR.ILIKE, f'%{rhs}%')

    def startswith(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, f'{rhs}%')

    def endswith(self, rhs):
        return Expression(self, self.OPERATOR.ILIKE, f'%{rhs}')

    def between(self, low, hig):
        return Expression(self, self.OPERATOR.BETWEEN, NodeList([low, self.OPERATOR.AND, hig]))

    def nbetween(self, low, hig):
        return Expression(self, self.OPERATOR.NBETWEEN, NodeList([low, self.OPERATOR.AND, hig]))

    def desc(self):
        return _Desc(self)

    def asc(self):
        return _Asc()

    @utils.argschecker(alias=str, nullable=False)
    def as_(self, alias):
        return _Alias(self, alias)


class SQL:

    __slots__ = ('_sql', '_args')

    def __init__(self, sql, args=None):
        self._sql = sql
        self._args = args

    @property
    def sql(self):
        return self._sql


class NodeList:

    __slots__ = ('nodes', 'glue', 'parens')

    @utils.argschecker(nodes=list)
    def __init__(self, nodes=None, glue=' ', parens=False):
        self.nodes = nodes or []
        self.glue = glue
        self.parens = parens

    def append(self, nodes):
        if isinstance(nodes, list):
            self.nodes.extend(nodes)
        else:
            self.nodes.append(nodes)
        return self

    def pop(self):
        self.nodes.pop()
        return self

    @property
    def sql(self):
        return self.glue.join(
            [n.sql if isinstance(n, SQL) else n for n in self.nodes]
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


class Expression(Column):

    __slots__ = ('lhs', 'op', 'rhs', 'flat', 'logic')

    def __init__(self, lhs, op, rhs, flat=True, logic=False):

        self.lhs = self.adapt(lhs, _l=True)
        self.op = op
        self.rhs = self.adapt(rhs)
        self.flat = flat
        self.logic = logic

    # def __and__(self, expr):
    #     if isinstance(expr, self.__class__):
    #         return self.__class__(self.__sql__(), self.OPERATOR.AND, expr.__sql__(), logic=True)
    #     raise ValueError()

    # def __or__(self, expr):
    #     if isinstance(expr, self.__class__):
    #         return self.__class__(self.__sql__(), self.OPERATOR.OR, expr.__sql__(), logic=True)
    #     raise ValueError()

    def adapt(self, hs, _l=False):

        if isinstance(hs, Column):
            hs = hs.name
        elif isinstance(hs, db.Doer):
            if _l:
                raise RuntimeError()
            hs = f'({hs.sql})'
        elif isinstance(hs, self.__class__):
            hs = hs.sql
        return hs

    @property
    def sql(self):
        if self.op in (self.OPERATOR.IN, self.OPERATOR.NOT_IN, self.OPERATOR.EXISTS):
            if not isinstance(self.rhs, Iterable):
                raise TypeError(
                    f"The value of the operator {self.rhs} should be an Iterable object")
            self.rhs = tuple(self.rhs)
        return NodeList([self.lhs, self.op, self.rhs], parens=True).sql


class FieldDef:

    __slots__ = ('defsql',)

    types = utils.Tdict(
        t='{type}',
        lt='{type}({length})',
        mdt='{type}({length},{float_length})',
    )

    def __init__(self, field):

        self.parse_name(field)
        ops = self.parse_field(field)

        defsql = NodeList([SQL(ops.name), SQL(ops.type_)])
        if ops.unsigned:
            defsql.append(SQL('unsigned'))
        if ops.encoding:
            defsql.append(SQL(f'CHARACTER SET {ops.encoding}'))
        if ops.zerofill:
            defsql.append(SQL('zerofill'))
        defsql.append(SQL('NULL') if ops.allow_null else SQL('NOT NULL'))
        if ops.default:
            defsql.append(self.parse_default(ops.ai, ops.default, ops.adapt))
        if ops.comment:
            defsql.append(SQL(f"COMMENT '{ops.comment}'"))

        self.defsql = defsql.sql

    def parse_name(self, field):

        def sname(self):
            return f"`{self.name}`"

        field.__class__.sname = property(sname)
        return field

    def parse_field(self, field):
        return utils.Tdict(
            name=field.sname,
            type_=self.parse_type(field),
            ai=getattr(field, 'ai', None),
            unsigned=getattr(field, 'unsigned', None),
            zerofill=getattr(field, 'zerofill', None),
            encoding=getattr(field, 'encoding', None),
            allow_null=field.null,
            default=field.default,
            comment=field.comment,
            adapt=field.db_value
        )

    def parse_type(self, field):
        type_render = {'type': field.db_type}

        type_tpl = self.types.t
        length = getattr(field, 'length', None)

        if length:
            type_render['length'] = length
            type_tpl = self.types.lt
        if isinstance(length, tuple) and len(length) == 2:
            float_length = length[1]
            if float_length:
                type_render['float_length'] = float_length
                type_tpl = self.types.mdt

        return SQL(type_tpl.format(**type_render))

    def parse_default(self, ai, default, adapt):

        if ai:
            return 'AUTO_INCREMENT'
        if default:
            default = default if not callable(default) else default()
            if isinstance(default, SQL):
                default = f"DEFAULT {default.sql}"
            else:
                default = adapt(default)
                default = f"DEFAULT '{default}'"
        else:
            default = 'DEFAULT NULL'
        return SQL(default)


class FieldBase(Column):

    __slots__ = ('null', 'default', 'comment', '_seqnum', 'name')

    py_type = None
    db_type = None

    _field_counter = 0

    @utils.argschecker(null=bool, comment=str)
    def __init__(self, null, default, comment, name=None):
        """
        :params default str|callbale object
        """

        if not isinstance(default, (self.py_type, SQL)) and not callable(default):
            raise TypeError(
                f"Invalid {self.__class__.__name__} default value ({default}),\
                must be '{self.py_type}', 'SQL' object or callable object.")

        self.null = null
        self.default = default
        self.comment = comment
        self.name = name

        self._default_wain()
        FieldBase._field_counter += 1
        self._seqnum = FieldBase._field_counter
        super().__init__()

    @property
    def __def__(self):
        return FieldDef(self).defsql

    def _default_wain(self,):
        if not self.default and not self.null:
            warnings.warn(
                f'Not to give default value for NOT NULL field {self.__class__.__name__}')

    @property
    def seqnum(self):
        return self._seqnum

    def py_value(self, value):
        raise NotImplementedError()

    def db_value(self, value):
        raise NotImplementedError()


class Tinyint(FieldBase):

    __slots__ = ('unsigned', 'length', 'zerofill')

    py_type = int
    db_type = 'tinyint'

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

    def adapt(self, value):
        pass

    def py_value(self, value):
        return self.py_type(value) if value is not None else None

    def db_value(self, value):
        return self.py_type(value) if value is not None else None


class Smallint(Tinyint):

    __slots__ = ()

    db_type = 'smallint'


class Int(Tinyint):

    __slots__ = ('pk', 'ai',)

    db_type = 'int'

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
                    'Primary key is not allow null, use default(null=False, default=None)',
                    errors.ProgrammingWarning
                )
            null = False
            default = None
        elif self.ai:
            warnings.warn(
                "'AUTO_INCREMENT' cannot be set for non-primary key fields, \
                use default(ai=False)",
                errors.ProgrammingWarning
            )
            self.ai = False

        super().__init__(
            name=name, null=null, default=default, comment=comment,
            length=length, unsigned=unsigned, zerofill=zerofill
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
        self.encoding = encoding
        super().__init__(
            name=name, null=null, default=None, comment=comment
        )

    def _default_wain(self):
        pass

    def py_value(self, value):
        pass

    def db_value(self, value):
        pass


class Char(FieldBase):

    __slots__ = ('length', 'encoding',)

    py_type = str
    db_type = 'char'

    __type__ = '{type}({length})'

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

    def py_value(self, value):
        pass

    def db_value(self, value):
        pass


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
            if isinstance(length, (tuple, list)) and len(length) == 2:
                self.length = tuple(length)
            else:
                raise TypeError(f"Invalid `Float` length type({length})")
        self.unsigned = unsigned
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def py_value(self, value):
        pass

    def db_value(self, value):
        pass


class Double(Float):

    __slots__ = ()

    db_type = 'double'


class Decimal(Float):

    __slots__ = ('length', 'unsigned',)

    py_type = decimal.Decimal
    db_type = 'decimal'

    def __init__(self,
                 length,
                 unsigned=False,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        if not isinstance(length, (tuple, list)) or len(length) != 2:
            raise TypeError(f"`Decimal` length type must be list or tuple")
        self.length = tuple(length)
        self.unsigned = unsigned
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )


class Datetime(FieldBase):

    __slots__ = ()

    py_type = datetime.datetime
    db_type = 'datetime'

    format_ = '%Y-%m-%d %H:%M:%S'

    def __init__(self,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )

    def __call__(self, *args, **kwargs):
        return datetime.datetime.now()

    def py_value(self, value):
        return self.default.strftime(self.format_)

    def db_value(self, value):
        pass


ON_CREATE = SQL("CURRENT_TIMESTAMP")
ON_UPDATE = SQL("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")


class Timestamp(Datetime):

    __slots__ = ()

    db_type = 'timestamp'

    def __init__(self,
                 null=True,
                 default=None,
                 comment='',
                 name=None):
        # if not isinstance(default, self.py_type, SQL)
        #     default = default
        # else:
        #     raise ValueError(
        #         f"Invalid `Timestamp` default: {default}"
        #     )
        super().__init__(
            null=null, default=default, comment=comment, name=name
        )


class Func:

    def __init__(self, field):
        if isinstance(field, Expression):
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
