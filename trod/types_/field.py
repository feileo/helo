from collections.abc import Iterable

from trod.utils import TrodDict


OPER = TrodDict(
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
    BETWEEN='BETWEEN',
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
        self.lhs = lhs.name if isinstance(lhs, Column) else lhs
        self.op = op
        self.rhs = rhs.name if isinstance(rhs, Column) else rhs
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

    def sql(self):
        return self.__sql__()


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

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.start is None or item.stop is None:
                raise ValueError(
                    'BETWEEN range must have both a start and end-point.'
                )
            return self.between(item.start, item.stop)
        return self == item


class FieldBase(Column):

    __slots__ = ('null', 'default', 'comment',)

    _py_type = None
    _db_type = None

    def __init__(self, name, null, default, comment):

        self.null = null
        self.default = default
        self.comment = comment
        super().__init__(name)

    def build_type(self):
        """ Build field type """

        raise NotImplementedError

    def build_stmt(self):
        """ Build field definition """

        stmt = []

        if self.allow_null is True or self.allow_null == 1:
            stmt.append('NULL')
        elif self.allow_null is False or self.allow_null == 0:
            stmt.append('NOT NULL')
        else:
            raise ValueError('Allow_null value must be True, False, 0 or 1')
        if self.default is not None:
            default = self.default
            if isinstance(self, Float):
                default = float(default)
            if not isinstance(default, self._py_type):
                raise ValueError(
                    f'Except default value {self._py_type} now is {default}'
                )
            if isinstance(default, str):
                default = f"'{self.default}'"
            stmt.append(f'DEFAULT {default}')
        elif not self.allow_null:
            Logger.warning(f'Not to give default value for NOT NULL field {self.name}')
        stmt.append(f"COMMENT '{self.comment}'")
        return stmt

    def build(self):
        """ Generate field definition syntax """

        field_sql = [self.build_type()]
        field_sql.extend(self.build_stmt())
        return ' '.join(field_sql)
