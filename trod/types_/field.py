
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


class Expr:

    def __init__(self, lhs, op, rhs, flat=False):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.flat = flat

    def __sql__(self, ctx):
        pass


class NodeList:
    pass


class Column:

    __slots__ = ('name',)

    @staticmethod
    def _expr(op, inv=False):

        def wraper(self, rhs):
            if inv:
                return Expr(rhs, op, self.name)
            return Expr(self.name, op, rhs)
        return wraper

    def __init__(self, name):
        self.name = name

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
        return Expr(self.name, op, rhs)

    def __ne__(self, rhs):
        op = OPER.IS_NOT if rhs is None else OPER.NE
        return Expr(self.name, op, rhs)

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
        return Expr(self.name, op, None)

    def contains(self, rhs):
        return Expr(self.name, OPER.ILIKE, '%{}%'.format(rhs))

    def startswith(self, rhs):
        return Expr(self.name, OPER.ILIKE, '{}%'.format(rhs))

    def endswith(self, rhs):
        return Expr(self.name, OPER.ILIKE, '%{}'.format(rhs))

    def between(self, low, hig):
        return Expr(self.name, OPER.BETWEEN, NodeList((low, SQL('AND'), hig)))

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.start is None or item.stop is None:
                raise ValueError('BETWEEN range must have both a start- and '
                                 'end-point.')
            return self.between(item.start, item.stop)
        return self == item

    def distinct(self):
        return NodeList((SQL('DISTINCT'), self.name))

    def collate(self, collation):
        return NodeList((self.name, SQL('COLLATE %s' % collation)))
