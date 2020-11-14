from helo import _sql
from helo.types import core


class Key(core.Index):

    __slots__ = ()
    db_type = _sql.SQL("KEY")


class UKey(core.Index):

    __slots__ = ()
    db_type = _sql.SQL("UNIQUE KEY")
