"""
    trod.g
    ~~~~~~

    Implements some global objects.
"""

from . import _helper
from ..utils import Tdict, Tcontainer

__all__ = (
    "or_",
    "and_",
    "SQL",
    "ENCODINGS",
)
RT = Tdict(
    MODEL=1,
    TDICT=2,
    TUPLE=3,
)
SEQUENCE = (list, tuple, set, frozenset)
ENCODINGS = Tcontainer(
    utf8="utf8",
    utf16="utf16",
    utf32="utf32",
    utf8mb4="utf8mb4",
    gbk="gbk",
    gb2312="gb2312",
)
SQL = _helper.SQL
ON_CREATE = SQL("CURRENT_TIMESTAMP")
ON_UPDATE = SQL("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")


def and_(*exprs):
    return _helper.and_(*exprs)


def or_(*exprs):
    return _helper.or_(*exprs)


del Tcontainer, Tdict
