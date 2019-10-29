"""
    trod.g
    ~~~~~~

    Implements some global objects.
"""

from ._helper import Query, SQL, Value, parse, and_, or_
from ..util import tdict

__all__ = (
    "and_",
    "or_",
    "parse",
    "Query",
    "SQL",
    "Value",
    "SEQUENCE",
    "RT",
    "ENCODINGS",
    "ON_CREATE",
    "ON_UPDATE",
)


SEQUENCE = (list, tuple, set, frozenset)
RT = tdict(
    MODEL=1,
    TDICT=2,
    TUPLE=3,
)
ENCODINGS = tdict(
    utf8="utf8",
    utf16="utf16",
    utf32="utf32",
    utf8mb4="utf8mb4",
    gbk="gbk",
    gb2312="gb2312",
)
ON_CREATE = SQL("CURRENT_TIMESTAMP")
ON_UPDATE = SQL("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")


del tdict
