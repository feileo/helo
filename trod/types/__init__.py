"""
    trod.types
    ~~~~~~~~~~
"""

from ._impl import (
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Bool,
    Auto,
    BigAuto,
    UUID,
    Float,
    Double,
    Decimal,
    Text,
    Char,
    VarChar,
    Date,
    Time,
    DateTime,
    Timestamp,
    Key as K,
    UKey as UK,
    FS as F,
    ENCODING,
    SEQUENCE,
    ON_CREATE,
    ON_UPDATE,
)
from .._helper import SQL


__all__ = (
    "Tinyint",
    "Smallint",
    "Int",
    "Bigint",
    "Bool",
    "Auto",
    "BigAuto",
    "UUID",
    "Float",
    "Double",
    "Decimal",
    "Text",
    "Char",
    "VarChar",
    "Date",
    "Time",
    "DateTime",
    "Timestamp",
    "K",
    "UK",
    "F",
    "SQL",
    "ENCODING",
    "SEQUENCE",
    "ON_CREATE",
    "ON_UPDATE",
)
