"""
    trod.types
    ~~~~~~~~~~
"""

from .types import (
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Text,
    Char,
    VarChar,
    Float,
    Double,
    Decimal,
    Bool,
    Auto,
    BigAuto,
    UUID,
    Date,
    Time,
    DateTime,
    Timestamp,
    Func,
    ON_CREATE,
    ON_UPDATE,
    SQL,
    Key,
    UKey
)

__all__ = (
    # Fields
    "Tinyint",
    "Smallint",
    "Int",
    "Bigint",
    "Text",
    "Char",
    "VarChar",
    "Float",
    "Double",
    "Decimal",
    "Bool",
    "Auto",
    "BigAuto",
    "UUID",
    "Date",
    "Time",
    "DateTime",
    "Timestamp",
    # Indexs
    "Key",
    "UKey",

    "ON_CREATE",
    "ON_UPDATE",
    "SQL",
    "Func",
)
