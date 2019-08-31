"""
    trod.types
    ~~~~~~~~~~
"""

from trod.types import __impl__

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
    "Key",
    "UKey",
    "FS",
    "SQL",
    "ON_CREATE",
    "ON_UPDATE",
    "SEQUENCE",
    "ENCODINGS",
)


Tinyint = __impl__.Tinyint
Smallint = __impl__.Smallint
Int = __impl__.Int
Bigint = __impl__.Bigint
Bool = __impl__.Bool
Auto = __impl__.Auto
BigAuto = __impl__.Bigint
UUID = __impl__.UUID
Float = __impl__.Float
Double = __impl__.Double
Decimal = __impl__.Decimal
Text = __impl__.Text
Char = __impl__.Char
VarChar = __impl__.VarChar
Date = __impl__.Date
Time = __impl__.Time
DateTime = __impl__.DateTime
Timestamp = __impl__.Timestamp
FS = __impl__.Funcs
Key = __impl__.Key
UKey = __impl__.UKey
SQL = __impl__.SQL
ON_CREATE = __impl__.ON_CREATE
ON_UPDATE = __impl__.ON_UPDATE
SEQUENCE = __impl__.SEQUENCE
ENCODINGS = __impl__.ENCODINGS
