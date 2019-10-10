"""
    trod.types
    ~~~~~~~~~~
"""

from . import _impl

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
)


Tinyint = _impl.Tinyint
Smallint = _impl.Smallint
Int = _impl.Int
Bigint = _impl.Bigint
Bool = _impl.Bool
Auto = _impl.Auto
BigAuto = _impl.Bigint
UUID = _impl.UUID
Float = _impl.Float
Double = _impl.Double
Decimal = _impl.Decimal
Text = _impl.Text
Char = _impl.Char
VarChar = _impl.VarChar
Date = _impl.Date
Time = _impl.Time
DateTime = _impl.DateTime
Timestamp = _impl.Timestamp
K = _impl.Key  # pylint: disable=invalid-name
UK = _impl.UKey
F = _impl.FS
