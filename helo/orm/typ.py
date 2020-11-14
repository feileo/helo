from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Any, Dict, Tuple, NewType

from .. import err
from .. import _helper
from ..util import adict
from ..types import core as types

__all__ = (
    "ROWTYPE",
    "JOINTYPE",
    "ModelType",
    "Model",
)

ROWTYPE = adict(
    MODEL=1,
    ADICT=2,
)
JOINTYPE = adict(
    INNER="INNER",
    LEFT="LEFT",
    RIGHT="RIGHT",
)

BUILTIN_MODEL_NAME = "Model"


class ModelType(type):

    def __new__(
        cls, name: str, bases: Tuple[ModelType, ...], attrs: Dict[str, Any]
    ) -> ModelType:

        attrs["__table__"] = None
        if name != BUILTIN_MODEL_NAME:
            attrs = _build_model_attrs(name, bases, attrs)

        attrs["__rowtype__"] = ROWTYPE.MODEL
        return type.__new__(cls, name, bases, attrs)  # type: ignore

    def __getattr__(cls, name: str) -> Any:
        if cls.__table__ is not None:
            if name in cls.__table__.fields_dict:
                return cls.__table__.fields_dict[name]

        raise AttributeError(
            f"'ModelType' object has no attribute '{name}'"
        )

    def __repr__(cls) -> str:
        if cls.__name__ == BUILTIN_MODEL_NAME:
            return f"{__name__}.{cls.__name__}"
        return f"<Model: {cls.__name__}>"

    def __str__(cls) -> str:
        return cls.__name__

    def __hash__(cls) -> int:
        if cls.__table__:
            return hash(cls.__table__)
        return 0


Model = NewType(BUILTIN_MODEL_NAME, ModelType)  # type: ignore


def _build_model_attrs(
    name: str, bases: Tuple[ModelType, ...], attrs: Dict[str, Any]
) -> Dict[str, Any]:
    model_fields, model_attrs = {}, {}
    for attr in attrs.copy():
        field = attrs[attr]
        if isinstance(field, types.Field):
            field.name = field.name or attr
            model_fields[attr] = field
            model_attrs[field.name] = attr
            attrs.pop(attr)

    baseclass = bases[0] if bases else None
    if baseclass:
        base_table = deepcopy(baseclass.__table__)
        if base_table:
            base_table.fields_dict.update(model_fields)
            model_fields = base_table.fields_dict
            base_names = deepcopy(baseclass.__attrs__)
            base_names.update(model_attrs)
            model_attrs = base_names

    metaclass = attrs.get('Meta')
    if not metaclass:
        metaclass = getattr(baseclass, 'Meta', None)

    indexes = getattr(metaclass, 'indexes', [])
    if indexes and not isinstance(indexes, (tuple, list)):
        raise TypeError("indexes type must be `tuple` or `list`")
    for index in indexes:
        if not isinstance(index, types.Index):
            raise TypeError(f"invalid index type {type(index)}")

    primary = adict(auto=False, field=None, attr=None, begin=None)
    for attr_name, field in model_fields.items():
        if getattr(field, 'primary_key', None):
            if primary.field is not None:
                raise err.DuplicatePKError(
                    "duplicate primary key found for field "
                    f"{field.name}"
                )
            primary.field = field
            primary.attr = attr_name
            if getattr(field, "auto", False):
                primary.auto = True
                primary.begin = int(field.auto)
                if field.name != types.Table.PK:
                    warnings.warn(
                        "The field name of AUTO_INCREMENT "
                        "primary key is suggested to use "
                        f"`id` instead of {field.name}",
                        err.ProgrammingWarning)

    attrs["__attrs__"] = model_attrs
    attrs["__table__"] = types.Table(
        name=getattr(metaclass, 'name', _helper.snake_name(name)),
        fields_dict=model_fields,
        primary=primary,
        indexes=indexes,
        engine=getattr(metaclass, "engine", None),
        charset=getattr(metaclass, "charset", None),
        comment=getattr(metaclass, "comment", None),
    )

    return attrs
