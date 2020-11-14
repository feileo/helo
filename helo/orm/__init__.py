"""
    helo.orm
    ~~~~~~~~
"""

from . import api, typ

__all__ = (
    "Model",  # pylint: disable=undefined-all-variable
    "ModelType",
    "JOINTYPE",
    "ROWTYPE",
)

JOINTYPE = typ.JOINTYPE
ROWTYPE = typ.ROWTYPE
ModelType = typ.ModelType


def __getattr__(name: str) -> typ.ModelType:
    if name == typ.BUILTIN_MODEL_NAME:
        return typ.ModelType(name, (api.Model,), {})
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
