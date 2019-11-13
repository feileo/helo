"""
    trod.db
    ~~~~~~~
"""

from ._impl import (
    binding,
    execute,
    unbinding,
    select_db,
    is_bound,
    get_state,
    FetchResult,
    ExecResult,
    Binder,
    DefaultURL,
    SUPPORTED_SCHEMES,
)


__all__ = (
    'binding',
    'unbinding',
    'execute',
    'select_db',
    'is_bound',
    'get_state',
    'FetchResult',
    'ExecResult',
    'Binder',
    'DefaultURL',
    'SUPPORTED_SCHEMES',
)
