"""
    trod.g._helper
    ~~~~~~~~~~~~~~

    Implements query completion base.
"""

from __future__ import annotations

from typing import Any, Optional, List, Union

from .util import argschecker


class Query:

    __slots__ = ('_sql', '_params', '_read')

    _KEYS = ("SELECT", "SHOW")

    @argschecker(sql=str, params=(list, tuple))
    def __init__(
        self,
        sql: str,
        params: Optional[list] = None,
        read: Optional[bool] = None
    ) -> None:
        self._sql = sql
        self._params = params or []
        self._read = read

    def __repr__(self) -> str:
        return f"Query({self.sql} % {self.params})"

    def __str__(self) -> str:
        return f"{self.sql} % {self.params}"

    def __bool__(self) -> bool:
        return bool(self._sql)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Query):
            raise TypeError(
                f"Unsupported operation: 'Query' == {other}"
            )
        return self.sql == other.sql and self.params == other.params

    @property
    def sql(self) -> str:
        return self._sql.strip()

    @property
    def params(self) -> tuple:
        return tuple(self._params)

    @property
    def r(self) -> bool:
        if self._read is not None:
            return self._read

        for k in self._KEYS:
            if k in self.sql or k.lower() in self.sql:
                return True
        return False

    @r.setter
    def r(self, _is) -> None:
        if not isinstance(_is, bool):
            raise TypeError()
        self._read = _is


class Node:

    __slots__ = ()

    def __sql__(self, ctx: Context):
        raise NotImplementedError


class NodeList(Node):

    __slots__ = ('nodes', 'glue', 'parens')

    def __init__(
        self, nodes: List[Node], glue: str = ' ', parens: bool = False
    ) -> None:
        self.nodes = nodes
        self.glue = glue
        self.parens = parens
        if parens and len(self.nodes) == 1:
            if hasattr(self.nodes[0], 'parens'):
                self.nodes[0].parens = False  # type: ignore

    def append(self, node: Union[Node, List[Node]]) -> NodeList:
        if isinstance(node, list):
            self.nodes.extend(node)
        else:
            self.nodes.append(node)
        return self

    def __sql__(self, ctx: Context) -> Context:
        nlen = len(self.nodes)
        if nlen == 0:
            return ctx.literal('()') if self.parens else ctx

        def paser(ctx, node):
            if isinstance(node, Node):
                ctx.sql(node)
            else:
                ctx.literal(node)  # type: ignore

        with ctx(parens=self.parens):
            for i in range(nlen - 1):
                paser(ctx, self.nodes[i])
                ctx.literal(self.glue)

            paser(ctx, self.nodes[-1])

        return ctx


def CommaNodeList(  # pylint: disable=invalid-name
    nodes: List[Node]
) -> NodeList:
    return NodeList(nodes, glue=', ')


def EnclosedNodeList(  # pylint: disable=invalid-name
    nodes: List[Node]
) -> NodeList:
    return NodeList(nodes, glue=', ', parens=True)


class SQL(Node):

    __slots__ = ('sql', 'params')

    def __init__(self, sql, params=None):
        self.sql = sql
        self.params = params

    def __repr__(self):
        if self.params:
            return f"SQL({self.sql} % {self.params})"
        return f"SQL({self.sql})"

    def __sql__(self, ctx):
        ctx.literal(self.sql)
        if self.params:
            for param in self.params:
                ctx.value(param, False, add_param=False)
        return ctx


class Value(Node):

    __slots__ = ('_value',)

    def __init__(self, _value):
        self._value = _value

    @property
    def v(self):
        return self._value

    def __sql__(self, ctx):
        ctx.literal('%s').values(self.v)
        return ctx


class Context:

    __slots__ = ('_sql', '_values', 'stack', 'state')
    _multi_types = (tuple, list)
    _semi = ';'

    def __init__(self, **settings: Any) -> None:
        self._sql = []  # type: List[str]
        self._values = []  # type: List[Any]
        self.stack = []  # type:List[dict]
        self.state = settings

    def sql(self, obj) -> Context:
        if isinstance(obj, (Node, Context)):
            return obj.__sql__(self)

        return self.values(obj)

    def __sql__(self, ctx) -> Context:
        ctx._sql.extend(self._sql)
        ctx.values(self._values)
        return ctx

    @property
    def parens(self) -> Optional[bool]:
        return self.state.get('parens')

    def literal(self, kwd: str) -> Context:
        self._sql.append(kwd)
        return self

    def __enter__(self) -> Context:
        if self.parens:
            self.literal('(')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.parens:
            self.literal(')')
        self.state = self.stack.pop()

    def __call__(self, **overrides: Any) -> Context:
        self.stack.append(self.state)
        self.state = overrides
        return self

    def values(self, value: Any) -> Context:

        converter = self.state.get('converter')
        if value is not None and converter:
            if isinstance(value, self._multi_types):
                value = tuple(map(converter, value))
            else:
                value = converter(value)

        if self.state.get('params'):
            self.literal('%s')

        if isinstance(value, self._multi_types):
            if not self.state.get('nesting'):
                self._values.extend(value)
                return self
        self._values.append(value)
        return self

    def parse(self, node: Any) -> Context:
        return self.sql(node)

    def query(self) -> Query:
        if self._sql[-1] != self._semi:
            self.literal(self._semi)
        return Query(''.join(self._sql), params=tuple(self._values))


def parse(node: Node) -> Query:
    return Context().parse(node).query()


def with_metaclass(meta, *bases):

    class MetaClass(type):

        def __new__(cls, name, _this_bases, attrs):
            return meta(name, bases, attrs)

    return type.__new__(MetaClass, 'temporary_class', (), {})
