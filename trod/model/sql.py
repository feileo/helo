from trod.db import Transitioner
from trod.model import SQL
from trod.model.loader import Loader
from trod.types.index import Key, UniqueKey
from trod.utils import Dict, dict_formatter


class Where:
    """
    self.where=Dict(
        column='name',
        operator='==',
        value='hehe'
    )
    self.format_() ==> "name = %s", "hehe"
    """
    OPERATORS = {
        '=': ' = ',
        '==': ' = ',
        '!=': ' != ',
        '>': ' > ',
        '>=': ' >= ',
        '<': ' < ',
        '<=': ' <= ',
        'in': ' IN ',
        'IN': ' IN ',
        'like': ' LIKE ',
        'LIKE': ' LIKE '
    }

    def __init__(self, column, operator, value):
        if operator not in self.OPERATORS:
            raise TypeError(f'Does not support the {operator} operator')
        if isinstance(value, str):
            value = f"'{value}'"
        if operator in ['in', 'IN']:
            if not isinstance(value, (list, tuple)):
                raise TypeError(
                    f'The value of the operator {operator} should be list or tuple'
                )
            value = tuple(value)

        self.where = Dict(
            column=column,
            operator=self.OPERATORS[operator],
            value=value
        )

    def get(self):
        return self.where

    @dict_formatter
    def format_(self):
        return {
            'where': '{col}{ope}%s'.format(
                col=self.where.column, ope=self.where.operator
            ),
            'arg': '{}'.format(self.where.value),
            'col': self.where.column
        }


class _Logic:

    _LOGIC = ""

    def __init__(self, *where_objects):
        wheres, args, cols = [], [], []
        for where in where_objects:
            if isinstance(where, Where):
                _w_format = where.format_()
                wheres.append(_w_format.where)
            elif isinstance(where, _Logic):
                _w_format = where.format_()
                wheres.append("({})".format(_w_format.where))
            else:
                raise TypeError(f'{where}')
            if isinstance(_w_format.arg, list):
                args.extend(_w_format.arg)
                cols.extend(_w_format.cols)
            else:
                args.append(_w_format.arg)
                cols.append(_w_format.col)
            if isinstance(_w_format.cols, list):
                cols.extend(_w_format.cols)
            else:
                cols.append(_w_format.col)
        self._all_cdtns = wheres
        self._args = args
        self._cols = cols

    @dict_formatter
    def format_(self):
        return {
            'where': self._LOGIC.join(self._all_cdtns),
            'arg': self._args,
            'col': self._cols
        }


class And(_Logic):
    _LOGIC = " AND "


class Or(_Logic):
    _LOGIC = " OR "


class Generator:

    _sql_template = SQL.select.complete
    _render_data_default = Dict(
        where_clause='',
        group_by_clause='',
        order_by_clause='',
        limit_clause=''
    )

    def __init__(self, model, cols):
        self._model = model
        self._cols = cols
        self._values = None
        self._render_data = self._render_data_default.copy()

    def _render_query(self):

        if not self._cols:
            self._render_data.cols = ','.join([c.join('``') for c in self._cols])
        else:
            self._render_data.cols = '*'
        self._render_data.table_name = self._model.__meta__.table.join('``')

    def _render(self):
        self._render_query()
        return self._sql_template.format(**self._render_data)

    def filter(self, where):
        if not isinstance(where, (Where, _Logic)):
            raise TypeError('')
        where_format = where.format_()
        self._model.has_cols_checker(where_format.col)
        self._render_data.where_clause = "WHERE {}".format(where_format.where)
        self._values = where_format.arg
        return self

    def group_by(self, *cols):
        group_by_tpl = "GROUP BY {cols}"
        if not cols:
            raise TypeError('Group by no rag')
        self._model.has_cols_checker(cols)
        self._render_data.group_by_clause = group_by_tpl.format(
            cols=','.join([c.join('``') for c in cols])
        )
        return self

    def order_by(self, col=None, desc=False):
        order_by_tpl = "ORDER BY {col} {desc}"
        desc = 'DESC' if desc else 'ASC'
        if col is None:
            col = self._model.__table__.pk
        self._model.has_cols_checker(col)
        self._render_data.order_by_clause = order_by_tpl.format(
            col=col.join('``'), desc=desc
        )
        return self

    def rows(self, limit=1000, offset=0):
        limit_tpl = 'LIMIT {limit} OFFSET {offset}'
        self._render_data.limit_clause = limit_tpl.format(
            limit=limit, offset=offset
        )
        result = Transitioner(self._render(), args=self._values).fetch()
        return Loader(self._model, result).load()

    def first(self):
        limit = 'LIMIT 1'
        self._render_data.limit_clause = limit
        result = Transitioner(
            self._render(), args=self._values, rows=1
        ).fetch()
        return Loader(self._model, result).load()

    def all(self):
        result = Transitioner(self._render(), args=self._values).fetch()
        return Loader(self._model, result).load()

    @classmethod
    def alter(cls, new_dict, table_name, modify_list=None,
              add_list=None, drop_list=None):
        alter_clause = []
        if modify_list is not None:
            for col in modify_list:
                if col in new_dict.field_dict:
                    alter_clause.append(
                        "MODIFY COLUMN `{}` {}".format(col, new_dict.field_dict[col].build())
                    )
                else:
                    raise TypeError(f"Modify column/key '{col}' not found")
        if add_list is not None:
            for col in add_list:
                if col in new_dict.field_dict:
                    cols = list(new_dict.field_dict.keys())
                    col_seq = cols.index(col)
                    if col_seq == 0:
                        alter_clause.append(
                            "ADD COLUMN `{}` {}".format(
                                col, new_dict.field_dict[col].build()
                            )
                        )
                    else:
                        alter_clause.append(
                            "ADD COLUMN `{}` {} AFTER `{}`".format(
                                col, new_dict.field_dict[col].build(), cols[col_seq-1]
                            )
                        )
                elif col in new_dict.index_dict:
                    if isinstance(new_dict.index_dict[col], Key):
                        alter_clause.append(
                            "ADD KEY `{}` {}".format(col, new_dict.field_dict[col].build())
                        )
                    elif isinstance(new_dict.index_dict[col], UniqueKey):
                        alter_clause.append(
                            "ADD UNIQUE KEY `{}` {}".format(col, new_dict.field_dict[col].build())
                        )
                    else:
                        raise TypeError('Add key type illegal')
                else:
                    raise TypeError(f"Add column/key '{col}' not found")
        if drop_list is not None:
            for col in drop_list:
                alter_clause.append(f"DROP COLUMN `{col}`")

        return SQL.alter.format(table_name=table_name, clause=', '.join(alter_clause))
