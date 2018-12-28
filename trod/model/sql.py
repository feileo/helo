from trod.db.executer import RequestClient
from trod.model.loader import Loader
from trod.types.index import Key, UniqueKey
from trod.utils import Dict, dict_formatter, _do_format


SQL = Dict(
    create="CREATE TABLE `{tn}` ({cd}) ENGINE={eg} DEFAULT CHARSET={cs} COMMENT='{cm}';",
    drop="DROP TABLE `{table_name}`;",
    show=Dict(
        tables='SHOW TABLES',
        status='SHOW TABLE STATUS',
        create="SHOW CREATE TABLE `{table_name}`;",
        columns="SHOW FULL COLUMNS FROM `{table_name}`;",
        indexs="SHOW INDEX FROM `{table_name}`;"
    ),
    exist="SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}' AND table_name='{table}'",
    alter="ALTER TABLE `{table_name}` {clause};",
    insert="INSERT INTO `{table_name}` ({cols}) VALUES ({values});",
    delete="DELETE FROM `{table_name}` WHERE {condition};",
    update_=Dict(
        complete="UPDATE `{table_name}` SET {kv} WHERE {condition};",
        no_where="UPDATE `{table_name}` SET {kv}"
    ),
    select=Dict(
        complete="SELECT {cols} FROM `{table_name}` {where_clause} {group_clause} {order_clause} {limit_clause}",
        by_id="SELECT {cols} FROM `{table_name}` WHERE `{condition}`=%s;",
        by_ids="SELECT {cols} FROM `{table_name}` WHERE `{condition}` IN {data};",
    )
)


class _Where:

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
            raise ValueError(f'Does not support the {operator} operator')
        if isinstance(value, str):
            value = f"'{value}'"
        if operator in ['in', 'IN']:
            if not isinstance(value, (list, tuple)):
                raise ValueError(
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

    def format_(self):

        if self.where.operator == self.OPERATORS['in']:
            _where_stmt = '{col}{ope}{values}'.format(
                col=self.where.column, ope=self.where.operator,
                values=self.where.value
            )
            _arg = None
        else:
            _where_stmt = '{col}{ope}%s'.format(
                col=self.where.column, ope=self.where.operator
            )
            _arg = '{}'.format(self.where.value)

        return Dict(
            where=_where_stmt,
            arg=_arg,
            col=self.where.column
        )


class _Logic:

    _LOGIC = ""

    def __init__(self, *where_objects):

        wheres, args, cols = [], [], []
        for where in where_objects:
            if isinstance(where, _Where):
                _w_format = where.format_()
                wheres.append(_w_format.where)
            elif isinstance(where, _Logic):
                _w_format = where.format_()
                wheres.append("({})".format(_w_format.where))
            else:
                raise ValueError(f'Invalid logic operator')
            if isinstance(_w_format.arg, list):
                args.extend(_w_format.arg)
            elif _w_format.arg:
                args.append(_w_format.arg)
            if isinstance(_w_format.col, list):
                cols.extend(_w_format.col)
            else:
                cols.append(_w_format.col)
        self._all_cdtns = wheres
        self._args = args
        self._cols = cols

    def format_(self):

        return Dict(
            where=self._LOGIC.join(self._all_cdtns),
            arg=self._args,
            col=self._cols
        )


class _Generator:

    _sql_template = SQL.select.complete
    _render_data_default = Dict(
        where_clause='',
        group_clause='',
        order_clause='',
        limit_clause=''
    )

    @property
    @dict_formatter
    def _tpl(self):
        return self._render_data_default.copy()

    def __init__(self, model, cols):
        self._model = model
        self._cols = cols
        self._values = None
        self._has_func = False
        self._has_col = False
        self._render_data = self._tpl

    def _render_query(self):
        if not self._cols:
            raise ValueError('No column given the query')
        if not isinstance(self._cols, list):
            raise ValueError(
                f'The query column must be a list or tuple, now is {self._cols}'
            )
        cols, func_cols = [], []
        for _c in self._cols:
            if isinstance(_c, Func):
                func_cols.append(_c.func)
                self._has_func = True
            else:
                cols.append('`{}`'.format(_c))
                self._has_col = True
        cols.extend(func_cols)
        self._render_data.cols = ','.join(cols)
        self._render_data.table_name = self._model.__meta__.table

    def _render(self):
        self._render_query()
        return self._sql_template.format(**self._render_data)

    def filter(self, where):
        """ Query condition filter """

        if not isinstance(where, (_Where, _Logic)):
            raise ValueError('Invalid filter condition')
        where_format = where.format_()
        self._model._has_cols_checker(where_format.col)
        self._render_data.where_clause = "WHERE {}".format(where_format.where)
        self._values = where_format.arg
        return self

    def group_by(self, *cols):
        """ Generate group by clause  """

        group_by_tpl = "GROUP BY {cols}"
        if not cols:
            raise ValueError("Group by can't have no field")
        col_names = []
        for _c in cols:
            col_names.append(_get_col_type_name(_c))
        self._model._has_cols_checker(col_names)
        self._render_data.group_clause = group_by_tpl.format(
            cols=','.join([c.join('``') for c in col_names])
        )
        return self

    def order_by(self, col=None, desc=False):
        """ Generate order by clause  """

        order_by_tpl = "ORDER BY {col} {desc}"
        desc = 'DESC' if desc else 'ASC'
        if col is None:
            col = self._model.__table__.pk
        col = _get_col_type_name(col)
        self._model._has_cols_checker(col)
        self._render_data.order_clause = order_by_tpl.format(
            col=col.join('``'), desc=desc
        )
        return self

    async def rows(self, limit=1000, offset=0):
        """ Generate limit clause  """

        limit_tpl = 'LIMIT {limit} OFFSET {offset}'
        self._render_data.limit_clause = limit_tpl.format(
            limit=limit, offset=offset
        )
        result = await RequestClient().fetch(self._render(), args=self._values)
        if self._has_func:
            return _do_format(result)
        return Loader(self._model, result).load()

    async def first(self):
        """ Select first  """

        limit = 'LIMIT 1'
        self._render_data.limit_clause = limit
        result = await RequestClient().fetch(
            self._render(), args=self._values, rows=1
        )
        if self._has_func:
            return _do_format(result)
        return Loader(self._model, result).load()

    async def all(self):
        """ Select all  """

        result = await RequestClient().fetch(self._render(), args=self._values)
        if self._has_func:
            return _do_format(result)
        return Loader(self._model, result).load()

    async def scalar(self):
        """ return a count  """

        if self._has_col is True:
            raise RuntimeError('Invalid call, Maybe you can try to call first()')
        result = await RequestClient().fetch(
            self._render(), args=self._values, rows=1
        )
        if len(result) == 1:
            result = list(result.values())[0]
        else:
            result = _do_format(result)
        return result

    @classmethod
    def alter(cls, new_dict, table_name, modify_list=None,
              add_list=None, drop_list=None):
        """ Table alter syntax generator """

        alter_clause = []
        if modify_list is not None:
            for col in modify_list:
                col = _get_col_type_name(col)
                if col in list(new_dict.field_dict.keys()):
                    alter_clause.append(
                        "MODIFY COLUMN `{}` {}".format(col, new_dict.field_dict[col].build())
                    )
                else:
                    raise RuntimeError(f"Modify column/key '{col}' not found")
        if add_list is not None:
            for col in add_list:
                col = _get_col_type_name(col)
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
                        raise ValueError("Add an invalid 'Key' type")
                else:
                    raise RuntimeError(f"Add column/key '{col}' not found")
        if drop_list is not None:
            for col in drop_list:
                col = _get_col_type_name(col)
                alter_clause.append(f"DROP COLUMN `{col}`")

        return SQL.alter.format(table_name=table_name, clause=', '.join(alter_clause))


class And(_Logic):
    """
    Model query by:
        And(User.name == user.name, User.age < user.age)
    """

    _LOGIC = " AND "


class Or(_Logic):
    """
    Model query by:
        Or(User.age == user.age, User.id.in_([1,2,3]))
    """

    _LOGIC = " OR "


class Func:
    """ Basic mysql function  """

    def __init__(self, func=None):
        self.func = func

    @classmethod
    def sum(cls, col):
        func = 'SUM({}) AS sum'.format(_get_col_type_name(col))
        return cls(func=func)

    @classmethod
    def max(cls, col):
        func = 'MAX({}) AS max'.format(_get_col_type_name(col))
        return cls(func=func)

    @classmethod
    def min(cls, col):
        func = 'MIN({}) AS min'.format(_get_col_type_name(col))
        return cls(func=func)

    @classmethod
    def avg(cls, col):
        func = 'AVG({}) AS avg'.format(_get_col_type_name(col))
        return cls(func=func)

    @classmethod
    def count(cls, col=None):
        func_tpl = 'COUNT({}) AS count'
        if col:
            name = _get_col_type_name(col)
        else:
            name = 1
        return cls(func=func_tpl.format(name))


def _get_col_type_name(col):
    if hasattr(col, 'name') and col.name:
        name = col.name
    else:
        name = col
    return name
