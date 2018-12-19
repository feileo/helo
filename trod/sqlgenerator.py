#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio


class BaseGenerator(object):
    def render(self):
        raise NotImplementedError


class WhereGenerator(BaseGenerator):

    _command_where_templates = {
        'eq': '{field}=%s',
        'lk': '{field} LIKE %s',
        'lt': '{field}<%s',
        'mt': '{field}>%s',
        'leq': '{field}<=%s',
        'meq': '{field}>=%s'
    }
    _method_sep = '__'

    def __init__(self, condiitons):
        self.conditions = condiitons

    def render(self):
        values = []
        condition_clauses = []
        for key, value in self.conditions.items():
            if key.find(self._method_sep) != -1:
                command, field = key.split(self._method_sep)
                field = field.join('``')
            else:
                command = 'eq'
                field = key.join('``')
            values.append(value)
            condition_clauses.append(self._command_where_templates[command].format(field=field))
        tmp_clause = ' AND '.join(condition_clauses)
        where_clause = 'WHERE ' + tmp_clause if tmp_clause else ''
        return where_clause, values


class OrderGenerator(BaseGenerator):
    _template = 'ORDER BY {field} {dir}'

    def __init__(self, field, desc=False):
        self.field = field
        self.desc = desc

    def render(self):
        return self._template.format(field=self.field.join('``'), dir='DESC' if self.desc is True else 'ASC')


class LimitGenerator(BaseGenerator):
    _template = 'Limit {start_num},{count}'

    def __init__(self, count, start_num=0):
        self.count = count
        self.start_num = start_num

    def render(self):
        return self._template.format(start_num=self.start_num, count=self.count)


class SQLGenerator(BaseGenerator):
    _sql_template = None
    _template_default = None

    def render(self):
        raise NotImplementedError


class SelectGenerator(SQLGenerator):
    _sql_template = 'SELECT {fields} FROM {table} {where_clause} {order_clause} {limit_clause}'
    _template_default = {
        'where_clause': '',
        'order_clause': '',
        'limit_clause': ''
    }

    def __init__(self, fields, model):
        self.model = model
        self.render_data = self._template_default.copy()
        self.values = []
        self.has_where = False
        self.has_order = False
        self.has_limit = False
        self.fields = fields
        self.render_data['fields'] = ','.join([each_field.join('``') for each_field in self.fields])
        self.render_data['table'] = model.__table__.join('``')

    def _set_where(self, where):
        if not self.has_where:
            where_obj = WhereGenerator(where)
            self.render_data['where_clause'], where_value = where_obj.render()
            self.values.extend(where_value)
            self.has_where = True

    def filter(self, **kwargs):
        self._set_where(kwargs)
        return self

    def _set_order(self, field, desc):
        if not self.has_order:
            order_obj = OrderGenerator(field, desc)
            self.render_data['order_clause'] = order_obj.render()
            self.has_order = True

    def order_by(self, field=None, desc=False):
        if field is not None and field not in self.model.__fields__:
            if field != self.model.__primary_key__:
                raise Exception('model %s don\'t have field %s' % (self.model.__name__, field))
        if field is None and self.model.__primary_key__ is None:
            raise Exception('must have a field to order by')
        if field is None:
            self._set_order(self.model.__primary_key__, desc)
        else:
            self._set_order(field, desc)
        return self

    def _set_limit(self, count, start_num):
        if not self.has_limit:
            limit_obj = LimitGenerator(count, start_num)
            self.render_data['limit_clause'] = limit_obj.render()
            self.has_limit = True

    def limit(self, count, start_num=0):
        self._set_limit(count, start_num)
        return self

    async def select(self, rows=None):
        execute_sql, values = self.render()
        result_list = await self.model.select(execute_sql, values, rows)
        tmp_obj = self.model()
        if len(result_list) > 1:
            all_objs = []
            for each_dict in result_list:
                for each_field in self.fields:
                    if each_field is not None:
                        tmp_obj.set_value(each_field, each_dict[each_field])
                all_objs.append(tmp_obj)
            return all_objs
        elif len(result_list) == 1:
            for each_field in self.fields:
                if each_field is not None:
                    tmp_obj.set_value(each_field, result_list[0][each_field])
            return tmp_obj
        else:
            return None

    def render(self):
        return self._sql_template.format(**self.render_data), self.values
