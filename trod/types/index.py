import traceback
from trod.utils import Tdict


class BaseIndex:
    """ Key types base """

    _type_sql_tpl = None
    _attr_key_num = 0

    def __init__(self, column, comment='', name=None, engine=None):
        if name is None:
            (_, _, _, text) = traceback.extract_stack()[-2]
            self.name = text[:text.find('=')].strip()
        else:
            self.name = name

        self.comment = comment
        if isinstance(column, list):
            self.column = column
        else:
            self.column = [column]
        self.column = [f'`{c}`' for c in self.column]

        self.options = Tdict(
            key_name=self.name,
            cols=','.join(self.column),
            comment=self.comment
        )
        self.is_modify = False

    def __str__(self):
        return '<{} ({}: {})>'.format(self.__class__.__name__, self.name, self.comment)

    def build(self):
        """ Generate key definition syntax """

        return self._type_sql_tpl.format(**self.options)

    def modify(self):
        """ Deprecated  """

        self.is_modify = True
        return self

    @property
    def _attr_key(self):
        """ Deprecated  """

        BaseIndex._attr_key_num += 1
        return BaseIndex._attr_key_num


class Key(BaseIndex):
    """ Mysql Key """

    _type_sql_tpl = "KEY `{key_name}` ({cols}) COMMENT '{comment}'"


class UniqueKey(BaseIndex):
    """ Mysql unique key """

    _type_sql_tpl = "UNIQUE KEY `{key_name}` ({cols}) COMMENT '{comment}'"
