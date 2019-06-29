import traceback
from abc import ABC


class IndexBase(ABC):

    __slots__ = ('name', 'columns', 'comment', '_seq_num', )

    _type_tpl = None

    _field_counter = 0

    def __init__(self, columns, comment='', name=None):
        if name is None:
            (_, _, _, text) = traceback.extract_stack()[-2]
            self.name = text[:text.find('=')].strip()
        else:
            self.name = name

        self.comment = comment or f"{self.__class__.__name__} {self.name}"
        if isinstance(columns, (list, tuple)):
            self.columns = columns
        else:
            self.columns = [columns]
        self.columns = [f'`{c}`' for c in self.columns]

        IndexBase._field_counter += 1
        self._seq_num = IndexBase._field_counter

    def __str__(self):
        return '<{0} ({1}: {2})>'.format(
            self.__class__.__name__, self.name, self.columns
        )
    __repr__ = __str__

    def sql(self):

        return self._type_tpl.format(
            key_name=self.name,
            cols=','.join(self.columns),
            comment=self.comment
        )


class Key(IndexBase):

    _type_tpl = "KEY `{key_name}` ({cols}) COMMENT '{comment}'"


class UKey(IndexBase):

    _type_tpl = "UNIQUE KEY `{key_name}` ({cols}) COMMENT '{comment}'"
