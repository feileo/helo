from abc import ABC


class IndexBase(ABC):

    __slots__ = ('name', 'fields', 'comment', '_seq_num', )

    _type_tpl = None

    _field_counter = 0

    def __init__(self, name, fields, comment=''):
        self.name = name
        self.comment = comment or f"{self.__class__.__name__} {self.name}"
        if isinstance(fields, (list, tuple)):
            self.fields = fields
        else:
            self.fields = [fields]
        self.fields = [f'`{c}`' for c in self.fields]

        IndexBase._field_counter += 1
        self._seq_num = IndexBase._field_counter

    def __str__(self):
        return '<{0} ({1}: {2})>'.format(
            self.__class__.__name__, self.name, self.fields
        )
    __repr__ = __str__

    @property
    def sql(self):

        return self._type_tpl.format(
            self.name,
            ', '.join(self.fields),
            self.comment
        )


class Key(IndexBase):

    _type_tpl = "KEY `{0}` ({1}) COMMENT '{2}'"


class UKey(IndexBase):

    _type_tpl = "UNIQUE KEY `{0}` ({1}) COMMENT '{2}'"
