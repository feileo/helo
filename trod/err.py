class Error(Exception):
    description = ''

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class ProgrammingError(Error):
    pass


# db
class UnboundError(ProgrammingError):
    description = 'db has no binding, maybe you should call `trod.bind()` before.'


class DuplicateBinding(ProgrammingError):
    description = 'db already bound to {host}:{port}'

    def __init__(self, msg=None, **kwargs):
        super().__init__(msg or self.description.format(**kwargs))


class UnsupportedError(Error):
    pass


#
class DataError(Error):
    description = ''


class DBError(Error):
    description = ''


class NoColumnNameError(RuntimeError):
    description = "Column name must be specified outside the model"

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class NoSuchColumnError(RuntimeError):
    pass


class SetNoAttrError(AttributeError):
    description = "{} object not allowed set attribute '{name}'"


class SetInvalidColumnsValueError(RuntimeError):
    pass


class InvalidColumnsVlaueError(RuntimeError):
    pass


class DuplicateFieldNameError(RuntimeError):
    pass


class DuplicatePKError(RuntimeError):
    pass


class NoPKError(RuntimeError):
    pass


class InvalidFieldType(ValueError):
    pass


class IllegalModelAttrAssigendError(RuntimeError):
    pass


class DeleteUnsavedError(RuntimeError):
    pass


class MissingPKError(RuntimeError):
    pass


class ModifyAutoPkError(RuntimeError):
    description = "AUTO_INCREMENT table not allowed modify primary name"


class AddEmptyInstanceError(RuntimeError):
    pass


class ModelSetAttrError(AttributeError):
    pass


class ProgrammingWarning(RuntimeWarning):
    pass
