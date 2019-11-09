class Error(Exception):
    description = 'trod internal error'

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class ProgrammingError(Error):
    description = 'user programming error'


class DataError(Error):
    description = 'data error'


class DBError(Error):
    description = ''

# db


class UnboundError(ProgrammingError):
    description = 'db has no binding, maybe you should call `trod.bind()` before.'


class DuplicateBinding(ProgrammingError):
    description = 'db already bound to {host}:{port}'

    def __init__(self, msg=None, **kwargs):
        super().__init__(msg or self.description.format(**kwargs))


class NoColumnNameError(ProgrammingError):
    description = "Column name must be specified outside the model"

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class UnsupportedError(Error):
    pass


#


class InvalidColumnVlaueError(DataError):
    pass


class DuplicateFieldNameError(RuntimeError):
    pass


# pk
class DuplicatePKError(ProgrammingError):
    pass


class NoPKError(ProgrammingError):
    pass


class InvalidFieldType(ValueError):
    pass


class IllegalModelAttrAssigendError(RuntimeError):
    pass


class DeleteUnsavedError(RuntimeError):
    pass


class AddEmptyInstanceError(RuntimeError):
    pass


class NotAllowedError(ProgrammingError):
    pass


class ProgrammingWarning(RuntimeWarning):
    pass


class DangerousOperation(RuntimeError):
    pass
