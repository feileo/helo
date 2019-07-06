

class DuplicateBindError(RuntimeError):
    pass


class NoConnectorError(RuntimeError):
    pass


class NoExecuterError(RuntimeError):
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
    pass


class AddEmptyInstanceError(RuntimeError):
    pass


class ModelSetAttrError(AttributeError):
    pass


class ProgrammingWarning(RuntimeWarning):
    pass
