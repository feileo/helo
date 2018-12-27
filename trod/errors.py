
class InvaildDBUrlError(RuntimeError):
    pass


class DuplicateBindError(RuntimeError):
    pass


class NoBindError(RuntimeError):
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
