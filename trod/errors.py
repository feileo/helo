
class UnboundError(RuntimeError):
    description = "Maybe you should call `trod.bind()` before."

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class DuplicateBinding(RuntimeError):
    description = "Already bound to {host}:{port}"

    def __init__(self, msg=None, **kwargs):
        super().__init__(msg or self.description.format(**kwargs))


class NoColumnNameError(RuntimeError):
    description = ""

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


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
