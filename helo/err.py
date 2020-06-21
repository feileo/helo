"""
    helo.err
    ~~~~~~~~
"""


class Error(Exception):
    """Exception that is the base class of all other error exceptions"""

    description = 'Helo internal error'

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class ProgrammingError(Error):
    """Exception that caused by incorrect user programming logic"""
    description = 'User programming error'


class UnboundError(ProgrammingError):
    description = 'Database is not bound yet, maybe you should call `bind` before.'


class DuplicateBinding(ProgrammingError):
    description = 'Database already bound to {host}:{port}'

    def __init__(self, msg=None, **kwargs):
        super().__init__(msg or self.description.format(**kwargs))


class NoColumnNameError(ProgrammingError):
    description = "Column name must be specified outside the model"

    def __init__(self, msg=None):
        super().__init__(msg or self.description)


class DuplicatePKError(ProgrammingError):
    """Exceptions of two primary key fields in the same model"""


class NoPKError(ProgrammingError):
    """Exception for model missing primary key"""


class NotAllowedError(ProgrammingError):
    """Operation not allowed in helo"""


class DangerousOperation(NotAllowedError):
    """Dangerous operation due to wrong programming"""


class InvalidValueError(Error):
    """Exceptions of illegal value"""


class ProgrammingWarning(RuntimeWarning):
    """Some warnings about not being appreciated"""


class InterfaceError(Error):  # for pymysql
    """Exception raised for errors that are related to the database
    interface rather than the database itself."""


class MySQLError(Error):  # for pymysql
    description = 'Exception related to operation with MySQL.'


class MySQLWarning(Warning, MySQLError):  # for pymysql
    """Exception raised for important warnings like data truncations
    while inserting, etc."""


class MySQLDataError(MySQLError):  # for pymysql
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc."""


class OperationalError(MySQLError):  # for pymysql
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""


class IntegrityError(MySQLError):  # for pymysql
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails, duplicate key,
    etc."""


class NotSupportedError(MySQLError):  # for pymysql
    """Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off."""
