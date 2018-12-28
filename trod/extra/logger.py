import inspect
import os
import threading
from datetime import datetime

from trod.utils import Dict


EVENT_DIR = 'trod'
DEFAULT_EVENT_DIR = os.path.sep.join([os.getcwd(), EVENT_DIR])
TIME_FORMAT = '%Y%m%d-%H:%M:%S'


def find_eventname(common_prefix=DEFAULT_EVENT_DIR):
    norm_common_prefix = os.path.normpath(common_prefix)
    lastframe = inspect.getouterframes(inspect.currentframe())
    complete = False
    for l_f in lastframe[2:]:
        call_path = os.path.normpath(l_f[1])
        if os.path.commonprefix([call_path, norm_common_prefix]) == norm_common_prefix:
            complete = True
            break
    if not complete:
        return None
    common_length = len(common_prefix.split(os.path.sep))
    return call_path.split(os.path.sep)[common_length]


class BaseLog:

    Bcolors = Dict(
        HEADER='\033[95m',
        OKBLUE='\033[94m',
        OKGREEN='\033[92m',
        WARNING='\033[93m',
        ERROR='\033[91m',
        ENDC='\033[0m',
    )

    @staticmethod
    def now_time():
        return datetime.now().strftime(TIME_FORMAT)

    @staticmethod
    def info(message):
        print(message)

    @staticmethod
    def error(message):
        print(message)

    @staticmethod
    def warning(message):
        print(message)


class Logger:
    """
    BaseLog.Bcolors [E event time] trace(filename->funcname) message
    BaseLog.Bcolors [I event time] message
    BaseLog.Bcolors [W event time] message
    """

    error_partten = BaseLog.Bcolors.ERROR + '[E {0} {1}] {2}' + BaseLog.Bcolors.ENDC
    info_partten = '[I {0} {1}] {2}'
    warning_partten = BaseLog.Bcolors.WARNING + '[W {0} {1}] {2}' + BaseLog.Bcolors.ENDC
    __lock = threading.Lock()

    @staticmethod
    def info(message, task=None):
        if task is None:
            event_name = find_eventname(DEFAULT_EVENT_DIR)
        else:
            event_name = task
        Logger.__lock.acquire()
        log_str = Logger.info_partten.format(
            event_name, BaseLog.now_time(), message
        )
        BaseLog.info(log_str)
        Logger.__lock.release()

    @staticmethod
    def error(message=None, error=None, task=None):
        if task is None:
            event_name = find_eventname(DEFAULT_EVENT_DIR)
        else:
            event_name = task
        error_str = '{}:{}'.format(error.__class__.__name__, error.message if error else '')
        real_message = '{}, {}'.format(message if message else '', error_str)
        Logger.__lock.acquire()
        log_str = Logger.error_partten.format(
            event_name, BaseLog.now_time(), real_message
        )
        BaseLog.error(log_str)
        Logger.__lock.release()

    @staticmethod
    def warning(message, task=None):
        if task is None:
            event_name = find_eventname(DEFAULT_EVENT_DIR)
        else:
            event_name = task
        Logger.__lock.acquire()
        log_str = Logger.warning_partten.format(
            event_name, BaseLog.now_time(), message
        )
        BaseLog.warning(log_str)
        Logger.__lock.release()

    @staticmethod
    def dberrlog(message, task=None):
        pass
