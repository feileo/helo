#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 日志目前只有三种级别，dberrlog尚在开发中

import os
import threading
from .const import EVENT_DIR
from .eventfinder import find_eventname
from ._baselog import BaseLog
from ._baselog import Bcolors
EVENT_PATH = os.path.sep.join([os.getcwd(), EVENT_DIR])


class EventLogger(BaseLog):
    """
    生成如下格式log
    Bcolors [E event time] trace(filename->funcname) message
    Bcolors [I event time] message
    Bcolors [W event time] message
    """
    error_partten = Bcolors.ERROR + '[E {0} {1}] {2}' + Bcolors.ENDC
    info_partten = '[I {0} {1}] {2}'  # Bcolors.OKGREEN + + Bcolors.ENDC
    warning_partten = Bcolors.WARNING + '[W {0} {1}] {2}' + Bcolors.ENDC
    __lock = threading.Lock()

    @staticmethod
    def info(message, task=None):
        if task is None:
            event_name = find_eventname(EVENT_PATH)
        else:
            event_name = task
        EventLogger.__lock.acquire()
        log_str = EventLogger.info_partten.format(event_name, BaseLog.now_time(), message)
        BaseLog.info(log_str)
        EventLogger.__lock.release()

    @staticmethod
    def error(message=None, error=None, task=None):
        if task is None:
            event_name = find_eventname(EVENT_PATH)
        else:
            event_name = task
        error_str = '%s:%s' % (error.__class__.__name__, error.message) if error else ''
        real_message = '%s %s' % (message if message else '', error_str)
        EventLogger.__lock.acquire()
        log_str = EventLogger.error_partten.format(event_name, BaseLog.now_time(), real_message)
        BaseLog.error(log_str)
        EventLogger.__lock.release()

    @staticmethod
    def warning(message, task=None):
        if task is None:
            event_name = find_eventname(EVENT_PATH)
        else:
            event_name = task
        EventLogger.__lock.acquire()
        log_str = EventLogger.warning_partten.format(event_name, BaseLog.now_time(), message)
        BaseLog.warning(log_str)
        EventLogger.__lock.release()

    @staticmethod
    def dberrlog(message, task=None):
        pass
