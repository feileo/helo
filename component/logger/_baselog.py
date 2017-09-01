#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# import sys
from datetime import datetime
from .const import TIME_FORMAT

# def log_imp(string):
#     print string

def now_log_time():
    return datetime.now().strftime(TIME_FORMAT)

class BaseLog(object):
    @staticmethod
    def now_time():
        return now_log_time()

    @staticmethod
    def info(message):
        print(message)

    @staticmethod
    def error(message):
        print(message)

    @staticmethod
    def warning(message):
        print(message)

class Bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    ENDC = '\033[0m'