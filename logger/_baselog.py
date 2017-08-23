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
    def log(message):
        print(message)

    @staticmethod
    def error(message):
        print(message)

    @staticmethod
    def warning(message):
        print(message)
