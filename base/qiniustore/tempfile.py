#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os
from config import TEMP_FILES_DIR
from logger.const import EVENT_DIR
from logger.eventfinder import find_eventname

TASK_PATH = os.path.sep.join([os.getcwd(),EVENT_DIR])


class BaseStore(object):
    pass


class TempFiles(BaseStore):
    __dir = TEMP_FILES_DIR

    def __init__(self,filename):
        self.status = False
        # 分配一个名字 会绑定一个文件
        taskname = find_eventname(TASK_PATH)
        path = os.path.sep.join([TempFiles.__dir,taskname])
        if not os.path.exists(path):
            os.makedirs(path)
        self.filename = os.path.sep.join([path,filename])

    def save(self,method ,*args,**kwargs):
        with open(self.filename,'wb') as f:
            method(f,*args,**kwargs)
        self.status = True

    def remove(self):
        if self.status is True:
            os.remove(self.filename)


class SaveFiles(BaseStore):

    def __init__(self, base_path, relative_path):
        full_path = os.path.abspath(os.path.join(base_path,relative_path))
        dirname = os.path.dirname(full_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.filename = full_path

    def save(self,method,*args,**kwargs):
        with open(self.filename,'wb') as f:
            method(f,*args,**kwargs)