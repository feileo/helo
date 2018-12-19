#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 启动

# from usertasks.test import TestTask
# from component import EventLogger
import asyncio
from tests.test_connector import run as runs


def run():
    runs()
    # EventLogger.info(task='usertask', message='started')
    # loop = asyncio.get_event_loop()
    # TestTask.tasks_list.append(TestTask.test_task(loop))
    # loop.run_until_complete(asyncio.wait(TestTask.tasks_list))
    # loop.close()
    # EventLogger.info(task='usertask', message='finished')


if __name__ == '__main__':
    run()
