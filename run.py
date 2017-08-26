#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from usertasks.test import TestTask
from component import EventLogger
import asyncio

def run():
	EventLogger.log(task='usertask', message='started')
	loop = asyncio.get_event_loop()
	TestTask.tasks_list.append(TestTask.test_task(loop))
	loop.run_until_complete(asyncio.wait(TestTask.tasks_list))
	loop.close()
	EventLogger.log(task='usertask', message='finished')

if __name__ == '__main__':
    run()
