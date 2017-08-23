#!/test_objsr/bin/env python3
# -*- coding:utf-8 -*-
from usertasks.test import TestTask
import asyncio

def Run():
	# 建立事件循环
	loop = asyncio.get_event_loop()
	# 定义事件列表
	tasks = [TestTask.test_task(loop),Con_TestTask.test_task(loop)]
	# run
	loop.run_until_complete(asyncio.wait(tasks))
	# close
	loop.close()

if __name__ == '__main__':
    Run()
