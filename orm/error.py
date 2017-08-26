#!/usr/bin/env python3
# -*- coding:utf-8 -*-


class DeleteNoneTypeError(Exception):
	"""删除为保存对象"""
	
class NoDbConnPathError(Exception):
	""" 没有为任务配置数据库连接"""