#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from orm import create_all,drop_all
from . import model
from config import DB_PATH
from component import EventLogger
import asyncio
from datetime import datetime,timedelta
import random
from base import BaseTask

name_list = ['gjwdw','lmgjw','jbbdw','lizha','gjjmd','chpoo','acths','mmssa','dwawb','tyuxa']
def ran_name():
    return name_list[random.randint(0,9)]
def ran_password():
    return str(random.randint(0,100000))
def ran_score():
    return random.random()*100
def ran_sex(i):
    return 'm' if i%2==0 else 'f'
def ran_date():
    return datetime.now()-timedelta(days=random.randint(0,100))

# 本任务为简单测试任务，操作无实际意义

class TestTask(BaseTask):
    # 配置连接数据库
    conn_path = DB_PATH.test1

    @classmethod
    async def test_task(cls,loop):
        # 开始任务
        await cls.start(loop)
        await create_all(model)

        test_stu_obj = model.Student(name = ran_name(),password = ran_password(),cppscore = 199.12,
                                     dbscore = ran_score(),enscore = ran_score(),chscore = ran_score(),
                                     rgdate = ran_date())
        
        # test_tea_obj = model.Teacher(name = ran_name(), password = ran_password(), 
        #                              age = random.randint(20,100),sex = ran_sex(random.randint(0,10)),
        #                              rgdate = datetime.now()-timedelta(days=random.randint(0,100)))
        test_tea_obj = model.Teacher()
        for i in range(len(name_list)):
            test_tea_obj.name = name_list[i]
            test_tea_obj.password = ran_password()
            test_tea_obj.age = random.randint(20,100)
            test_tea_obj.sex = ran_sex(i)
            test_tea_obj.rgdate = datetime.now()-timedelta(days=random.randint(0,100))
            exist_tea = await \
            model.Teacher.query_all().filter(name=test_tea_obj.name,age=test_tea_obj.age).order_by('id').select()
            if exist_tea is not None:
                continue
            await test_tea_obj.save()

        ############################# 主键 ##########################
        # 当主键属性auto_increase(AI)设为true时,则主键由框架管理,此时test_stu_obj.id将返回None
        # print(test_stu_obj.id)
        # save之后可以正常返回id test_stu_obj.id
        ############################# 建表 ##########################
        # 全部创建 使用create_all  推荐
        # await create_all(model)
        # 创建表
        #  try:
        #     await model.Student.create_table()或者await test_stu_obj.create_table()
        # except:
        #     EventLogger.warning('<Table \'{}>\' already exists"'.format(test_stu_obj.__table__))
        # 查看建表语句
        # test_stu_obj.show_create_table()
        
        ############################# 对象 ###########################
        # 对于当前对象,你可以
        await test_stu_obj.save()
        # await test_stu_obj.delate()
        ############################# 维护 ###########################
        ### 添加行 ###
        # 保存当前对象(行),save()会判断对象是否存在,不存在就添加记录,存在就进行更新
        # await test_tea_obj.save()
        # await test_stu_obj.save()
        # 插入行
        # data = {'name':'gjw','password':'mammaa','rgdate':ran_date()}
        # await model.Student.insert(data)
        # 批量插入
        # model.Student.batch_insert(data)
        # 条件插入
        # model.Student.conditional_insert(data, where)
        ### 更新行 ###
        # 修改当前对象 save()方法会判断对象对应记录是否存在,已存在就更新,不存在就添加
        # test_stu_obj.name='mmmmmmmm'
        # await test_stu_obj.save()
        # 按指定条件修改update(uid=None, where={}, what={})
        # await model.Student.update(uid=1,what={'name':'lllllll','cppscore': 199.12})
        ### 删除行 ###
        # 删除当前对象(只能对象调用)
        # await test_stu_obj.delete()
        # 按条件批量删除
        # await model.Student.remove(uid=None, where={'id':3})
        # 按主键列表批量删除
        # await model.Student.remove_by_ids([1,2,3,4,5,6])
        ### 查询 ###
        # 主键查询 直接使用get
        EventLogger.info('get: {}'.format(len(await model.Student.get(3))))
        # # 查询全部 
        EventLogger.info('all: {}'.format(len(await model.Student.select_all())))
        # 等值查询 
        EventLogger.info('eq: {}'.format(len(await model.Student.select_eq_filter(cppscore = 199.12,name='gjwdw'))))
        # like查询 
        EventLogger.info('like: {}'.format(len(await model.Student.select_like_filter(name='gjw'))))
        # # 自定义filter查询 
        # EventLogger.info('custom_filter: {}'.format(len(await model.Student.select_custom_filter(filter1='name=\'acths\'',filter2='id=17'))))
        # # 自定义where查询
        # EventLogger.info('custom_where: {}'.format(len(await model.Student.select_custom_where('WHERE id=1 and name=\'acths\''))))
        # 综合条件查询  常用 推荐
        # 过滤器filter(id=1,name=test_stu_obj.name[,...]), limit(count,start_num=0), select(rows)
        query = await \
            model.Student.query_all('id','name','cppscore').filter(name=test_stu_obj.name).order_by('id').select()
            # model.Student.query_all().filter(name=test_stu_obj.name,cppscore=199.12).order_by('id').select()
        # print(query[[0]])
        if not isinstance(query,list) and query is not None:
            EventLogger.info('query: 1 row')
            EventLogger.info('({} {} {})'.format(query.id,query.name,query.cppscore))
        elif isinstance(query,list):
            EventLogger.info('query: {} rows'.format(len(query)))
            for query_i in query:
                EventLogger.info('({} {} {})'.format(query_i.name,query_i.id,query_i.cppscore))
        else:
            EventLogger.info('query: Did not query the results')

        ############################# 删除表 ##########################
        # 删除表
        # await model.Student.drop_table()
        # 全部删除
        # await drop_all(model)
        
        await cls.end()
