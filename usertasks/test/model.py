#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import orm

class Student(orm.Model):
	__table__ = 'student'
	__name__  = '测试用表,无实际意义'


	id = orm.IntegerField(name='id',comment='主键',length=20,unsigned=True,auto_increase=True,primary_key=True)
	name = orm.StringField(name='name',length=50,varchar=True,blank=False,comment='姓名')
	password = orm.StringField(name='password',length=50,varchar=True,blank=False,comment='密码')
	cppscore = orm.DecimalField(name='cppscore',length=5,float_length=2,blank=True,comment='cpp分数')
	dbscore = orm.FloatField(name='dbscore',length=5,float_length=2,blank=True,comment='db分数')
	enscore = orm.FloatField(name='enscore',length=7,float_length=4,blank=True,double=True,comment='en分数')
	rgdate = orm.DatetimeField(name='rgdate',blank=False,comment='注册时间')
	created_at = orm.TimestampField(name='create_at',blank=False,auto='on_create',comment='创建时间')
	update_at = orm.TimestampField(name='update_at',blank=False,auto='on_update',comment='更新时间')
	unique_key_by_na_rg = orm.UniqueKey(key_name='unique_key_by_na_pass',col_name=['name','rgdate'],comment='唯一键')
	id_index = orm.Key(key_name='id_index',col_name='id',comment='索引')

	def __repr__(self):
		return '<{} \'{},{}\'>'.format(self.__class__.__name__,self.name,self.id)


class Teacher(orm.Model):
	__table__ = 'teacher'
	__name__  = '测试用表,无实际意义'


	id = orm.IntegerField(name='id',comment='主键',length=20,unsigned=True,auto_increase=True,primary_key=True)
	name = orm.StringField(name='name',length=50,varchar=True,blank=False,comment='姓名')
	password = orm.StringField(name='password',length=50,varchar=True,blank=False,comment='密码')
	age = orm.IntegerField(name='age',length=10,unsigned=True,blank=True,comment='年龄')
	sex = orm.StringField(name='age',length=10,varchar=False,blank=True,comment='性别')
	rgdate = orm.DatetimeField(name='rgdate',comment='注册时间')
	created_at = orm.TimestampField(name='create_at',blank=False,auto='on_create',comment='创建时间')
	update_at = orm.TimestampField(name='update_at',blank=False,auto='on_update',comment='更新时间')
	unique_key_by_na_rg = orm.UniqueKey(key_name='unique_key_by_na_pass',col_name=['name','rgdate'],comment='唯一键')
	id_index = orm.Key(key_name='id_index',col_name='id',comment='索引')

	def __repr__(self):
		return '<{} \'{},{}\'>'.format(self.__class__.__name__,self.name,self.id)




