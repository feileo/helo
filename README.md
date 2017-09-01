# ASORM
#### ASORM是本人使用`Python3.5`开发和封装的简易异步`ORM`，协程实现，操作容易，目前只支持`MySQL`，不定期更新中

## 简单说明几点
1. ASORM的orm包是核心，其封装了访问和操作底层数据库的所有方法，可独立与其他组件单独使用；
2. 后续版本肯定也会支持这种情况，但是包名现不能确定；
3. 编写此ORM的初衷只是为了方便爬虫的编写，后来逐步完善；
4. 异步设计的目的是框架旨在为使用异步web服务的开发者提供异步数据库访问，(目标太远大呀)；
5. 显而易见存在的问题是，此项目的结构(见目录结构)与3中所述存在矛盾，但这只是一个组件的选择和进一步封装的问题；
6. 目前所采用的结构是为了对用户任务的管理，实则是一个测试和学习的过程；
7. 其实后续要做的还很多，任重道远，能力有限，还望有大牛们不吝指教，如果有这个价值的话；

## 目录结构
 - `base/`        基础任务模型
 - `component/`      组件包
 - `orm/`           orm包
 - `usertasks/` 用户任务目录
 - `config.py` 配置文件
 - `run.py`      启动
 
## base模型

base基础任务模型实现了`BaseTask`类，用户任务需继承`BaseTask`类并配置类属性`conn_path`，以指定此任务的数据库读写为`config.py`配置文件中`DB_SETING`字典中已有的数据库，该类实现了`start()`和`end()`类方法来启动和结束一个用户任务，其在内部维护了一个数据库连接池，尽量复用连接，用户必须在进行数据库读写前使用`start()`方法创建一个数据库连接池`db_con_pool`，全部完毕后使用`end()`方法关闭连接池；该基础任务模型现尚不完善，还在更新计划中。

## component组件
component组件提供了日志(`logger`)，七牛存储(`qiniustore`)和扫描器(`scanner`)。
### logger 
logger日志模块提供了一个线程安全的用户日志功能，实现了对系统以及不同用户任务中打印的日志捕获的功能，打印出的日志格式为：
```
[logtype taskname datetime] message
```
其中`logtype`是消息类型，分别是`E、I、W`之一，代表`error`、`info`和`warning`，`taskname`是当前运行的任务(可通过`task`参数指定详细方法名)，对用户而言在不指定`task`时默认为ASORM/usertasks/下的用户任务（即文件夹）名称，`datetime`是日志记录的时间，`message`为具体消息内容，使用：

```
from component import EventLogger
```

EventLogger提供`info()`，`error()`，`warning()`三个方法来打印不同类型的日志；

### qiniustore
qiniustore模块提供了七牛存储的简单api,使用方法：

```
from component import save_to_qiniu_by_url 
new_url = save_to_qiniu_by_url(file_url)
```

该方法会将将文件下载到本地，然后再从本地上传到七牛云，本地临时文件(临时文件目录可在config.py中的`TEMP_FILES_DIR`设置)会在上传成功后自动从硬盘删除，其会检测`file_url`的合法性，不合法返回空字符串并给出`warning`日志，在获取失败时返回原url即`file_url`并打印一条失败(`error`)日志 。

### scanner
scanner扫描器会自动扫描usertasks目录下的继承自`BaseTask`类的用户任务并将其加入到事件列表，该功能目前还在开发中；

## orm模块
orm包为本项目的核心，该模块封装了orm的所有操作方法并使用协程实现了一般orm应具有的常用功能，所有涉及对数据库的连接及读写操作的方法均`async`声明为协程，用户需要在使用时将方法声明为`async`并使用`await`来调用；除此之外其他操作非常简单，下面简单介绍。
### 数据类型
<table>
<tr><td>ASORM</td> <td>MySQL </td> <td>说明 </td> </tr>
<tr><td>StringField</td><td>VARCHAR和CHAR</td>
<td>通过属性varchar选择，默认varchar=False，即StringField默认为VARCHAR</td></tr>
<tr><td>IntegerField </td><td> BIGINT和INT</td> 
<td>通过属性bigint来选择，默认bigint=False，即IntegerField默认为INT</td></tr>
<tr><td>DecimalField </td><td> DECIMAL</td> 
<td>通过属性length和float_length来设置DECIMAL的(M,D)值</td></tr>
<tr><td>FloatField </td><td> DOUBLE和FLOAT</td> 
<td>通过属性double来选择，默认double=False，即FloatField默认为FLOAT</td></tr>
<tr><td>TimestampField </td><td> TIMESTAMP</td> 
<td>通过属性auto可设置是on_create还是on_update</td></tr>
<tr><td>DatetimeField </td><td> DATETIME</td> 
<td>DATETIME类型，对应与Python的datetime类型</td></tr>
</table>

### 索引类型

<table>
<tr><td>ASORM</td> <td>MySQL </td>  <td>说明 </td> </tr>
<tr><td>Key </td><td> KEY</td> 
<td>可通过其属性col_name列表来设置索引列</td></tr>
<tr><td>UniqueKey </td><td> UNIQUE KEY</td> 
<td>可通过其属性col_name列表来设置索引列</td></tr>
<tr><td>ForeignKey </td><td> FOREIGN KEY</td> 
<td>关系目前还在更新计划中</td></tr>
</table>

### 定义列属性

<table>
<tr><td>常用属性</td> <td>说明 </td><td>常用属性</td> <td>说明 </td></tr>
<tr><td>name </td><td> 名称</td><td>blank </td><td>能为空否</td></tr>
<tr><td>comment </td><td>备注，说明</td><td>primary_key </td><td>是否主键</td></tr>
<tr><td>default </td><td>默认值</td><td>auto_increase </td><td>是否自增</td></tr>
<tr><td>length </td><td>指定长度</td><td>unsigned</td><td>是否无符号</td></tr>
</table>

### 如何使用

用户数据对象模型的建立只需继承`orm.Model`，如下示例：

```
import orm 
class Student(orm.Model)：
    __table__ = 'student'
    __comment__ = '学生信息表'
	
    id = orm.IntegerField(name='id',length=20,unsigned=True,auto_increase=True,primary_key=True，comment='主键')
    name = orm.StringField(name='name',length=50,varchar=True,blank=False,comment='姓名')
    ...
```
子类中使用类属性`__table__`来命名表名，如果没有指定，则默认使用类名，`__comment__`属性可以对表做简单说明；完成子类后便可使用该类创建数据对象实例，如`test_stu_obj = Student()`，具体可参照usertasks目录下test任务中的示例 。
继承自`orm.Model`的子类会被自动扫描进行 buliding model关系映射，并给出如下示例的log：

```
[I scanning 20170827-12:43:19] found model 'Student', table_name [student]
[I omapping 20170827-12:43:19]    |=>  buliding model 'Student'...
```
构建完成后待usertask任务开始后，会与配置的数据库建立连接，并打印如下log：

```
[I building 20170827-12:43:19] create database connection pool
[I building 20170827-12:43:19]    |=>  linking model 'Student' to conn_path 'test1'
[I building 20170827-12:43:19]    |=>  linking model 'Teacher' to conn_path 'test1'
```
### API使用简介

#### 建表/删除表
建表和删除表分别提供两个方法。<br>
 - 建表：`create_all(module)`和`create_table()`<br>
 - 删表：`drop_all(module)`和`drop_table()`<br>
 
前者用以全部创建（全部删除），只需给出`module`，该方法会创建（删除）此模块的所有表，其会在创建前检查是否存在，已存在就跳过，删除时同理。<br>
后者由继承自`orm.Model`的子类或其实例调用，创建或删除类本身对应的表。
#### 主键
当设置主键`auto_increase=True`后，对象主键将由系统管理，在`test_stu_obj`对象未进行保存（成功提交到数据库）时，`test_obj.id`将返回`None`，保存后方可返回其id。
#### 添加/插入行

 - 保存当前对象

向数据库添加行使用`save()`方法，该方法必须为实例方法，通过实例调用如`await test_stu_obj.save()`，该方法会将调用对象的信息保存到数据库，其会判断是执行`_add()`进行插入，还是`_save_update()`做更新。

 - 直接插入和批量插入
 
还可以通过类方法`insert(data)`和`batch_insert(data)`进行直接插入和批量插入，前者传入`data`为插入的数据字典，后者为一个列表，每个元素为一个字典（即1行数据）。

 - 条件插入
 
类方法`conditional_insert(data,where)`为条件插入，其可根据传入的`where`参数进行查询，如果已经存在记录就更新数据，不存在就进行插入。
#### 更新行
更新行使用类方法`update(uid=None,where={},what={})`，当`uid`不为`None`时，查询条件使用`uid`，为`None`使用`where`条件。

#### 删除行
 - 删除当前对象

从数据库删除当前对象数据使用方法`delete()`，其也必须为实例方法，通过实例调用如`await test_stu_obj.delete()`。

 -  批量删除
 
类方法`remove(uid=None,where={})`会根据所给查询条件进行删除，用法与`update()`类似，不在赘述。

 - 特例·按主键列表批量删除
 
类方法`remove_by_ids(uid=[])`提供一种特殊的删除方法，其可根据传入的主键列表进行批量删除。
### 查询行
一般来说，查询应该是比较重要的了，所以我把这个标题都放大了一级，哈哈哈哈。

#### 特别推荐用法，常用

综合查询我会推荐大家使用下面的方法，其可基本满足一般的数据库查询需要，其完整原型为：
```
query_all(*query_fields).filter(**kwargs).order_by(field=None,desc=False).limit(count,start_num=0).select()
```

 1. 可在`query_all`方法的`*query_fields`参数给出想要查询的字段（列），`query_all`方法为类方法，可通过类调用，别忘了`await`哦；
 2. 过滤器`filter`可以给出数量不等的查询条件，例如`filter(name='gjw',age=24)`，还可包括like(`lk`)，小于(`lt`)，大于(`mt`)，小于等于(`leq`)，大于等于(`meq`)等查询;
 3. `order_by`方法可以指定按哪个字段排序，`DESC`还是`ASC`，默认为`DESC`，此方法可选；
 4. `limit`方法可以指定返回的行偏移量和行数，此方法可选；
 5. `select`方法执行数据库查询并返回符合查询条件的所有对象（列表）。

该方法完成了一次双向的映射，即返回的是类对象（列表），即可以通过访问属性值的方法来查看返回的值。

#### 其他常用查询方法
 - 主键查询

通过主键查询数据可直接使用类`get(uid)`方法，其会返回给定主键的行数据；

 -  查询全部
 
返回全部行数据使用类`select_all()`方法；

 - 等值查询
 
类方法`select_eq_filter(**kwargs)`提供等值过滤查询，只需给出查询条件（数量不等的等值条件或键值对）即可；

 - like查询
 
类方法`select_like_filter(**kwargs)`提供`LIKE`过滤查询，使用方法与上面的方法类似。

 - 自定义查询

orm包还提供了两种自定义查询的方法`select_custom_filter(**kwargs)`和`select_custom_where(where_clause)`

查询就介绍到这里了，推荐使用上面第一种推荐的方法，合理的使用其完全可以涵盖下面介绍的所有功能。
## usertasks
usertasks为用户的任务存放目录，管理将以该目录下的文件夹作为单位，该目录下目前只有一个test测试任务。
## 启动
run.py启动文件现在尚不完善，只能手动导入，需等待`scanner`完成后自动扫描任务并加入事件列表，不定期更新吧。
## TODO

 - 现阶段还有一些细小的待修正的不正确不合理设计需要update
 - 完善base基础模型，完成`scanner`和任务的管理调度
