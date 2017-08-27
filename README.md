# ASORM
#### ASORM是本人使用`Python3`开发和封装的简易异步ORM工具包，协程实现基于`asyncio`和`aiomysql`，目前只支持`MySQL`

## 目录结构
 - `base/`        基础任务模型
 - `component/`      组件包
 - `orm/`           orm包
 - `usertasks/` 用户任务目录
 - `config.py` 配置文件
 - `run.py`      启动
 
## base

`base`基础任务模型实现了`BaseTask`类，用户任务需继承`BaseTask`类并配置类属性`conn_path`，以指定此任务的数据库读写为`config.py`配置文件中`DB_SETING`字典中已有的数据库，该类实现了`start()`和`end()`类方法来启动和结束一个用户任务，其在内部维护了一个数据库连接池，尽量复用连接，用户必须在进行数据库读写前使用`start()`方法创建一个数据库连接池`db_con_pool`，全部完毕后使用`end()`方法关闭连接池；该基础任务模型现尚不完善，还在未来计划中，等待更新。

## component
`component`组件提供了日志(`logger`)，七牛存储(`qiniustore`)和扫描器(`scanner`)。
### logger 
`logger`日志模块提供了一个线程安全的用户日志功能，实现了对系统以及不同用户任务中打印的日志捕获的功能，打印出的日志格式为：
```
[logtype taskname datetime] message
```
其中`logtype`是消息类型，分别是`[E、I、W]`之一，代表`error`、`info`和`warning`，`taskname`是当前运行的任务(可通过`task`参数指定详细方法名)，对用户而言在不指定`task`时默认为`ASORM/usertasks/`下的用户任务（即文件夹）名称，`datetime`是日志记录的时间，`message`为具体消息内容，使用：

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

## orm
orm包为本项目的核心，该模块使用协程实现了一般orm应具有的常用功能，所有涉及对数据库的连接及读写操作的方法均`async`声明为协程，用户需要在使用时将方法声明为`async`并使用`await`来调用；除此之外其他操作非常简单，下面简单介绍。
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
### 其他常用属性

<table>
<tr><td>常用属性</td> <td>说明 </td><td>常用属性</td> <td>说明 </td></tr>
<tr><td>name </td><td> 名称</td><td>blank </td><td>能为空否</td></tr>
<tr><td>comment </td><td>备注，说明</td><td>primary_key </td><td>是否主键</td></tr>
<tr><td>default </td><td>默认值</td><td>auto_increase </td><td>是否自增</td></tr>
<tr><td>length </td><td>指定长度</td><td>unsigned</td><td>是否无符号</td></tr>
</table>
### 数据对象模型
用户数据对象模型的建立只需继承`orm.Model`，如下示例：

```
import orm 
class Student(orm.Model)：
	__table__ = 'student'
	__comment__  = '学生信息表'
	id = orm.IntegerField(name='id',length=20,unsigned=True,auto_increase=True,primary_key=True，comment='主键')
	name = orm.StringField(name='name',length=50,varchar=True,blank=False,comment='姓名')
	...
```
子类中使用类属性`__table__`来命名表名，如果没有指定，则默认使用类名，`__comment__`属性可以对表做简单说明；完成子类后便可使用该类创建数据对象实例，具体可参照usertasks目录下test任务中的示例 。
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

### API
#### 1.建表/删除表
建表和删除表分别提供两个方法。
建表：`create_all(module)`和`create_table()`
删表：`drop_all(module)`和`drop_table()`
前者用以全部创建（全部删除），只需给出`module`，该方法会创建（删除）此模块的所有表，其会在创建前检查是否存在，已存在就跳过，删除时同理。
后者由继承自`orm.Model`的子类调用，创建或删除类本身对应的表。
#### 2.




