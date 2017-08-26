# ASORM
#### ASORM是一个我个人使用`Python3`开发和封装的简易异步ORM工具包，协程实现基于`asyncio`和`aiomysql`，目前只支持MySQL

### 目录结构
 - `base/`        基础任务模型
 - `component/`      组件包
 - `orm/`           orm包
 - `usertasks/` 用户任务目录
 - `config.py` 配置文件
 - `run.py`      启动
 
### base

`base`基础任务模型实现了`BaseTask`类，用户任务需继承`BaseTask`类并配置类属性`conn_path`，以指定此任务的数据库读写为`config.py`配置文件中`DB_SETING`字典中已有的数据库，该类实现了`start()`和`end()`类方法来启动和结束一个用户任务，其在内部维护了一个数据库连接池，尽量复用连接，用户必须在进行数据库读写前使用`start()`方法创建一个数据库连接池`db_con_pool`，全部完毕后使用`end()`方法关闭连接池；该基础任务模型现尚不完善，还在未来计划中，等待更新。

### component
`component`组件提供了日志(`logger`)，七牛存储(`qiniustore`)和扫描器(`scanner`)。
#### logger 
`logger`日志模块提供了一个线程安全的用户日志功能，实现了对不同用户任务中打印的日志捕获的功能，打印出的日志格式为：
> `[logtype taskname datetime] message`

其中`logtype`是消息类型，分别是[`E`、`I`、`W`]之一，代表`error`、`info`和`warning`，`taskname`是`ASORM/usertasks/`下的用户任务（即文件夹）名称，`datetime`是日志记录的时间，`message`为具体消息内容，使用：

>`from component import EventLogger`

`EventLogger`提供`info()`，`error()`，`warning()`三个方法来打印日志；

#### qiniustore
`qiniustore`模块提供了七牛存储的简单api,使用：

> `from component import save_to_qiniu_by_url`
> `new_url = save_to_qiniu_by_url(file_url)`

该方法会将将文件下载到本地，然后再从本地上传到七牛云，其会检测`file_url`的合法性，不合法返回空字符串并给出`warning`日志，在获取失败时返回原url即`file_url`并打印一条失败(`error`)日志 。

#### scanner
`scanner`扫描器自动扫描`usertasks`目录下的继承自`BaseTask`类的用户任务并将其加入到事件列表，该功能目前还在开发中；

### orm
`orm`包为本项目的核心，该模块以协程实现了一般orm应具有的常用功能，所有涉及对数据库的连接及读写操作的方法均`async`声明为协程，用户需要使用`await`来调用；除此之外其他操作非常简单，下面进行简单介绍。
#### 数据类型
<table>
<tr><td>ASORM</td> <td>MySQL </td>  <td>说明 </td> </tr>
<tr><td>StringField</td><td>VARCHAR和CHAR</td>
<td>通过属性varchar来选择，默认varchar=False，即StringField默认为VARCHAR</td><tr>
<tr><td>IntegerField </td><td> BIGINT和INT</td> 
<td>通过属性bigint来选择，默认bigint=False，即IntegerField默认为INT</td><tr>
<tr><td>DecimalField </td><td> `DECIMAL`</td> 
<td>通过属性length和float_length来设置`DECIMAL`的(M,D)值</td><tr>
<tr><td>FloatField </td><td> DOUBLE和FLOAT</td> 
<td>通过属性double来选择，默认double=False，即FloatField默认为FLOAT</td><tr>
<tr><td>TimestampField </td><td> TIMESTAMP</td> 
<td>通过属性auto可设置是on_create还是on_update</td><tr>
<tr><td>DatetimeField </td><td> DATETIME</td> 
<td>DATETIME类型，对应与Python的datetime类型</td><tr>
<table>
