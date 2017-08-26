# ASORM
`ASORM`是一个我个人使用`Python3`开发和封装的简单异步ORM工具包，协程实现基于`asyncio`和`aiomysql`，目前只支持MySQL。
### 目录结构
 - `base/`        基础任务模型
 - `component/`      组件包
 - `orm/`           orm包
 - `usertasks/` 用户任务目录
 - `config.py` 配置文件
 - `run.py`      启动
 
`base`基础任务模型实现了`BaseTask`类，用户任务需继承`BaseTask`类并配置类属性`conn_path`，以指定此任务的数据库读写为`config.py`配置文件中`DB_SETING`字典中已有的数据库，该类实现了`start()`和`end()`方法来启动和结束一个用户任务，其在内部维护了一个数据库连接池，尽量复用连接，用户必须在进行数据库读写前使用`start()`方法创建一个数据库连接池`db_con_pool`，全部完毕后使用`end()`方法关闭连接池；该基础任务模型现尚不完善，还在未来计划中，等待更新。

`component`组件提供了日志(`logger`)，七牛存储(`qiniustore`)和扫描器(`scanner`)。

`logger`日志模块提供了一个线程安全的用户日志功能，实现了对不同用户任务中打印的日志捕获的功能，打印出的日志格式为：
> `[logtype taskname datetime] message`

其中`logtype`是消息类型，分别是[`E`、`I`、`W`]之一，代表`error`、`info`和`warning`，`taskname`是`ASORM/usertasks/`下的用户任务（即文件夹）名称，`datetime`是日志记录的时间，`message`为具体消息内容，用法如下：

>`from component import EventLogger`
>`EventLogger`提供`info()`，`error()`，`warning()`三个方法来打印日志

`qiniustore`模块提供了七牛存储的简单api；
`scanner`扫描器自动扫描`usertasks`目录下的继承自`BaseTask`类的用户任务并将其加入到事件列表，该功能目前还在开发中；


