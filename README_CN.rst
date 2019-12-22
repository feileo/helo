====
trod
====

.. image:: https://img.shields.io/pypi/v/trod.svg
        :target: https://pypi.python.org/pypi/trod

.. image:: https://travis-ci.org/at7h/trod.svg?branch=master
    :target: https://travis-ci.org/at7h/trod

.. image:: https://coveralls.io/repos/github/at7h/trod/badge.svg?branch=master
        :target: https://coveralls.io/github/at7h/trod?branch=master

.. image:: https://img.shields.io/github/license/at7h/trod?color=9cf
        :target: https://img.shields.io/github/license/at7h/trod?color=9cf
        :alt: GitHub

**Trod** 是一个使用 Python asyncio_ 开发的低级别的简单的异步 ORM。

* 用它能轻松的构建出富有表达力的常用 SQL，适于业务结构较简单有一定并发量的场景
* 支持的 Python 版本为 3.7+
* 目前仅支持 MySQL，使用 aiomysql_ 作为连接驱动
* 不支持表关系操作

快速上手:

* 查看 `基础示例 </examples>`_


安装
----

.. code-block:: console

    pip install trod


简单示例
--------

定义 `Model` 很简单:

.. code-block:: python

    from trod import Model, types

    class User(Model):
        id = types.BigAuto()
        name = types.VarChar(length=45, null=False)
        email = types.Email(default='')
        password = types.VarChar(length=100, null=False)
        create_at = types.Timestamp(default=types.ON_CREATE)


    class Post(Model):
        id = types.Auto()
        title = types.VarChar(length=100)
        author = types.Int(default=0)
        content = types.Text(encoding=types.ENCODING.utf8mb4)
        create_at = types.Timestamp(default=types.ON_CREATE)
        update_at = types.Timestamp(default=types.ON_UPDATE)


下面的脚本展示一些基本的示例:

.. code-block:: python

    import asyncio
    from datetime import datetime

    from trod import Trod, JOINTYPE, types


    db = Trod()


    async def base_example():

        # Binding the database(creating a connection pool)
        # and create the table:
        await db.bind('mysql://user:password@host:port/db')
        await db.create_tables([User, Post])

        # Inserting few rows:

        user = User(name='at7h', password='1111')
        user_id = await user.save()
        print(user_id)  # 1

        users = await User.get(user_id)
        print(user.id, user.name)  # 1, at7h

        await User.update(email='g@gmail.com').where(User.id == user_id).do()

        ret = await User.insert(name='pope', password='2222').do()
        posts = [
            {'title': 'Python', 'author': 1},
            {'title': 'Golang', 'author': 2},
        ]
        ret = await Post.minsert(posts).do()
        print(ret)  # (2, 1)

        # Supports expressive and composable queries:

        count = await User.select().count()
        print(count) # 2

        # Last gmail user
        user = await User.select().where(
            User.email.endswith('gmail.com')
        ).order_by(
            User.create_at.desc()
        ).first()
        print(user) # [<User object> at 1]

        # using `trod.util.tdict`
        users = await User.select(
            User.id, User.name
        ).where(
            User.id < 2
        ).all(wrap=False)
        print(user)  # [{'id': 1, 'name': 'at7h'}]

        # Paginate get users who wrote Python posts this year
        users = await User.select().where(
            User.id.in_(
                Post.select(Post.author).where(
                    Post.update_at > datetime(2019, 1, 1),
                    Post.title.contains('Python')
                ).order_by(
                    Post.update_at.desc()
                )
            )
        ).paginate(1, 10)
        print(users) # [<User object> at 1]

        # How many posts each user wrote?
        user_posts = await User.select(
            User.name, types.F.COUNT(types.SQL('1')).as_('posts')
        ).join(
            Post, JOINTYPE.LEFT, on=(User.id == Post.author)
        ).group_by(
            User.name
        ).rows(100)


    asyncio.run(base_example())

👉 查看 `更多示例 </examples>`_


其他
----

* 🌱 Trod 还很稚嫩，有很多不足，希望大家感兴趣的可以参与进来，我也会常常更新
* 😊 特别感谢项目 aiomysql_ 和 peewee_, trod 使用了前者，并在设计上参考了后者
* 👏 十分欢迎任何类型的贡献：报 bug 🐞，提 issues 或提交 PR 🙋‍♂️


Todo
----

* ✍️  编写文档


.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _peewee: https://github.com/coleifer/peewee
