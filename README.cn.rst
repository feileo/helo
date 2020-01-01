====
trod
====

.. image:: https://img.shields.io/pypi/v/trod.svg
        :target: https://pypi.python.org/pypi/trod

.. image:: https://travis-ci.org/at7h/trod.svg?branch=master
        :target: https://travis-ci.org/at7h/trod

.. image:: https://coveralls.io/repos/github/at7h/trod/badge.svg?branch=master
        :target: https://coveralls.io/github/at7h/trod?branch=master

.. image:: https://api.codacy.com/project/badge/Grade/24451621f9554f7a8d857c5b3dd6e522
        :target: https://www.codacy.com/manual/at7h/trod?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=at7h/trod&amp;utm_campaign=Badge_Grade

.. image:: https://img.shields.io/pypi/pyversions/trod
        :target: https://img.shields.io/pypi/pyversions/trod
        :alt: PyPI - Python Version

🌟 **Trod** 是一个简单的低级别异步(asyncio_) Python ORM。它非常的直观且容易使用。

Trod 可以在你的异步应用中帮助你轻松的构建出富有表达力的常用 SQL 语句，你只需以友好的对象化 API 来操作数据，
而不用关心 SQL 语句编写、数据处理等细节。适合于业务逻辑结构较简单有一定并发量的场景。

* 支持的版本: Python 3.7+
* 目前仅支持 MySQL
* 不支持表关系操作


快速上手
--------

更多信息和上手文档请查看 trod wiki_ 页面。


安装
----

.. code-block:: console

    $ pip install trod

更多安装选项请查看 installation_ 页面。


简单示例
--------

首先，你需要引入 ``Trod`` 类并实例化一个全局变量，假设(通常)其称为 ``db``:

.. code-block:: python

    from trod import Trod

    db = Trod()


接下来，声明你的 models:

.. code-block:: python

    from trod import types

    class User(db.Model):
        id = types.BigAuto()
        name = types.VarChar(length=45, null=False)
        email = types.Email(default='')
        password = types.VarChar(length=100, null=False)
        create_at = types.Timestamp(default=types.ON_CREATE)


    class Post(db.Model):
        id = types.Auto()
        title = types.VarChar(length=100)
        author = types.Int(default=0)
        content = types.Text(encoding=types.ENCODING.utf8mb4)
        create_at = types.Timestamp(default=types.ON_CREATE)
        update_at = types.Timestamp(default=types.ON_UPDATE)


下面的脚本展示一些基本的操作示例:

.. code-block:: python

    import asyncio
    from datetime import datetime

    from trod import JOINTYPE, types


    async def show_case():

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

        # Using `trod.util.tdict`
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


    asyncio.run(show_case())

👉 查看 `更多示例 </examples>`_


贡献 👏
-------

希望感兴趣的同学可以参与进来，群策群力。

十分欢迎任何类型的贡献：
报 bug 🐞、提 issues 或提交 PR 🙋‍♂️


感谢 🤝
-------

* 特别感谢项目 aiomysql_ 和 peewee_, trod 使用了前者(作为 MySQL 连接驱动)，并在设计上参考了后者。
* 如果项目对你有帮助请朝 ⭐️ 猛戳 😉 !


.. _wiki: https://github.com/at7h/trod/wiki
.. _quickstart: https://github.com/at7h/trod/wiki#quickstart
.. _installation: https://github.com/at7h/trod/wiki#installation
.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _peewee: https://github.com/coleifer/peewee
