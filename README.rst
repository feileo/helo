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

**Trod** is a low-level simple asynchronous ORM using Python asyncio_.

View `Chinese </README_CN.rst>`_

* Using it to easily build expressive and commonly used SQL, 
  suitable for scenarios with simple business structures and a certain amount of concurrency
* Requires: Python 3.7+
* Now only supports MySQL, using aiomysql_ as the connection driver
* Not supports table relationship

Quickstart:

* See `basic example </examples>`_


Installation
------------

.. code-block:: console

    pip install trod


Base Examples
-------------

Defining models is simple:

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


Shows some basic examples:

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

👉 See `more examples </examples>`_


Contributing 👏
---------------

* I hope those who are interested can join in and work together.
  Any kind of contribution is expected: 
  report a bug 🐞, give a advice or create a pull request 🙋‍♂️.


Thanks 🤝
---------

* Special thanks to projects aiomysql_ and peewee_, trod uses aiomysql_, 
  and referenced peewee_ in program design.
* Please feel free to ⭐️ this repository if this project helped you 😉! 

TODO 📝
-------

* Documents ✍️


.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _peewee: https://github.com/coleifer/peewee
