====
trod
====

🌎 [`English </README.rst>`_] ∙ [`简体中文 </README.cn.rst>`_]

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

🌟 **Trod** is a simple low-level asynchronous ORM using Python asyncio_.
It is very intuitive and easy to use.

Trod can help you easily build expressive common SQL statements in your asynchronous applications.
You only need to use friendly object-oriented APIs to manipulate data without caring about the details of SQL statement writing and data processing. 
Suitable for scenarios where the business logic structure is relatively simple and has a certain amount of concurrency.

* Requires: Python 3.7+
* Now only supports MySQL
* Not supports table relationship

Quickstart
----------

See the wiki_ page for more information and quickstart_ documentation.


Installation
------------

.. code-block:: console

    $ pip install trod

See the installation_ wiki page for more options.


Basic Examples
--------------

First, you should to import the ``Trod`` and instantiate a global variable:

.. code-block:: python

    from trod import Trod

    db = Trod()


Defining models is simple:

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


Show some basic examples:

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

👉 See `more examples </examples>`_


Contributing 👏
---------------

I hope those who are interested can join in and work together.

Any kind of contribution is expected:
report a bug 🐞, give a advice or create a pull request 🙋‍♂️.


Thanks 🤝
---------

* Special thanks to projects aiomysql_ and peewee_, trod uses aiomysql_ (as the MySQL connection driver),
  and referenced peewee_ in program design.
* Please feel free to ⭐️ this repository if this project helped you 😉!

.. _wiki: https://github.com/at7h/trod/wiki
.. _quickstart: https://github.com/at7h/trod/wiki#quickstart
.. _installation: https://github.com/at7h/trod/wiki#installation
.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _peewee: https://github.com/coleifer/peewee
