.. image:: https://camo.githubusercontent.com/2b515b67e9b90f7168811598839a76c0f1553152/687474703a2f2f63646e2e617437682e636f6d2f68656c6f2e706e67?t=0

====
helo
====

🌎  [English_] ∙ [`简体中文`_]

.. image:: https://img.shields.io/pypi/v/helo.svg
        :target: https://pypi.python.org/pypi/helo

.. image:: https://travis-ci.org/at7h/helo.svg?branch=master
        :target: https://travis-ci.org/at7h/helo

.. image:: https://coveralls.io/repos/github/at7h/helo/badge.svg?branch=master
        :target: https://coveralls.io/github/at7h/helo?branch=master

.. image:: https://app.codacy.com/project/badge/Grade/c68578653eb546488fadddd95f19939c
        :target: https://www.codacy.com/manual/at7h_/helo?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=at7h/helo&amp;utm_campaign=Badge_Grade

.. image:: https://img.shields.io/pypi/pyversions/helo
        :target: https://img.shields.io/pypi/pyversions/helo
        :alt: PyPI - Python Version

**Helo** is a simple and small low-level asynchronous ORM using Python asyncio_.
It is very intuitive and easy to use.

Helo can help you easily build expressive common SQL statements in your asynchronous applications.
You only need to use friendly object-oriented APIs to manipulate data without caring about the details of SQL statement writing and data processing.

* Requires: Python 3.7+
* Only supports MySQL now, and the version is 5.7+
* Integration with web framework:

  - quart_, see `with quart <#with-quart>`_

* Not supports table relationship now


Installation
============

.. code-block:: bash

    $ pip install helo

See the installation_ wiki page for more options.


Quickstart
==========

See the wiki_ page for more information and quickstart_ documentation.


Basic Examples
==============

First, you should to import ``helo`` and instantiate a global variable with ``helo.G``

.. code-block:: python

    import helo

    db = helo.G()


Defining models is simple:

.. code-block:: python

    class Author(helo.Model):
        id = helo.BigAuto()
        name = helo.VarChar(length=45, null=False)
        email = helo.Email(default='')
        password = helo.VarChar(length=100, null=False)
        create_at = helo.Timestamp(default=helo.ON_CREATE)


    class Post(helo.Model):
        id = helo.Auto()
        title = helo.VarChar(length=100)
        author = helo.Int(default=0)
        content = helo.Text(encoding=helo.ENCODING.UTF8MB4)
        create_at = helo.Timestamp(default=helo.ON_CREATE)
        update_at = helo.Timestamp(default=helo.ON_UPDATE)


Show some basic examples:

.. code-block:: python

    import asyncio
    import datetime


    async def show_case():
        # Binding the database(creating a connection pool)
        await db.bind('mysql://user:password@host:port/db')
        # Creating tables
        await db.create_tables([Author, Post])

        # Inserting few rows:

        author = Author(name='at7h', password='1111')
        author_id = await author.save()
        print(author_id)  # 1

        authors = await Author.get(author_id)
        print(author.id, author.name)  # 1, at7h

        await Author.update(email='g@gmail.com').where(Author.id == author_id).do()

        ret = await Author.insert(name='pope', password='2222').do()
        posts = [
            {'title': 'Python', 'author': 1},
            {'title': 'Golang', 'author': 2},
        ]
        ret = await Post.minsert(posts).do()
        print(ret)  # (2, 1)

        # Supports expressive and composable queries:

        count = await Author.select().count()
        print(count) # 2

        # Last gmail author
        author = await Author.select().where(
            Author.email.endswith('gmail.com')
        ).order_by(
            Author.create_at.desc()
        ).first()
        print(author) # [<Author object at 1>]

        # Using `helo.adict`
        authors = await Author.select(
            Author.id, Author.name
        ).where(
            Author.id < 2
        ).all(wrap=False)
        print(author)  # [{'id': 1, 'name': 'at7h'}]

        # Paginate get authors who wrote Python posts this year
        authors = await Author.select().where(
            Author.id.in_(
                Post.select(Post.author).where(
                    Post.update_at > datetime.datetime(2019, 1, 1),
                    Post.title.contains('Python')
                ).order_by(
                    Post.update_at.desc()
                )
            )
        ).paginate(1, 10)
        print(authors) # [<Author object at 1>]

        # How many posts each author wrote?
        author_posts = await Author.select(
            Author.name, helo.F.COUNT(helo.SQL('1')).as_('posts')
        ).join(
            Post, helo.JOINTYPE.LEFT, on=(Author.id == Post.author)
        ).group_by(
            Author.name
        ).rows(100)

    asyncio.run(show_case())


With Quart
----------

If you're using quart_ , a minimum application example is:

.. code-block:: python

    import quart
    import helo

    app = quart.Quart(__name__)
    app.config["HELO_DATABASE_URL"] = "mysql://user:password@host:port/db"

    db = helo.G(app)


    @app.route('/api/authors')
    async def authors():
        await Author.insert(
            name='at7h', email='g@test.com', password='xxxx'
        ).do()
        author_list = await Author.select().all(False)
        return quart.jsonify(author_list)


    app.run()

Run it:

.. code-block:: bash

    $ curl http://127.0.0.1:5000/api/authors
    [{"email":"g@test.com","id":1,"name":"at7h","password":"xxxx"}]

👉 See `more examples </examples>`_


Contributing 👏
===============

I hope those who are interested can join in and work together.

Any kind of contribution is expected:
**report a bug 🐞, give a advice or create a pull request 🙋‍♂️**


Thanks 🤝
=========

* Helo used aiomysql_ and was inspired by peewee_ in programming. Thank you very much for both!
* Please feel free to ⭐️ this repository if this project helped you 😉 !

.. _quart: https://github.com/pgjones/quart
.. _wiki: https://github.com/at7h/helo/wiki/Helo-quick-start-guide
.. _English: https://github.com/at7h/helo
.. _简体中文: https://github.com/at7h/helo/blob/master/README.CN.rst
.. _quickstart: https://github.com/at7h/helo/wiki/Helo-quick-start-guide
.. _installation: https://github.com/at7h/helo/wiki#installation
.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _peewee: https://github.com/coleifer/peewee
