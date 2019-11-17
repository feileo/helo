====
trod
====

.. image:: https://img.shields.io/pypi/v/trod.svg
        :target: https://pypi.python.org/pypi/trod

.. image:: https://travis-ci.org/at7h/trod.svg?branch=master
    :target: https://travis-ci.org/at7h/trod

.. image:: https://coveralls.io/repos/github/at7h/trod/badge.svg?branch=master
        :target: https://coveralls.io/github/at7h/trod?branch=master

.. image:: https://img.shields.io/github/license/at7h/trod   
        :target: https://img.shields.io/github/license/at7h/trod
        :alt: GitHub


**Trod** is a simple asynchronous ORM using the asyncio_ (PEP-3156/tulip) framework.
Now it supports only MySQL and uses aiomysql_ as the access 'driver' for the database.

* Requires: Python 3.7+


Installation
------------

.. code-block:: console

    pip install trod


Base Examples
-------------

First, let's create a model:

.. code-block:: python
    
    from trod import Model, types


    class User(Model):

        id = types.Auto()
        name = types.VarChar(length=45, comment='nickname')
        password = types.VarChar(length=100, default='')
        create_at = types.Timestamp(default=types.ON_CREATE)
        update_at = types.Timestamp(default=types.ON_UPDATE)


Connect to the database and create tables: 

.. code-block:: python

    from trod import Trod


    db = Trod()

    # In fact, a connection pool was created
    await db.bind('mysql://user:password@host:port/db')
    await db.create_tables([User])


Create and retrieve row data in a table as a shortcut:

.. code-block:: python

    user = User(name='at7h', password='7777')
    # Save it, and get by user id
    user = User.get(await user.save())
    print(user.id, user.name) 
    # 1 at7h

    # Add another row
    await User.add({'name': 'bobo', 'password': '8888'})

    # Get all
    users = [user async for user in User]:
    print(users)
    # [<User object> at 1, <User object> at 2]


More commonly, use the direct translation of the SQL statement(DQL, DML) API.
And You must explicitly execute them via the do() method.

.. code-block:: python

    ret = await User.insert(name='guax', password='9999').do()

    await User.update(password='0000').where(User.id == ret.last_id).do()

    user = await User.select().order_by(User.create_at.desc()).first()
    print(user.name) # guax

    users = await User.select().where(User.name.startswith('at')).all()
    print(users)
    # [<User object> at 1]


About
-----

* Trod is like a newborn baby, and it currently has a lot of missing 
  features and temporary solutions, waiting for us to supplement and 
  optimize. Anyway, this is just the beginning.

* Any kind of contribution is expected: report a bug, give a advice or create a pull request.


TODO
----

* Documents
* Join And Relationship


.. _asyncio: https://docs.python.org/3.7/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _QuickStart: https://github.com/acthse/trod/blob/master/docs/doc.md
