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


**Trod** is a simple asynchronous(asyncio) Python ORM.
Now it supports only MySQL and uses aiomysql_ as the access 'driver' for the database.

* Requires: Python 3.7+


Installation
------------

.. code-block:: console

    pip install trod


Simple Example
-------------

.. code-block:: python

    import asyncio

    from trod import Trod, Model, types


    class User(Model):

        id = types.Auto()
        name = types.VarChar(length=45, comment='user nickname')
        password = types.VarChar(length=100)
        create_at = types.Timestamp(default=types.ON_CREATE)
        update_at = types.Timestamp(default=types.ON_UPDATE)


    async show_case():

        db = Trod()

        async with db.Binder('mysql://user:password@host:port/db'):

            await User.create()

            user = User(name='at7h', password='123456')
            user = await User.get((await user.save()).last_id)
            print(user.password)  # 123456

            await User.insert(name='guax', password='654321').do()

            async for user in User:
                if user.name == 'at7h':
                    assert user.name == '123456'

            user = await User.select().order_by(User.create_at.desc()).first()
            print(user.password) # 654321


    asyncio.run(show_case())


About
-----

* Strictly, trod is not an ORM, it just working in an ORM-like mode. 
  The objects in trod is completely isolated from the data in the database. 
  It is only a Python object in memory, changing it does not affect the database. 
  You must explicitly execute the commit request to the database.

* Trod uses model and object APIs to compose SQL statements and submit 
  them to the database when executed. When loaded, the data is retrieved 
  from the database and then packaged into objects. 
  Of course, you can also choose other data loading methods.

Author at7h is a junior Pythoner, and trod has a lot of temporary 
solutions to optimize and continue to add new features, this is just the beginning 💪.

Welcome your issues and pull requests.


.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _QuickStart: https://github.com/acthse/trod/blob/master/docs/doc.md
