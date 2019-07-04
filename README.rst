====
trod 
====

.. image:: https://img.shields.io/pypi/v/trod.svg
        :target: https://pypi.python.org/pypi/trod

.. image:: https://travis-ci.org/acthse/trod.svg?branch=master
        :target: https://travis-ci.org/acthse/trod

.. image:: https://codecov.io/gh/acthse/trod/branch/master/graph/badge.svg
        :target: https://codecov.io/gh/acthy/trod

.. image:: https://img.shields.io/pypi/pyversions/trod.svg
        :target: https://img.shields.io/pypi/pyversions/trod.svg

.. image:: https://img.shields.io/pypi/l/trod.svg
        :target: https://img.shields.io/pypi/l/trod.svg

.. image:: https://img.shields.io/static/v1.svg?label=status&message=👨‍💻rewriting&color=brightgreen

🌻 **Trod** is a very simple asynchronous Python ORM based on asyncio_. 
Now it only supports MySQL and uses aiomysql_ as the access 'driver' for the database.

* Strictly, trod is not an ORM, it just working in an ORM-like mode. 
  The objects in trod is completely isolated from the data in the database. 
  It is only a Python object in memory, changing it does not affect the database. 
  You must explicitly execute the commit request to the database.

* Trod only uses model and object APIs to compose SQL statements and submit 
  them to the database when executed. When loaded, the data is retrieved 
  from the database and then packaged into objects.


Installation
------------

.. code-block:: console

    pip install trod


Basic Example
-------------

.. code-block:: python

    import asyncio

    from trod import Trod, And, Auto
    from trod.types import field, index

    db = Trod()

    class User(db.Model):
        __table__ = 'user'
        __comment__ = 'user info'

        id = field.Bigint(length=20, unsigned=True, primary_key=True, comment='primary key')
        name = field.String(length=20, use_varchar=True, allow_null=False, comment='user name')
        password = field.String(length=45, use_varchar=True, comment='password')
        date = field.Datetime(comment='registration time')
        created_at = field.Timestamp(auto=Auto.on_create)
        updated_at = field.Timestamp(auto=Auto.on_update)

        name_idx = index.Key(column='name')

    async show_case():
        """ show some base case """

        await db.bind('mysql://user:password@host:port/db')

        # create_table
        await User.create()

        # add a user
        user = User(id=1,name='name', password='123456')
        user_id = await User.add(user)
        print(user_id)  # 1

        # get a user by id
        user = await User.get(user_id)
        print(user.password)  # 123456

        # update user password
        await User.update(dict(password=654321), User.name == user.name)
        user = await User.get(user_id)
        print(user.password)  # 654321

        # delete a user
        await User.remove(User.id == user.id) 

        # query
        users = [
            User(id=2, name='zs', password='222222')
            User(id=3, name='ls', password='333333')
        ]
        await User.batch_add(users)
        query_users = await User.query().filter(
            User.id.in_([1,2,3])
        ).order_by(User.data).all()
        print(query_users) 
        # [<User(table 'user' : user info)>, <User(table 'user' : user info)>, <User(table 'user' : user info)>] 

        user = await User.query(User.password, User.name).filter(
            And(User.id.in_([1,2,3], User.name == 'ls'))
        ).first()
        print(user.password) # 333333

        await db.unbind()

    asyncio.get_event_loop().run_until_complete(show_case())


About
-----
The author of trod (that's me 😊) is a junior Pythoner, and trod has a lot of temporary 
solutions to optimize and continue to add new features, this is just the beginning 💪.
I will continue later, and welcome your issues and pull requests.


Requirements
------------

* Python 3.6+
* MySQL 5.6.5+

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _QuickStart: https://github.com/acthse/trod/blob/master/docs/doc.md
