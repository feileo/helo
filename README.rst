====
trod
====
.. image https://img.shields.io/badge/language-python3-orange.svg

Trod is a very simple asynchronous Python ORM based on asyncio. 
Currently it only supports mysql and uses aiomysql as the database access driver.

* Strictly, trod is not an orm, just working in orm mode. The objects in trod 
  are completely isolated from the data in the database. It is only a Python object 
  in memory. Changing it does not affect the database. To change the database, 
  you must explicitly submit the request to the database.

* Trod simply uses the model and the object and its API to form the SQL statement, 
  which is submitted to the database for change when executed. When loading, 
  the data is taken from the database and then wrapped into objects.

Installation
------------

.. code-block:: console

    pip install trod


Documentation
-------------

* Quick start

Basic Example
--------

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

        db.bind('mysql://user:password@host:port/db')

        # create_table
        await User.create()

        # add a user
        user = User(id=1,name='name',password='123456')
        user_id = await User.add(user)
        print(user_id)  # 1

        # get a user by id
        user = User.get(user_id)
        print(user.password)  # 123456

        # update user password
        await User.update(dict(password=654321), User.name == user.name)
        user = User.get(user_id)
        print(user.password)  # 654321

        # delete a user
        await User.remove(User.id == user.id) 

        # query
        users = [
            User(id=2,name='zs',password='222222')
            User(id=3,name='ls',password='333333')
        ]
        User.batch_add(users)
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

Requirements
------------
* Python 3.6+
* asyncio
* asyncinit
