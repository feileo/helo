import asyncio
import datetime

import helo


db = helo.G()


class Person(helo.Model):
    id = helo.BigAuto()
    name = helo.VarChar(length=45, null=False)


class Employee(Person):
    department = helo.Smallint()
    salary = helo.Float(default=0)


class User(Person):
    email = helo.Email(default='')
    password = helo.VarChar(length=100, null=False)
    create_at = helo.Timestamp(default=helo.ON_CREATE)

    class Meta:
        indexes = [helo.K('idx_ep', ['email', 'password'])]


class Post(helo.Model):
    id = helo.Auto(comment='auto increment pk')
    title = helo.VarChar(length=100)
    content = helo.Text(encoding=helo.ENCODING.UTF8MB4)
    author = helo.Int(default=0)
    create_at = helo.Timestamp(default=helo.ON_CREATE)
    update_at = helo.Timestamp(default=helo.ON_UPDATE)

    class Meta:
        indexes = [
            helo.K('idx_title', 'title'),
            helo.K('idx_author', 'author'),
        ]


async def basic_example1():
    # Creating a connection pool, see `helo.db.Pool`
    await db.bind('mysql://user:pwd@127.0.0.1:3306/db')
    print(db.isbound)
    # True
    print(db.state)
    # {'minsize': 1, 'maxsize': 15, 'size': 1, 'freesize': 1}

    await db.create_tables([User, Employee, Post])

    # CRUD
    await ex_for_short()
    await ex_for_dml()
    await ex_for_dql()
    await ex_for_instance()

    await db.unbind()


async def basic_example2():
    async with db.binder():
        async for post in Post:
            print(post)
        # <Post object at 1>
        # <Post object at 2>
        # <Post object at 3>
        # <Post object at 4>
        # <Post object at 5>

        users = User.select().where(User.id < 5).order_by(User.id.desc())
        async for user in users:
            print(user)
        # <User object at 4>
        # <User object at 3>
        # <User object at 2>
        # <User object at 1>

        await db.drop_tables([User, Employee, Post])


async def ex_for_short():
    """ Simple API for short """

    # Adding a user
    user_id = await User.add(name='at7h', password='1111')
    assert user_id == 1
    user_id = await User.add({'name': 'bobo', 'password': '2222'})
    assert user_id == 2

    # Adding multiple using values dict list
    users = [
        {'name': 'mingz', 'password': '3333'},
        {'name': 'xy69z', 'password': '4444'},
    ]
    assert (await User.madd(users)) == len(users)
    # Or user object list
    users = [
        User(name='mejor', password='5555'),
        User(name='gaver', password='6666'),
    ]
    assert (await User.madd(users)) == len(users)

    # Getting a user
    user = await User.get(1)
    assert isinstance(user, User)
    assert user.name == 'at7h'
    user = await User.get(User.name == user.name)
    assert user.name == 'at7h'

    # Getting multiple
    uid_list = [1, 2, 3]
    users = await User.mget(uid_list)
    assert users.count == 3
    print(users)
    # [<User object at 1>, <User object at 2>, <User object at 3>]

    # Specify columns
    users = await User.mget(uid_list, columns=[User.id, User.name])
    assert users[0].password is None

    # Or by query
    users = await User.mget((User.id < 2) | (User.name == 'mingz'))
    print(users)
    # [<User object at 1>, <User object at 3>]

    # Setting the value of a row with the primary key
    email = 'z@hello.com'
    await User.set(1, email=email)
    user = await User.get(1)
    assert user.email == email


async def ex_for_dml():
    """API that translates directly from SQL statements"""

    # Inserting a row
    ret = await User.insert(name='poper', password='7777').do()
    assert ret.affected == 1
    assert ret.last_id == 7
    print(ret)  # (1, 7)
    # Or
    ret = await User.insert({'name': 'bingo', 'password': '8888'}).do()
    assert ret.last_id == 8
    print(ret)  # (1, 8)

    # Inserting multiple
    employees = [
        {'name': 'at7h', 'department': 1},
        {'name': 'bobo', 'department': 2},
        {'name': 'zogu', 'department': 3},
        {'name': 'yaya', 'department': 4},
    ]
    ret = await Employee.minsert(employees).do()
    print(ret)  # (4, 1)
    posts = [
        {'title': 'post1', 'author': 1},
        {'title': 'post2', 'author': 2},
    ]
    ret = await Post.minsert(posts).do()
    print(ret)  # (2, 1)

    # Specify row tuples columns the tuple values correspond to
    posts = [
        ('post3', 3),
        ('post4', 4),
        ('post5', 1),
    ]
    ret = await Post.minsert(
        posts, columns=[Post.title, Post.author]
    ).do()
    print(ret)  # (2, 3)

    # Inserting from select clause
    select = User.select(User.name).where(
        User.id < 3
    )
    ret = await Employee.insert_from(select, [Employee.name]).do()
    print(ret)  # (2, 5)

    # Updating
    ret = await Post.update(title='Python orm helo').where(
        Post.author == 1).do()
    assert ret.affected == 2
    ret = await Employee.update(salary=14000).where(
        Employee.name == 'at7h'
    ).do()
    assert ret.affected == 2
    # Pay rise 😄
    ret = await Employee.update(
        salary=Employee.salary + 1000
    ).where(
        (Employee.department.in_([1, 2])) | (Employee.name.endswith('zz'))
    ).do()
    assert ret.affected == 2

    # Deleting
    ret = await Post.delete().where(
        Post.create_at < datetime.datetime(2019, 1, 1)
    ).limit(
        100
    ).do()


async def ex_for_dql():
    """
    Expressive and composable queries
    Using it as if it were SQL
    """

    users = await User.select().limit(3).offset(2).all()
    print(users)
    # [<User object at 3>, <User object at 4>, <User object at 5>]

    # Row type using `helo.util.adict`, not the `Model`
    users = await User.select(User.id, User.name).limit(2).all(wrap=False)
    print(users)
    # [{'id': 1, 'name': 'at7h'}, {'id': 2, 'name': 'bobo'}]
    assert users[0].name == 'at7h'

    employee = await Employee.select().order_by(
        Employee.salary.desc()
    ).first(False)
    print(employee)
    # {'id': 1, 'name': 'at7h', 'department': 1, 'salary': 15000.0}

    salary_sum = await Employee.select(
        helo.F.SUM(Employee.salary).as_('salary_sum')
    ).scalar()
    print(salary_sum)  # 30000.0

    posts = await Post.select().where(
        Post.create_at >= datetime.datetime(2019, 7, 1),
        Post.title.contains('Python')
    ).order_by(
        Post.create_at.desc()
    ).paginate(2, 10)
    print(posts)  # []

    users = await User.select(
        User.id, User.name
    ).join(
        Employee, on=(User.name == Employee.name)
    ).where(
        Employee.salary >= 1000
    ).order_by(
        User.id.desc()
    ).all(wrap=False)
    assert users.count == 3
    # Or
    users = await User.select(
        User.id, User.name
    ).where(
        User.name.in_(
            Employee.select(Employee.name).where(
                Employee.salary >= 1000
            )
        )
    ).order_by(
        User.id.desc()
    ).all()
    assert users.count == 2

    user_posts = await User.select(
        User.name, helo.F.COUNT(helo.SQL('1')).as_('posts')
    ).join(
        Post, helo.JOINTYPE.LEFT, on=(User.id == Post.author)
    ).group_by(
        User.name
    ).rows(100)
    print(user_posts.count)
    # 8
    print(user_posts[0])
    # {'name': 'at7h', 'posts': 2}


async def ex_for_instance():
    """ For `Model` instance """

    # Saving it, Insert if user does not exist, otherwise update
    user = User(name='jaxu', password='9999')
    assert user.id is None
    user_id = await user.save()
    assert user_id == user.id == 9
    # Modifying it
    user.name = 'at8h'
    user.email = 'g@at7h.com'
    user_id = await user.save()
    assert user_id == user.id == 9
    # Removing it
    await user.remove()
    user = await User.get(user_id)
    assert user is None


if __name__ == '__main__':
    asyncio.run(basic_example1())
    asyncio.run(basic_example2())
