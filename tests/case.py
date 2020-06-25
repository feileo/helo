import datetime

import pytz

import helo

TZ = pytz.timezone('Asia/Shanghai')


class People(helo.Model):

    id = helo.Auto()
    name = helo.VarChar(length=45)
    gender = helo.Tinyint(length=1, unsigned=True)
    age = helo.Tinyint(unsigned=True)
    create_at = helo.Timestamp(default=helo.ON_CREATE)
    update_at = helo.Timestamp(default=helo.ON_UPDATE)

    class Meta:
        indexes = [helo.K('idx_name', 'name')]


class Employee(People):

    salary = helo.Float()
    departmentid = helo.Int()
    phone = helo.VarChar(default='')
    email = helo.Email(length=100, default='')

    class Meta:
        indexes = [helo.K('idx_age_salary', ['age', 'salary'])]


class User(People):

    nickname = helo.VarChar(length=100)
    password = helo.VarChar(name='pwd')
    role = helo.Int(default=0)
    lastlogin = helo.DateTime(default=datetime.datetime.now, name='loginat')

    class Meta:
        db = 'helo'
        name = 'user_'
        indexes = (
            helo.K('idx_name', 'name'),
            helo.UK('unidx_nickname', 'nickname')
        )


class Role(helo.Model):

    id = helo.Int(primary_key=True, auto=True)
    name = helo.VarChar(length=50)
    is_deleted = helo.Bool(default=False)
    create_at = helo.Timestamp(default=helo.ON_CREATE)
    update_at = helo.Timestamp(default=helo.ON_UPDATE)

    class Meta:
        name = 'role_'


class Author(helo.Model):

    id = helo.Auto()
    name = helo.VarChar(length=45, comment='username')
    password = helo.VarChar(length=100)
    create_at = helo.Timestamp(default=helo.ON_CREATE)
    update_at = helo.Timestamp(default=helo.ON_UPDATE)


class Column(helo.Model):

    id = helo.Auto()
    name = helo.Char(length=100)
    create_at = helo.Timestamp(default=helo.ON_CREATE)


class Post(helo.Model):

    id = helo.Int(primary_key=True, auto=True)
    name = helo.VarChar(length=100)
    author = helo.Int(default=0)
    column = helo.Int(default=0)
    is_deleted = helo.Tinyint(default=0)
    created = helo.DateTime(default=datetime.datetime(2019, 10, 10))

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.name)


def deltanow(add):
    if add > 0:
        return datetime.datetime.now(TZ)+datetime.timedelta(minutes=add)
    return datetime.datetime.now(TZ)-datetime.timedelta(minutes=add)
