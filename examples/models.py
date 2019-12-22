from trod import Model, types


class Person(Model):
    id = types.BigAuto()
    name = types.VarChar(length=45, null=False)


class Employee(Person):
    department = types.Smallint()
    salary = types.Float(default=0)


class User(Person):
    email = types.Email(default='')
    password = types.VarChar(length=100, null=False)
    create_at = types.Timestamp(default=types.ON_CREATE)

    class Meta:
        indexes = [
            types.K('idx_ep', ['email', 'password']),
        ]


class Post(Model):
    id = types.Auto(comment='auto increment pk')
    title = types.VarChar(length=100)
    content = types.Text(encoding=types.ENCODING.utf8mb4)
    author = types.Int(default=0)
    create_at = types.Timestamp(default=types.ON_CREATE)
    update_at = types.Timestamp(default=types.ON_UPDATE)

    class Meta:
        indexes = [types.K('idx_title', 'title')]
