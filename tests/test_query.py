import datetime

from helo import _builder, JOINTYPE, F, SQL

from .case import Author, Post, Column, Employee


class TestImportantQueries:

    def as_query(self, node):
        return node.__query__()

    def test_select(self):
        query = Author.select(
            Author.id, Author.name, F.COUNT(Post.id).as_('pct')
        ).join(
            Post, on=(Author.id == Post.author)
        ).where(
            Author.name == 'at7h',
            Author.id > 3
        ).order_by(
            Author.id.desc()
        ).limit(100).offset(1)
        assert self.as_query(query) == _builder.Query(
            'SELECT `t1`.`id`, `t1`.`name`, COUNT(`t2`.`id`) AS `pct` '
            'FROM `author` AS `t1` '
            'INNER JOIN `post` AS `t2` ON (`t1`.`id` = `t2`.`author`) '
            'WHERE ((`t1`.`name` = %s) AND (`t1`.`id` > %s)) '
            'ORDER BY `t1`.`id` DESC  LIMIT 100 OFFSET 1;',
            params=['at7h', 3]
        )

        query = Author.select(
            Author.id,
            Author.name.as_('username'),
            Author.password,
            F.COUNT(Post.id).as_('pct')
        ).join(
            Post, JOINTYPE.LEFT, on=(Post.author == Author.id)
        ).group_by(
            Author.id, Author.name
        ).having(
            Author.id.nin_([1, 2, 3])
        )
        assert self.as_query(query) == _builder.Query(
            'SELECT `t1`.`id`, `t1`.`name` AS `username`, `t1`.`password`,'
            ' COUNT(`t2`.`id`) AS `pct` '
            'FROM `author` AS `t1` '
            'LEFT JOIN `post` AS `t2` ON (`t1`.`id` = `t2`.`author`) '
            'GROUP BY `t1`.`id`, `t1`.`name` '
            'HAVING (`t1`.`id` NOT IN %s);',
            params=[(1, 2, 3)]
        )

        query = Post.select(F.COUNT(SQL('1'))).where(
            Post.created > datetime.datetime(2019, 10, 10),
            Post.is_deleted == 0
        ).limit(1)
        assert self.as_query(query) == _builder.Query(
            'SELECT COUNT(1) FROM `post` AS `t1` WHERE ((`t1`.`created` > %s)'
            ' AND (`t1`.`is_deleted` = %s)) LIMIT 1;',
            params=[datetime.datetime(2019, 10, 10, 0, 0), 0]
        )

        query = Author.select().where(
            Author.id.in_(
                Post.select(Post.author).where(
                    Post.author
                ).where(Post.id.between(10, 100))
            )
        ).order_by(
            Author.id.desc()
        ).limit(100)
        assert self.as_query(query) == _builder.Query(
            'SELECT * FROM `author` AS `t1` WHERE (`t1`.`id` '
            'IN (SELECT `t2`.`author` FROM `post` AS `t2` WHERE '
            '(`t2`.`id` BETWEEN %s AND %s))) ORDER BY '
            '`t1`.`id` DESC  LIMIT 100;',
            params=[10, 100]
        )

    def test_insert(self):
        query = Column.insert(name='c1')
        assert self.as_query(query) == _builder.Query(
            'INSERT INTO `column` (`name`) VALUES (%s);',
            params=['c1']
        )
        query = Post.insert(name='p1')
        assert self.as_query(query) == _builder.Query(
            'INSERT INTO `post` (`name`, `author`, `column`, `is_deleted`, `created`)'
            ' VALUES (%s, %s, %s, %s, %s);',
            params=['p1', 0, 0, 0, datetime.datetime(2019, 10, 10)]
        )
        q1 = Author.insert(name='u1', password='xxxx')
        q2 = Author.insert({'name': 'u1', 'password': 'xxxx'})
        assert self.as_query(q1) == self.as_query(q2) == _builder.Query(
            'INSERT INTO `author` (`name`, `password`) VALUES (%s, %s);',
            params=['u1', 'xxxx']
        )

    def test_minsert(self):
        q1 = Author.minsert([
            {'name': 'n1', 'password': 'p1'},
            {'name': 'n2', 'password': 'p2'},
            {'name': 'n3', 'password': 'p3'},
            {'name': 'n4', 'password': 'p4'},
        ])
        q2 = Author.minsert(
            [('n1', 'p1'),
                ('n2', 'p2'),
                ('n3', 'p3'),
                ('n4', 'p4')],
            columns=[Author.name, Author.password]
        )
        assert self.as_query(q1) == self.as_query(q2) == _builder.Query(
            'INSERT INTO `author` (`name`, `password`) VALUES (%s, %s);',
            params=[('n1', 'p1'), ('n2', 'p2'), ('n3', 'p3'), ('n4', 'p4')]
        )

    def test_insert_from(self):
        select = Employee.select(
            Employee.id, Employee.name
        ).where(Employee.id > 10)
        q1 = Author.insert_from(select, [Author.id, Author.name])
        q2 = Author.insert_from(select, ['id', 'name'])
        assert self.as_query(q1) == self.as_query(q2) == _builder.Query(
            'INSERT INTO `author` (`id`, `name`) SELECT `t1`.`id`, '
            '`t1`.`name` FROM `employee` AS `t1` WHERE (`t1`.`id` > %s);',
            params=[10]
        )

    def test_replace(self):
        q1 = Author.replace(name='at7h', password='7777')
        q2 = Author.replace({'name': 'at7h', 'password': '7777'})
        assert self.as_query(q1) == self.as_query(q2) == _builder.Query(
            'REPLACE INTO `author` (`id`, `name`, `password`) '
            'VALUES (%s, %s, %s);',
            params=[None, 'at7h', '7777']
        )

    def test_mreplace(self):
        q1 = Author.mreplace([
            {'name': 'n1', 'password': 'p1'},
            {'name': 'n2', 'password': 'p2'},
            {'name': 'n3', 'password': 'p3'},
            {'name': 'n4', 'password': 'p4'},
        ])
        q2 = Author.mreplace(
            [('n1', 'p1'),
                ('n2', 'p2'),
                ('n3', 'p3'),
                ('n4', 'p4')],
            columns=[Author.name, Author.password]
        )
        assert self.as_query(q1) == self.as_query(q2) == _builder.Query(
            'REPLACE INTO `author` (`id`, `name`, `password`) '
            'VALUES (%s, %s, %s);',
            params=[
                (None, 'n1', 'p1'), (None, 'n2', 'p2'),
                (None, 'n3', 'p3'), (None, 'n4', 'p4')
            ])

    def test_update(self):
        query = Post.update(author=2, name='p1').where(
            (Post.author.in_(
                Author.select(Author.id).where(Author.name.startswith('at'))
            ) | Post.column.nin_(
                Column.select(Column.id).where(Column.id >= 100)
            ))
        )
        assert self.as_query(query) == _builder.Query(
            'UPDATE `post` SET `author` = %s, `name` = %s WHERE '
            '((`author` IN (SELECT `t1`.`id` FROM `author` AS `t1` WHERE '
            '(`t1`.`name` LIKE %s))) OR (`t2`.`column` NOT IN '
            '(SELECT `t3`.`id` FROM `column` AS `t3` WHERE '
            '(`t3`.`id` >= %s))));',
            params=[2, 'p1', "at%", 100]
        )

    def test_update_from(self):
        query = Author.update(
            name=Post.name, create_at=Post.created
        ).from_(
            Post
        ).where(
            (Author.id == Post.author) | (Post.name == 'xxxx')
        )
        assert self.as_query(query) == _builder.Query(
            'UPDATE `author` SET `name` = `post`.`name`, '
            '`create_at` = `post`.`created` FROM `post` '
            'WHERE ((`author`.`id` = `post`.`author`) OR '
            '(`post`.`name` = %s));',
            params=['xxxx']
        )

    def test_delete(self):
        query = Post.delete().where(
            Post.author << (
                Author.select(Author.id).where(
                    Author.name.like('at')
                )
            )
        )
        assert self.as_query(query) == _builder.Query(
            'DELETE FROM `post` WHERE (`author` IN '
            '(SELECT `t1`.`id` FROM `author` AS `t1` '
            'WHERE (`t1`.`name` LIKE %s)));',
            params=['at']
        )
