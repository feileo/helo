from datetime import datetime, date, time

import pytest

from trod import types as t, model as m, g, db, err


def test_expr():
    age = t.Int(name='age')
    name = t.Char(name='name')
    phone = t.VarChar(name='phone')

    e = (age > 10) | (name == 'test')
    assert g.parse(e) == g.Query(
        '((`age` > %s) OR (`name` = %s));', (10, 'test')
    )
    e = (name == 'test') | (age > 10)
    assert g.parse(e) == g.Query(
        '((`name` = %s) OR (`age` > %s));', ('test', 10)
    )

    e = age + 1
    assert g.parse(e) == g.Query(
        '(`age` + %s);', (1,)
    )
    e = 1 + age
    assert g.parse(e) == g.Query(
        '(%s + `age`);', (1,)
    )

    e = age + '20'
    assert g.parse(e) == g.Query(
        '(`age` + %s);', (20,)
    )
    e = 20 + age
    assert g.parse(e) == g.Query(
        '(%s + `age`);', (20,)
    )

    e = age * '2'
    assert g.parse(e) == g.Query(
        '(`age` * %s);', (2,)
    )
    e = 2 * age
    assert g.parse(e) == g.Query(
        '(%s * `age`);', (2,)
    )

    e = 1000 / age
    assert g.parse(e) == g.Query(
        '(%s / `age`);', (1000,)
    )
    e = age / 2
    assert g.parse(e) == g.Query(
        '(`age` / %s);', (2,)
    )

    e = age ^ name
    assert g.parse(e) == g.Query(
        '(`age` # `name`);', ()
    )
    e = name ^ age
    assert g.parse(e) == g.Query(
        '(`name` # `age`);', ()
    )

    e = name == 'at7h'
    assert g.parse(e) == g.Query(
        '(`name` = %s);', ('at7h',)
    )
    e = name != 'at7h'
    assert g.parse(e) == g.Query(
        '(`name` != %s);', ('at7h',)
    )
    e = name <= 'at7h'
    assert g.parse(e) == g.Query(
        '(`name` <= %s);', ('at7h',)
    )
    e = name >= 'at7h'
    assert g.parse(e) == g.Query(
        '(`name` >= %s);', ('at7h',)
    )
    e = age < 90
    assert g.parse(e) == g.Query(
        '(`age` < %s);', (90,)
    )
    e = age > 20
    assert g.parse(e) == g.Query(
        '(`age` > %s);', (20,)
    )
    e = name >> None
    assert g.parse(e) == g.Query(
        '(`name` IS %s);', (None,)
    )
    e = name << ['at7h', 'mejer']
    assert g.parse(e) == g.Query(
        '(`name` IN %s);', (('at7h', 'mejer'),)
    )
    e = name % 'at'
    assert g.parse(e) == g.Query(
        '(`name` LIKE BINARY %s);', ('at',)
    )
    e = name ** 'at'
    assert g.parse(e) == g.Query(
        '(`name` LIKE %s);', ('at',)
    )
    e = age[slice(20, 30)]
    assert g.parse(e) == g.Query(
        '(`age` BETWEEN %s AND %s);', (20, 30,)
    )

    e = name.concat(10)
    assert g.parse(e) == g.Query(
        '(`name` || %s);', ('10',)
    )
    e = name.binand('at7h')
    assert g.parse(e) == g.Query(
        '(`name` & %s);', ('at7h',)
    )
    e = name.binor('at7h')
    assert g.parse(e) == g.Query(
        '(`name` | %s);', ('at7h',)
    )
    e = name.in_(['at7h', 'mejor'])
    assert g.parse(e) == g.Query(
        '(`name` IN %s);', (('at7h', 'mejor'),)
    )
    e = name.nin_(['at7h', 'mejor'])
    assert g.parse(e) == g.Query(
        '(`name` NOT IN %s);', (('at7h', 'mejor'),)
    )
    e = name.exists(['at7h', 'mejor'])
    assert g.parse(e) == g.Query(
        '(`name` EXISTS %s);', (('at7h', 'mejor'),)
    )
    e = name.nexists(['at7h', 'mejor'])
    assert g.parse(e) == g.Query(
        '(`name` NOT EXISTS %s);', (('at7h', 'mejor'),)
    )
    e = name.isnull()
    assert g.parse(e) == g.Query(
        '(`name` IS %s);', (None,)
    )
    e = name.isnull(False)
    assert g.parse(e) == g.Query(
        '(`name` IS NOT %s);', (None,)
    )
    e = name.regexp('at.*')
    assert g.parse(e) == g.Query(
        '(`name` REGEXP %s);', ('at.*',)
    )
    e = name.regexp('at.*', i=False)
    assert g.parse(e) == g.Query(
        '(`name` REGEXP BINARY %s);', ('at.*',)
    )
    e = phone.like(177)
    assert g.parse(e) == g.Query(
        '(`phone` LIKE %s);', ('177',)
    )
    e = phone.like(177, i=False)
    assert g.parse(e) == g.Query(
        '(`phone` LIKE BINARY %s);', ('177',)
    )
    e = phone.contains(7867)
    assert g.parse(e) == g.Query(
        '(`phone` LIKE %s);', ('%7867%',)
    )
    e = phone.contains(7867, i=False)
    assert g.parse(e) == g.Query(
        '(`phone` LIKE BINARY %s);', ('%7867%',)
    )
    e = name.endswith('7h')
    assert g.parse(e) == g.Query(
        '(`name` LIKE %s);', ('%7h',)
    )
    e = name.endswith('7h', i=False)
    assert g.parse(e) == g.Query(
        '(`name` LIKE BINARY %s);', ('%7h',)
    )
    e = name.startswith('at')
    assert g.parse(e) == g.Query(
        '(`name` LIKE %s);', ('at%',)
    )
    e = name.startswith('at', i=False)
    assert g.parse(e) == g.Query(
        '(`name` LIKE BINARY %s);', ('at%',)
    )
    e = age.between(10, 30)
    assert g.parse(e) == g.Query(
        '(`age` BETWEEN %s AND %s);', (10, 30)
    )
    e = age.nbetween(10, 30)
    assert g.parse(e) == g.Query(
        '(`age` NOT BETWEEN %s AND %s);', (10, 30)
    )
    e = age.asc()
    assert g.parse(e) == g.Query(
        '`age` ASC ;', ()
    )
    e = age.desc()
    assert g.parse(e) == g.Query(
        '`age` DESC ;', ()
    )
    e = age.as_('a')
    assert g.parse(e) == g.Query(
        '`age` AS `a` ;', ()
    )

    e = (age > 10) & (name == 'test')
    assert g.parse(e) == g.Query(
        '((`age` > %s) AND (`name` = %s));', (10, 'test')
    )
    e = (name == 'test') & (age > 10)
    assert g.parse(e) == g.Query(
        '((`name` = %s) AND (`age` > %s));', ('test', 10)
    )

    e = (age >= '20') & name.in_(['at7h', 'mejor']) | phone.startswith('153')
    assert g.parse(e) == g.Query(
        '(((`age` >= %s) AND (`name` IN %s)) OR (`phone` LIKE %s));',
        (20, ('at7h', 'mejor'), '153%')
    )


def test_tinyint():
    tinyint = t.Tinyint()
    try:
        assert tinyint.column
        assert tinyint.__def__()
        assert False, "Should be raise NoColumnNameError"
    except err.NoColumnNameError:
        pass
    tinyint = t.Tinyint(name="ty")
    assert tinyint.column == "`ty`"
    assert str(tinyint) == "`ty` tinyint(4) DEFAULT NULL;"

    tinyint = t.Tinyint(name='ty', length=3, null=False, default=1, comment="ty")
    assert str(tinyint) == "`ty` tinyint(3) NOT NULL DEFAULT '1' COMMENT 'ty';"

    tinyint = t.Tinyint(name='ty', unsigned=True, default=1, comment="ty")
    assert str(tinyint) == "`ty` tinyint(4) unsigned DEFAULT '1' COMMENT 'ty';"

    tinyint = t.Tinyint(name='ty', zerofill=True)
    assert str(tinyint) == f"`ty` tinyint(4) zerofill DEFAULT NULL;"

    assert tinyint.py_value('1') == 1
    assert tinyint.db_value('11') == 11

    try:
        assert isinstance(tinyint.db_value('1x'), int)
        assert False, "Should be raise ValueError"
    except ValueError:
        pass


def test_smllint():
    smallint = t.Smallint(name='sl', comment="sl")
    assert smallint.py_value('1') == 1
    assert smallint.db_value('2') == 2
    assert str(smallint) == "`sl` smallint(6) DEFAULT NULL COMMENT 'sl';"
    smallint = t.Smallint(
        name='sl', length=4, unsigned=True, zerofill=True, null=False, default=1, comment="sl"
    )
    assert str(smallint) == "`sl` smallint(4) unsigned zerofill NOT NULL DEFAULT '1' COMMENT 'sl';"


def test_int():
    int_ = t.Int(name='int', comment="int")
    assert str(int_) == "`int` int(11) DEFAULT NULL COMMENT 'int';"
    try:
        int_ = t.Int(name='int', default='xxx')
        assert str(int_)
        assert False, "Should be raise TypeError"
    except TypeError:
        pass
    try:
        int_ = t.Int(name='int', primary_key=True, default=1)
        int_ = t.Int(name='int', auto=True)
        assert False, "Should be raise ProgrammingError"
    except err.ProgrammingError:
        pass


def test_bigint():
    bigint = t.Bigint(name='bigint')
    assert str(bigint) == "`bigint` bigint(20) DEFAULT NULL;"
    bigint = t.Bigint(name='bigint', length=18, null=False, default=1)
    assert str(bigint) == "`bigint` bigint(18) NOT NULL DEFAULT '1';"
    try:
        bigint = t.Bigint(name='bigint', primary_key=True, default=1)
        bigint = t.Bigint(name='bigint', auto=True)
        assert False, "Should be raise ProgrammingError"
    except err.ProgrammingError:
        pass
    bigint = t.Bigint(name='bigint', primary_key=True, auto=True)
    assert str(bigint) == "`bigint` bigint(20) NOT NULL AUTO_INCREMENT;"


def test_auto():
    auto = t.Auto(name='auto')
    assert str(auto) == "`auto` int(11) NOT NULL AUTO_INCREMENT;"


def test_bigauto():
    bigauto = t.BigAuto(name='bigauto')
    assert str(bigauto) == "`bigauto` bigint(20) NOT NULL AUTO_INCREMENT;"


def test_bool():
    bool_ = t.Bool(name='bool', null=False, default=True)
    assert str(bool_) == "`bool` bool NOT NULL DEFAULT '1';"
    assert bool_.db_value(0) is False
    assert bool_.py_value(1) is True


def test_float():
    float_ = t.Float(name='float', null=False, default=43.54, length=7)
    assert str(float_) == "`float` float(7) NOT NULL DEFAULT '43.54';"
    float_ = t.Float(name='float', length=(4, 3), default=100.0, unsigned=True)
    assert str(float_) == "`float` float(4,3) unsigned DEFAULT '100.0';"
    try:
        float_ = t.Float(name='float', length=(4, 3, 5))
        float_ = t.Float(name='float', length='7')
        assert False, 'Should be raise TypeError'
    except TypeError:
        pass


def test_double():
    double = t.Double(name='double', null=False, length=7, default=43.54)
    assert str(double) == "`double` double(7) NOT NULL DEFAULT '43.54';"
    double = t.Double(name='double', length=(4, 3), unsigned=True)
    assert str(double) == "`double` double(4,3) unsigned DEFAULT NULL;"
    assert double.py_value(None) is None
    assert double.py_value(0) == 0.0
    try:
        double = t.Double(name='double', length=(4, 3, 5))
        double = t.Double(name='double', length='7')
        assert False, 'Should be raise TypeError'
    except TypeError:
        pass


def test_decimal():
    import decimal as d
    decimal = t.Decimal(name='decimal')
    assert str(decimal) == "`decimal` decimal(10,5) DEFAULT NULL;"
    decimal = t.Decimal(name='decimal', length=(6, 3), default=d.Decimal(str(43.54)))
    assert str(decimal) == "`decimal` decimal(6,3) DEFAULT '43.54';"
    assert decimal.py_value(None) is None
    assert decimal.py_value(0) == d.Decimal(0)
    assert str(decimal.py_value('100.12')) == '100.12'
    assert isinstance(decimal.db_value(10), d.Decimal)
    try:
        decimal = t.Decimal(name='decimal', length=10)
        assert False, 'Should be raise TypeError'
    except TypeError:
        pass


def test_text():
    text = t.Text(name='text', encoding=g.ENCODINGS.utf8mb4)
    assert str(text) == "`text` text CHARACTER SET utf8mb4 NULL;"
    assert hasattr(text, 'default') is False


def test_char():
    char = t.Char(name='char', length=100)
    assert str(char) == "`char` char(100) DEFAULT NULL;"


def test_varchar():
    varchar = t.VarChar(name='varchar', default='c')
    assert str(varchar) == "`varchar` varchar(255) DEFAULT 'c';"
    try:
        varchar = t.VarChar(name='varchar', default=7)
        assert False, 'Should be raise TypeError'
    except TypeError:
        pass


def test_uuid():
    import uuid as uu
    uuid = t.UUID(name='uuid')
    assert str(uuid) == "`uuid` varchar(40) NOT NULL;"
    id_ = str(uu.uuid1())
    assert isinstance(uuid.py_value(id_), uu.UUID)
    assert isinstance(uuid.db_value(uu.uuid1()), str)


def test_date():
    date_ = t.Date(name='date')
    assert str(date_) == "`date` date DEFAULT NULL;"
    date_ = t.Date(name='date', default=datetime.date)
    assert str(date_) == "`date` date DEFAULT NULL;"
    td = date(2019, 10, 1)
    date_ = t.Date(name='date', default=td)
    assert str(date_) == "`date` date DEFAULT '2019-10-01';"
    assert isinstance(date_.py_value(datetime.now()), date)
    assert isinstance(date_.py_value("2019-10-01"), date)
    assert date_.to_str(td) == "2019-10-01"


def test_time():
    time_ = t.Time(name='time')
    assert str(time_) == "`time` time DEFAULT NULL;"
    time_ = t.Time(name='time', default=datetime.time)
    assert str(time_) == "`time` time DEFAULT NULL;"
    td = time(22, 19, 34)
    time_ = t.Time(name='time', default=td)
    assert str(time_) == "`time` time DEFAULT '22:19:34.000000';"
    assert isinstance(time_.py_value(datetime.now()), time)
    assert isinstance(time_.py_value("10:23:23"), time)
    assert time_.to_str(td) == "22:19:34.000000"

    t.Time.formats = ['%H:%M:%S']
    td = time(22, 19, 34)
    time_ = t.Time(name='time', default=td)
    assert str(time_) == "`time` time DEFAULT '22:19:34';"
    assert time_.to_str(td) == "22:19:34"


def test_datetime():
    datetime_ = t.DateTime(name='dt', formats=['%Y-%m-%d %H:%M:%S'])
    assert str(datetime_) == "`dt` datetime DEFAULT NULL;"
    assert isinstance(datetime_.py_value("2019-10-10 10:23:23"), datetime)
    assert datetime_.to_str(datetime(2019, 10, 10, 10, 23, 23)) == "2019-10-10 10:23:23"
    datetime_ = t.DateTime(name='dt', default=datetime.now)
    assert str(datetime_) == "`dt` datetime DEFAULT NULL;"
    assert datetime_.to_str(datetime(2019, 10, 10, 10, 23, 23)) == "2019-10-10 10:23:23.000000"


def test_timestamp():
    timestamp = t.Timestamp(name='ts')
    assert str(timestamp) == "`ts` timestamp NULL DEFAULT NULL;"
    assert isinstance(timestamp.py_value("2019-10-10 10:23:23"), datetime)
    assert timestamp.to_str(datetime(2019, 10, 10, 10, 23, 23)) == "1570674203"
    assert timestamp.db_value(datetime(2019, 10, 10, 10, 23, 23)) == 1570674203
    timestamp = t.Timestamp(name='ts', default=datetime.now)
    assert str(timestamp) == "`ts` timestamp NULL DEFAULT NULL;"


def test_key():
    age = t.Int(name='age')
    name = t.Char(name='name')
    phone = t.VarChar(name='phone')
    key = t.K('idx_name', name)
    assert str(key) == 'KEY `idx_name` (`name`);'
    key = t.K('idx_name_age', (name, age))
    assert str(key) == 'KEY `idx_name_age` (`name`, `age`);'
    key = t.K('idx_name_age', ('name', 'age'))
    assert str(key) == 'KEY `idx_name_age` (`name`, `age`);'
    key = t.UK('uk_phone', phone, comment='phone')
    assert str(key) == "UNIQUE KEY `uk_phone` (`phone`) COMMENT 'phone';"


def test_funs():
    age = t.Int(name='age')
    s = t.F.SUM(age).as_('age_sum')
    assert g.parse(s).sql == 'SUM(`age`) AS `age_sum` ;'

    m_ = t.F.MAX(age).as_('age_max')
    assert g.parse(m_).sql == 'MAX(`age`) AS `age_max` ;'
    try:
        t.F.STR(age).as_('age_str')
        assert False, 'Should be raise RuntimeError'
    except RuntimeError:
        pass


class MT(m.Model):
    __table__ = 'test_types_table'
    __comment__ = 'type case table'
    __auto_increment__ = 7
    __indexes__ = (
        t.K('key', ['tinyint', 'datetime_'], comment='key test'),
        t.UK('ukey', 'varchar', comment='unique key test'),
    )

    id = t.Auto(comment='permary_key')
    tinyint = t.Tinyint(1, unsigned=True, default=0, comment='tinyint')
    smallint = t.Smallint(default=0, comment='smallint')
    int_ = t.Int(unsigned=True, null=False, default=0, comment='int')
    bigint = t.Bigint(45, null=False, default=0, comment='bigint')
    text = t.Text(encoding=g.ENCODINGS.utf8mb4, null=False, comment='text')
    char = t.Char(45, null=False, default='', comment='char')
    varchar = t.VarChar(45, null=False, default='', comment='varchar')
    float_ = t.Float((3, 3), default=0, comment='float')
    double_ = t.Double((4, 4), unsigned=True, default=0, comment='double')
    decimal = t.Decimal((4, 4), unsigned=True, default=0, comment='decimal')
    time_ = t.Time(default=datetime.now, comment='datetime')
    date_ = t.Date(default=datetime.now, comment='datetime')
    datetime_ = t.DateTime(default=datetime.now, comment='datetime')
    now_ts = t.Timestamp(default=datetime.now, comment='now ts')
    created_at = t.Timestamp(default=g.ON_CREATE, comment='created_at')
    updated_at = t.Timestamp(default=g.ON_UPDATE, comment='updated_at')


@pytest.mark.asyncio
async def test_types():

    async with db.BindContext():
        await MT.create()
        assert await MT.show().create_syntax() == (
            "CREATE TABLE `test_types_table` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'permary_key',\n"
            "  `tinyint` tinyint(1) unsigned zerofill DEFAULT NULL COMMENT 'tinyint',\n"
            "  `smallint` smallint(4) DEFAULT NULL COMMENT 'smallint',\n"
            "  `int_` int(11) unsigned NOT NULL COMMENT 'int',\n"
            "  `bigint` bigint(45) NOT NULL COMMENT 'bigint',\n"
            "  `text` text CHARACTER SET utf8mb4 NOT NULL COMMENT 'text',\n"
            "  `char` char(45) NOT NULL COMMENT 'char',\n"
            "  `varchar` varchar(45) NOT NULL COMMENT 'varchar',\n"
            "  `float_` float(3,3) DEFAULT NULL COMMENT 'float',\n"
            "  `double_` double(4,4) unsigned DEFAULT NULL COMMENT 'double',\n"
            "  `decimal` decimal(10,0) DEFAULT NULL COMMENT 'decimal',\n"
            "  `time_` time DEFAULT NULL COMMENT 'datetime',\n"
            "  `date_` date DEFAULT NULL COMMENT 'datetime',\n"
            "  `datetime_` datetime DEFAULT NULL COMMENT 'datetime',\n"
            "  `now_ts` timestamp NULL DEFAULT NULL COMMENT 'now ts',\n"
            "  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'created_at',\n"
            "  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON"
            " UPDATE CURRENT_TIMESTAMP COMMENT 'updated_at',\n"
            "  PRIMARY KEY (`id`),\n"
            "  UNIQUE KEY `ukey` (`varchar`) COMMENT 'unique key test',\n"
            "  KEY `key` (`tinyint`,`datetime_`) COMMENT 'key test'\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='type case table'"
        )
