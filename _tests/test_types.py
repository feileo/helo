#  type: ignore
#  pylint: disable=too-many-statements
"""
Tests for types module and outside the ``helo.Model``
"""
from datetime import datetime, date, time, timedelta

import pytest

from helo import err, _builder, _helper, types as t, G, Model

db = G()


def test_exprs():
    age = t.Int(name='age')
    name = t.Char(name='name')
    password = t.VarChar(name='password')
    lastlogin = t.DateTime(name='lastlogin', default=datetime.now)

    e = (age > 20) & True
    assert _builder.parse(e) == _builder.Query(
        '((`age` > %s) AND %s);', (20, True)
    )
    e = True & (age > 10)
    assert _builder.parse(e) == _builder.Query(
        '(%s AND (`age` > %s));', (True, 10,)
    )
    e = False | (age > 10)
    assert _builder.parse(e) == _builder.Query(
        '(%s OR (`age` > %s));', (False, 10,)
    )
    e = (age > 10) | (name == 'test')
    assert _builder.parse(e) == _builder.Query(
        '((`age` > %s) OR (`name` = %s));', (10, 'test')
    )
    e = (name == 'test') | (age > 10)
    assert _builder.parse(e) == _builder.Query(
        '((`name` = %s) OR (`age` > %s));', ('test', 10)
    )
    theday = datetime(year=2019, month=10, day=10)
    e = (name == 'test') | (lastlogin < theday)
    assert _builder.parse(e) == _builder.Query(
        '((`name` = %s) OR (`lastlogin` < %s));', ('test', theday)
    )
    e = lastlogin <= "2019-10-10"
    assert _builder.parse(e) == _builder.Query(
        '(`lastlogin` <= %s);', (theday,)
    )
    e = age + 1
    assert _builder.parse(e) == _builder.Query(
        '(`age` + %s);', (1,)
    )
    e = 1 + age
    assert _builder.parse(e) == _builder.Query(
        '(%s + `age`);', (1,)
    )
    e = age + '20'
    assert _builder.parse(e) == _builder.Query(
        '(`age` + %s);', (20,)
    )
    e = 20 + age
    assert _builder.parse(e) == _builder.Query(
        '(%s + `age`);', (20,)
    )
    e = name + 'name'
    assert _builder.parse(e) == _builder.Query(
        '(`name` || %s);', ('name',)
    )
    e = 'name' + name
    assert _builder.parse(e) == _builder.Query(
        '(%s || `name`);', ('name',)
    )
    nickname = t.VarChar(name='nickname')
    e = nickname + name
    assert _builder.parse(e) == _builder.Query(
        '(`nickname` || `name`);', ()
    )
    e = age - 1
    assert _builder.parse(e) == _builder.Query(
        '(`age` - %s);', (1,)
    )
    e = 100 - age
    assert _builder.parse(e) == _builder.Query(
        '(%s - `age`);', (100,)
    )
    e = age * '2'
    assert _builder.parse(e) == _builder.Query(
        '(`age` * %s);', (2,)
    )
    e = 2 * age
    assert _builder.parse(e) == _builder.Query(
        '(%s * `age`);', (2,)
    )
    e = 1000 / age
    assert _builder.parse(e) == _builder.Query(
        '(%s / `age`);', (1000,)
    )
    e = age / 2
    assert _builder.parse(e) == _builder.Query(
        '(`age` / %s);', (2,)
    )
    e = age ^ name
    assert _builder.parse(e) == _builder.Query(
        '(`age` # `name`);', ()
    )
    e = 'name' ^ name
    assert _builder.parse(e) == _builder.Query(
        '(%s # `name`);', ('name',)
    )
    e = name == 'at7h'
    assert _builder.parse(e) == _builder.Query(
        '(`name` = %s);', ('at7h',)
    )
    e = name != 'at7h'
    assert _builder.parse(e) == _builder.Query(
        '(`name` != %s);', ('at7h',)
    )
    e = name <= 'at7h'
    assert _builder.parse(e) == _builder.Query(
        '(`name` <= %s);', ('at7h',)
    )
    e = name >= 'at7h'
    assert _builder.parse(e) == _builder.Query(
        '(`name` >= %s);', ('at7h',)
    )
    e = age < 90
    assert _builder.parse(e) == _builder.Query(
        '(`age` < %s);', (90,)
    )
    e = age > 20
    assert _builder.parse(e) == _builder.Query(
        '(`age` > %s);', (20,)
    )
    e = name >> None
    assert _builder.parse(e) == _builder.Query(
        '(`name` IS %s);', (None,)
    )
    e = name << ['at7h', 'mejer']
    assert _builder.parse(e) == _builder.Query(
        '(`name` IN %s);', (('at7h', 'mejer'),)
    )
    e = name % 'at'
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE BINARY %s);', ('at',)
    )
    e = name ** 'at'
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE %s);', ('at',)
    )
    e = age[slice(20, 30)]
    assert _builder.parse(e) == _builder.Query(
        '(`age` BETWEEN %s AND %s);', (20, 30,)
    )
    e = age[10]
    assert _builder.parse(e) == _builder.Query(
        '(`age` = %s);', (10,)
    )
    try:
        e = age[slice(20)]
        _builder.parse(e)
        assert False, "Should raise ValueError"
    except ValueError:
        pass

    e = name.concat(10)
    assert _builder.parse(e) == _builder.Query(
        '(`name` || %s);', ('10',)
    )
    e = name.binand('at7h')
    assert _builder.parse(e) == _builder.Query(
        '(`name` & %s);', ('at7h',)
    )
    e = name.binor('at7h')
    assert _builder.parse(e) == _builder.Query(
        '(`name` | %s);', ('at7h',)
    )
    e = name.in_(['at7h', 'mejor'])
    assert _builder.parse(e) == _builder.Query(
        '(`name` IN %s);', (('at7h', 'mejor'),)
    )
    e = name.in_(_builder.SQL("SELECT * FROM `user`"))
    assert _builder.parse(e) == _builder.Query(
        '(`name` IN (SELECT * FROM `user`));', ()
    )
    e = name.in_(10)
    try:
        _builder.parse(e)
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    e = name.nin_(['at7h', 'mejor'])
    assert _builder.parse(e) == _builder.Query(
        '(`name` NOT IN %s);', (('at7h', 'mejor'),)
    )
    e = name.exists(['at7h', 'mejor'])
    assert _builder.parse(e) == _builder.Query(
        '(`name` EXISTS %s);', (('at7h', 'mejor'),)
    )
    e = name.nexists(['at7h', 'mejor'])
    assert _builder.parse(e) == _builder.Query(
        '(`name` NOT EXISTS %s);', (('at7h', 'mejor'),)
    )
    e = name.isnull()
    assert _builder.parse(e) == _builder.Query(
        '(`name` IS %s);', (None,)
    )
    e = name.isnull(False)
    assert _builder.parse(e) == _builder.Query(
        '(`name` IS NOT %s);', (None,)
    )
    e = name.regexp('at.*')
    assert _builder.parse(e) == _builder.Query(
        '(`name` REGEXP %s);', ('at.*',)
    )
    e = name.regexp('at.*', i=False)
    assert _builder.parse(e) == _builder.Query(
        '(`name` REGEXP BINARY %s);', ('at.*',)
    )
    e = password.like(177)
    assert _builder.parse(e) == _builder.Query(
        '(`password` LIKE %s);', ('177',)
    )
    e = password.like(177, i=False)
    assert _builder.parse(e) == _builder.Query(
        '(`password` LIKE BINARY %s);', ('177',)
    )
    e = password.contains(7867)
    assert _builder.parse(e) == _builder.Query(
        '(`password` LIKE %s);', ('%7867%',)
    )
    e = password.contains(7867, i=False)
    assert _builder.parse(e) == _builder.Query(
        '(`password` LIKE BINARY %s);', ('%7867%',)
    )
    e = name.endswith('7h')
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE %s);', ('%7h',)
    )
    e = name.endswith('7h', i=False)
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE BINARY %s);', ('%7h',)
    )
    e = name.startswith('at')
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE %s);', ('at%',)
    )
    e = name.startswith('at', i=False)
    assert _builder.parse(e) == _builder.Query(
        '(`name` LIKE BINARY %s);', ('at%',)
    )
    e = age.between(10, 30)
    assert _builder.parse(e) == _builder.Query(
        '(`age` BETWEEN %s AND %s);', (10, 30)
    )
    e = age.nbetween(10, 30)
    assert _builder.parse(e) == _builder.Query(
        '(`age` NOT BETWEEN %s AND %s);', (10, 30)
    )
    e = age.asc()
    assert _builder.parse(e) == _builder.Query(
        '`age` ASC ;', ()
    )
    e = age.desc()
    assert _builder.parse(e) == _builder.Query(
        '`age` DESC ;', ()
    )
    e = age.as_('a')
    assert _builder.parse(e) == _builder.Query(
        '`age` AS `a`;', ()
    )
    e = age.as_('')
    assert _builder.parse(e) == _builder.Query(
        '`age`;', ()
    )
    e = (age > 10) & (name == 'test')
    assert _builder.parse(e) == _builder.Query(
        '((`age` > %s) AND (`name` = %s));', (10, 'test')
    )
    e = (name == 'test') & (age > 10)
    assert _builder.parse(e) == _builder.Query(
        '((`name` = %s) AND (`age` > %s));', ('test', 10)
    )
    e = (age >= '20') & name.in_(['at7h', 'mejor']) | password.startswith('153')
    assert _builder.parse(e) == _builder.Query(
        '(((`age` >= %s) AND (`name` IN %s)) OR (`password` LIKE %s));',
        (20, ('at7h', 'mejor'), '153%')
    )

    # _builder
    sql = _builder.SQL("SELECT")
    assert repr(sql) == str(sql) == 'SQL(SELECT)'
    sql = _builder.SQL("SELECT * FROM `user` WHERE `id` IN %s", (1, 2, 3))
    assert repr(sql) == str(sql) == (
        'SQL(SELECT * FROM `user` WHERE `id` IN %s) % (1, 2, 3)')
    assert _builder.parse(sql) == _builder.Query(
        "SELECT * FROM `user` WHERE `id` IN %s;", (1, 2, 3)
    )
    q = _builder.Query("SELECT")
    assert repr(q) == "Query({})".format(str(q))
    try:
        q.r = 1
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    try:
        assert _builder.parse((age > 10) | (name == 'test')) == sql
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    assert q.r is True
    q.r = False
    assert q.r is False
    q = _builder.Query("SeLeCT FrOm")
    assert q.r is True
    q = _builder.Query("SShow")
    assert q.r is True
    q = _builder.Query("SSow")
    assert q.r is False
    try:
        assert _builder.Query("SELECT", {1: 1}).params
        assert False, 'Should raise TypeError'
    except TypeError:
        pass
    ctx = _builder.Context()
    ctx.literal("SELECT").values("100")
    assert _builder.parse(ctx) == _builder.Query('SELECT;', ("100",))


def test_fieldbase():
    nickname = t.VarChar()
    try:
        hash(nickname)
        assert False
    except err.NoColumnNameError:
        pass
    assert nickname.to_str({}) == '{}'
    try:
        nickname.to_str(None)
        assert False
    except ValueError:
        pass
    nickname = t.VarChar(null=False)
    assert nickname.db_value(1) == '1'


def parsef(field):
    return _builder.parse(field.__def__()).sql


def test_tinyint():
    tinyint = t.Tinyint()
    try:
        assert tinyint.column
        assert False, "Should raise NoColumnNameError"
    except err.NoColumnNameError:
        pass
    try:
        assert tinyint.__def__()
        assert False, "Should raise NoColumnNameError"
    except err.NoColumnNameError:
        pass
    tinyint = t.Tinyint(name="ty")
    assert tinyint.column == "`ty`"
    assert repr(tinyint) == "<types.Tinyint object 'ty'>"
    assert str(tinyint) == "ty"
    assert parsef(tinyint) == "`ty` tinyint(4) DEFAULT NULL;"

    tinyint = t.Tinyint(name='ty', length=3, null=False, default=1, comment="ty")
    assert parsef(tinyint) == "`ty` tinyint(3) NOT NULL DEFAULT '1' COMMENT 'ty';"

    tinyint = t.Tinyint(name='ty', unsigned=True, default=1, comment="ty")
    assert parsef(tinyint) == "`ty` tinyint(4) unsigned DEFAULT '1' COMMENT 'ty';"

    tinyint = t.Tinyint(name='ty', zerofill=True)
    assert parsef(tinyint) == f"`ty` tinyint(4) zerofill DEFAULT NULL;"

    assert tinyint.py_value('1') == 1
    assert tinyint.db_value('11') == 11

    assert tinyint.db_value('1') == 1
    assert tinyint.py_value(True) == 1
    try:
        tinyint.db_value('1x')
        assert False, "Should raise ValueError"
    except ValueError:
        pass


def test_smllint():
    smallint = t.Smallint(name='sl', comment="sl")
    assert smallint.py_value('1') == 1
    assert smallint.db_value('2') == 2
    assert repr(smallint) == "<types.Smallint object 'sl'>"
    assert str(smallint) == "sl"
    assert parsef(smallint) == "`sl` smallint(6) DEFAULT NULL COMMENT 'sl';"
    smallint = t.Smallint(
        name='sl', length=4, unsigned=True, zerofill=True,
        null=False, default=1, comment="sl"
    )
    assert parsef(smallint) == (
        "`sl` smallint(4) unsigned zerofill NOT NULL DEFAULT '1' COMMENT 'sl';")
    assert smallint.py_value(True) == 1
    try:
        smallint.db_value('1x')
        assert False, "Should raise ValueError"
    except ValueError:
        pass


def test_int():
    int_ = t.Int(name='int', comment="int")
    assert parsef(int_) == "`int` int(11) DEFAULT NULL COMMENT 'int';"
    try:
        int_ = t.Int(name='int', default='xxx')
        assert parsef(int_)
        assert False, "Should raise TypeError"
    except TypeError:
        pass
    try:
        int_ = t.Int(name='int', primary_key=True, default=1)
        assert False, "Should raise ProgrammingError"
    except err.ProgrammingError:
        pass
    try:
        int_ = t.Int(name='int', auto=True)
        assert False, "Should raise ProgrammingError"
    except err.ProgrammingError:
        pass


def test_bigint():
    bigint = t.Bigint(name='bigint')
    assert parsef(bigint) == "`bigint` bigint(20) DEFAULT NULL;"
    bigint = t.Bigint(name='bigint', length=18, null=False, default=1)
    assert parsef(bigint) == "`bigint` bigint(18) NOT NULL DEFAULT '1';"
    try:
        bigint = t.Bigint(name='bigint', primary_key=True, default=1)
        bigint = t.Bigint(name='bigint', auto=True)
        assert False, "Should raise ProgrammingError"
    except err.ProgrammingError:
        pass
    bigint = t.Bigint(name='bigint', primary_key=True, auto=True)
    assert parsef(bigint) == "`bigint` bigint(20) NOT NULL AUTO_INCREMENT;"


def test_auto():
    auto = t.Auto(name='auto')
    assert parsef(auto) == "`auto` int(11) NOT NULL AUTO_INCREMENT;"


def test_bigauto():
    bigauto = t.BigAuto(name='bigauto')
    assert parsef(bigauto) == "`bigauto` bigint(20) NOT NULL AUTO_INCREMENT;"


def test_bool():
    bool_ = t.Bool(name='bool', null=False, default=True)
    assert parsef(bool_) == "`bool` bool NOT NULL DEFAULT '1';"
    assert bool_.db_value(0) is False
    assert bool_.py_value(1) is True
    assert bool_.to_str(True) == '1'
    assert bool_.to_str(False) == '0'


def test_float():
    float_ = t.Float(name='float', null=False, default=43.54, length=7)
    assert parsef(float_) == "`float` float(7) NOT NULL DEFAULT '43.54';"
    float_ = t.Float(name='float', length=(4, 3), default=100.0, unsigned=True)
    assert parsef(float_) == "`float` float(4,3) unsigned DEFAULT '100.0';"
    try:
        float_ = t.Float(name='float', length=(4, 3, 5))
        float_ = t.Float(name='float', length='7')
        assert False, 'Should raise TypeError'
    except TypeError:
        pass


def test_double():
    double = t.Double(name='double', null=False, length=7, default=43.54)
    assert parsef(double) == "`double` double(7) NOT NULL DEFAULT '43.54';"
    double = t.Double(name='double', length=(4, 3), unsigned=True)
    assert parsef(double) == "`double` double(4,3) unsigned DEFAULT NULL;"
    assert double.py_value(None) is None
    assert double.py_value(0) == 0.0
    try:
        double = t.Double(name='double', length=(4, 3, 5))
        double = t.Double(name='double', length='7')
        assert False, 'Should raise TypeError'
    except TypeError:
        pass


def test_decimal():
    import decimal as d
    decimal = t.Decimal(name='decimal')
    assert parsef(decimal) == "`decimal` decimal(10,5) DEFAULT NULL;"
    decimal = t.Decimal(name='decimal', length=(6, 3), default=d.Decimal(str(43.54)))
    assert parsef(decimal) == "`decimal` decimal(6,3) DEFAULT '43.54';"
    assert decimal.py_value(None) is None
    assert decimal.py_value(0) == d.Decimal(0)
    assert str(decimal.py_value('100.12')) == '100.12'
    assert isinstance(decimal.db_value(10), d.Decimal)
    try:
        decimal = t.Decimal(name='decimal', length=10)
        assert False, 'Should raise TypeError'
    except TypeError:
        pass
    decimal = t.Decimal(name='decimal', auto_round=True)
    assert decimal.py_value(d.Decimal(10)) == d.Decimal(10)
    assert decimal.db_value(10) == d.Decimal(10)


def test_text():
    text = t.Text(name='text', encoding=t.ENCODING.utf8mb4)
    assert parsef(text) == "`text` text CHARACTER SET utf8mb4 NULL;"
    assert hasattr(text, 'default') is False
    try:
        text = t.Text(name='text', encoding='utf7')
        assert False
    except ValueError:
        pass
    assert text.py_value(100) == '100'
    assert text.db_value(100) == '100'
    e = text + 'text'
    assert _builder.parse(e) == _builder.Query('(`text` || %s);', ('text',))
    e = 'text' + text
    assert _builder.parse(e) == _builder.Query('(%s || `text`);', ('text',))


def test_char():
    char = t.Char(name='char', length=100)
    assert parsef(char) == "`char` char(100) DEFAULT NULL;"
    try:
        char = t.Char(name='char', encoding='utf7')
        assert False
    except ValueError:
        pass


def test_varchar():
    varchar = t.VarChar(name='varchar', default='c')
    assert parsef(varchar) == "`varchar` varchar(254) DEFAULT 'c';"
    try:
        varchar = t.VarChar(name='varchar', default=7)
        assert False, 'Should raise TypeError'
    except TypeError:
        pass


def test_uuid():
    import uuid as uu
    uuid = t.UUID(name='uuid')
    assert parsef(uuid) == "`uuid` varchar(40) NOT NULL;"
    id_ = str(uu.uuid1())
    assert isinstance(uuid.py_value(id_), uu.UUID)
    assert isinstance(uuid.db_value(uu.uuid1()), str)
    try:
        uuid = t.UUID(primary_key=True, default='uuid')
        assert False
    except err.ProgrammingError:
        pass
    idr = '1a862a72-6a34-4772-8c22-ad8fa3316db5'
    assert uuid.db_value(idr) == '1a862a726a3447728c22ad8fa3316db5'
    assert uuid.db_value('1a862a726a3447728c22ad8fa3316db5') == '1a862a726a3447728c22ad8fa3316db5'
    assert uuid.db_value(uuid) is uuid
    assert uuid.py_value(idr) == uu.UUID(idr)
    assert uuid.py_value(uu.UUID(idr)) == uu.UUID(idr)
    assert uuid.py_value(None) is None


def test_ip():
    """ipv4"""
    ip = t.IP(name='ip')
    assert parsef(ip) == "`ip` bigint(20) DEFAULT NULL;"
    ip = t.IP(name='ip', default=0)
    assert parsef(ip) == "`ip` bigint(20) DEFAULT '0';"
    ip = t.IP(name='ip', default='127.0.0.1')
    assert parsef(ip) == "`ip` bigint(20) DEFAULT '2130706433';"

    assert ip.db_value(None) is None
    assert ip.py_value(None) is None

    # 0 <= number <= 4294967295
    assert ip.db_value('0.0.0.0') == 0
    assert ip.db_value('1.1.1.1') == 16843009
    assert ip.db_value('192.168.1.100') == 3232235876
    assert ip.db_value('255.255.255.255') == 4294967295
    try:
        ip.db_value('')
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        ip.db_value('255.255.255.256')
        assert False, "Should raise ValueError"
    except ValueError:
        pass

    assert ip.py_value('0.0.0.0') == '0.0.0.0'
    assert ip.py_value('1.1.1.1') == '1.1.1.1'
    assert ip.py_value('192.168.1.100') == '192.168.1.100'
    assert ip.py_value(0) == '0.0.0.0'
    assert ip.py_value(16843009) == '1.1.1.1'
    assert ip.py_value(3232235876) == '192.168.1.100'
    assert ip.py_value(4294967295) == '255.255.255.255'
    try:
        ip.py_value(4294967296)
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        ip.py_value(-1)
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    for item in [None, '', {}, ]:
        assert _helper.is_ipv4(item) is False


def test_email():
    email = t.Email(name='email', length=100, default='')
    assert parsef(email) == "`email` varchar(100) DEFAULT '';"

    emails = [
        "",
        "g@at7h.com",
        "c.c@c.com",
        "127121@1127121.cc",
        "y@a.cc",
        "cy.Gg.w12@s.com.cc",
    ]

    for e in emails:
        assert email.py_value(e) == email.db_value(e)

    assert email.db_value(None) is None
    assert email.py_value(None) is None
    try:
        email.py_value(0)
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        email.py_value("y@a.c")
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        email.py_value("g.@at7h.com")
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    for item in [None, '', {}, ]:
        assert _helper.is_email(item) is False


def test_url():
    url = t.URL(name='url', default='')
    assert parsef(url) == "`url` varchar(254) DEFAULT '';"

    urls = [
        "",
        "http://at7h.com",
        "https://127.0.0.1:8000/files/1.txt",
        "ftp://ds.cc/1232",
        "http://www.ccc?query=1",
        "http://z12.ZZ",
    ]

    for u in urls:
        assert url.py_value(u) == url.db_value(u)

    assert url.db_value(None) is None
    assert url.py_value(None) is None
    try:
        url.py_value(0)
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        url.py_value("ftp://cc.c")
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    try:
        url.py_value("127.0.0.1:8800")
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    for item in [None, '', {}, ]:
        assert _helper.is_url(item) is False


def test_date():
    date_ = t.Date(name='date')
    assert parsef(date_) == "`date` date DEFAULT NULL;"
    date_ = t.Date(name='date', default=datetime.date)
    assert parsef(date_) == "`date` date DEFAULT NULL;"
    td = date(2019, 10, 1)
    date_ = t.Date(name='date', default=td)
    assert parsef(date_) == "`date` date DEFAULT '2019-10-01';"
    assert isinstance(date_.py_value(datetime.now()), date)
    assert isinstance(date_.py_value("2019-10-01"), date)
    assert date_.to_str(td) == "2019-10-01"
    assert isinstance(date_(), date)


def test_time():
    time_ = t.Time(name='time')
    assert parsef(time_) == "`time` time DEFAULT NULL;"
    time_ = t.Time(name='time', default=datetime.time)
    assert parsef(time_) == "`time` time DEFAULT NULL;"
    td = time(22, 19, 34)
    time_ = t.Time(name='time', default=td)
    assert parsef(time_) == "`time` time DEFAULT '22:19:34.000000';"
    assert isinstance(time_.py_value(datetime.now()), time)
    assert isinstance(time_.py_value("10:23:23"), time)
    assert time_.to_str(td) == "22:19:34.000000"

    t.Time.formats = ['%H:%M:%S']
    td = time(22, 19, 34)
    time_ = t.Time(name='time', default=td)
    assert parsef(time_) == "`time` time DEFAULT '22:19:34.000000';"
    assert time_.to_str(td) == "22:19:34.000000"
    assert isinstance(time_(), time)
    assert isinstance(time_.db_value(timedelta(weeks=7)), time)


def test_datetime():
    datetime_ = t.DateTime(name='dt', formats=['%Y-%m-%d %H:%M:%S'])
    assert parsef(datetime_) == "`dt` datetime DEFAULT NULL;"
    assert isinstance(datetime_.py_value("2019-10-10 10:23:23"), datetime)
    assert datetime_.to_str(datetime(2019, 10, 10, 10, 23, 23)) == "2019-10-10 10:23:23"
    datetime_ = t.DateTime(name='dt', default=datetime.now)
    assert parsef(datetime_) == "`dt` datetime DEFAULT NULL;"
    assert datetime_.to_str(datetime(2019, 10, 10, 10, 23, 23)) == "2019-10-10 10:23:23.000000"
    assert isinstance(datetime_(), datetime)


def test_timestamp():
    timestamp = t.Timestamp(name='ts')
    assert parsef(timestamp) == "`ts` timestamp NULL DEFAULT NULL;"
    assert isinstance(timestamp.py_value("2019-10-10 10:23:23"), datetime)
    strdt = "2019-10-10 10:23:23"
    dt = datetime(2019, 10, 10, 10, 23, 23)
    assert timestamp.to_str(dt) == strdt
    assert timestamp.db_value(strdt) == dt
    timestamp = t.Timestamp(name='ts', default=datetime.now)
    assert parsef(timestamp) == "`ts` timestamp NULL DEFAULT NULL;"
    assert timestamp.db_value(None) is None
    assert isinstance(timestamp.db_value(date(2019, 10, 10)), datetime)
    assert isinstance(timestamp.db_value(1573984070), datetime)
    assert isinstance(timestamp.py_value(1573984070), datetime)
    timestamp = t.Timestamp(name='ts', utc=True)
    assert isinstance(timestamp.db_value(1573984070), datetime)
    assert isinstance(timestamp.py_value(1573984070), datetime)


def test_key():
    age = t.Int(name='age')
    name = t.Char(name='name')
    password = t.VarChar(name='password')
    key = t.K('idx_name', name)
    assert str(key) == 'KEY `idx_name` (`name`);'
    key = t.K('idx_name_age', (name, age))
    assert str(key) == 'KEY `idx_name_age` (`name`, `age`);'
    key = t.K('idx_name_age', ('name', 'age'))
    assert str(key) == 'KEY `idx_name_age` (`name`, `age`);'
    key = t.UK('uk_password', password, comment='password')
    assert str(key) == "UNIQUE KEY `uk_password` (`password`) COMMENT 'password';"
    assert repr(key) == "<types.UK(UNIQUE KEY `uk_password` (`password`) COMMENT 'password';)>"
    assert hash(key) == hash('uk_password')
    try:
        t.K('idx_name', [1, 2, 4])
        assert False
    except TypeError:
        pass


def test_funs():
    age = t.Int(name='age')
    s = t.F.SUM(age).as_('age_sum')
    assert _builder.parse(s).sql == 'SUM(`age`) AS `age_sum`;'

    m_ = t.F.MAX(age).as_('age_max')
    assert _builder.parse(m_).sql == 'MAX(`age`) AS `age_max`;'


class TypesModel(Model):

    id = t.Auto(comment='primary_key')
    tinyint = t.Tinyint(1, unsigned=True, zerofill=True, comment='tinyint')
    smallint = t.Smallint(null=False, default=0, comment='smallint')
    int_ = t.Int(unsigned=True, null=False, default=0, comment='int')
    bigint = t.Bigint(45, null=False, default=0, comment='bigint')
    text = t.Text(encoding=t.ENCODING.UTF8MB4, null=False, comment='text')
    char = t.Char(45, null=False, default='', comment='char')
    varchar = t.VarChar(45, null=False, default='', comment='varchar')
    uuid = t.UUID(comment='uuid test')
    float_ = t.Float((3, 3), default=0, comment='float')
    double_ = t.Double((4, 4), unsigned=True, default=0, comment='double')
    decimal = t.Decimal((10, 2), unsigned=True, default=0, comment='decimal')
    ip = t.IP(default=0)
    email = t.Email(length=100, default='')
    url = t.URL(default='')
    time_ = t.Time(default=datetime.now, comment='time')
    date_ = t.Date(default=datetime.now, comment='date')
    datetime_ = t.DateTime(default=datetime.now, comment='datetime')
    now_ts = t.Timestamp(default=datetime.now, comment='now ts')
    created_at = t.Timestamp(default=t.ON_CREATE, comment='created_at')
    updated_at = t.Timestamp(default=t.ON_UPDATE, comment='updated_at')

    class Meta:
        name = 'test_types_table'
        comment = 'type case table'
        auto_increment = 7
        indexes = (
            t.K('key', ['tinyint', 'datetime_'], comment='key test'),
            t.UK('ukey', 'varchar', comment='unique key test'),
        )


@pytest.mark.asyncio
async def test_types():

    async with db.binder():
        await TypesModel.create()
        create = await TypesModel.show().create_syntax()
        assert (
            "CREATE TABLE `test_types_table` (\n"
            "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'primary_key',\n"
            "  `tinyint` tinyint(1) unsigned zerofill DEFAULT NULL COMMENT 'tinyint',\n"
            "  `smallint` smallint(6) NOT NULL DEFAULT '0' COMMENT 'smallint',\n"
            "  `int_` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'int',\n"
            "  `bigint` bigint(45) NOT NULL DEFAULT '0' COMMENT 'bigint',\n"
        ) in create
        assert (
            "PRIMARY KEY (`id`),\n"
            "  UNIQUE KEY `ukey` (`varchar`) COMMENT 'unique key test',\n"
            "  KEY `key` (`tinyint`,`datetime_`) COMMENT 'key test'\n"
        ) in create
        await TypesModel.drop()
