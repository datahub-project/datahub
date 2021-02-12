from gometa.configuration.common import AllowDenyPattern


def test_allow_all():
    pattern = AllowDenyPattern.allow_all()
    assert pattern.allowed("foo.table") == True


def test_deny_all():
    pattern = AllowDenyPattern(allow=[], deny=[".*"])
    assert pattern.allowed("foo.table") == False


def test_single_table():
    pattern = AllowDenyPattern(allow=["foo.mytable"])
    assert pattern.allowed("foo.mytable") == True
