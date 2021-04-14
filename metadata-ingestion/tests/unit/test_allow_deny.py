from datahub.configuration.common import AllowDenyPattern


def test_allow_all() -> None:
    pattern = AllowDenyPattern.allow_all()
    assert pattern.allowed("foo.table")


def test_deny_all() -> None:
    pattern = AllowDenyPattern(allow=[], deny=[".*"])
    assert not pattern.allowed("foo.table")


def test_single_table() -> None:
    pattern = AllowDenyPattern(allow=["foo.mytable"])
    assert pattern.allowed("foo.mytable")


def test_default_deny() -> None:
    pattern = AllowDenyPattern(allow=["foo.mytable"])
    assert not pattern.allowed("foo.bar")


def test_fully_speced():
    pattern = AllowDenyPattern(allow=["foo.mytable"])
    assert pattern.is_fully_specified_allow_list()
    pattern = AllowDenyPattern(allow=["foo.*", "foo.table"])
    assert not pattern.is_fully_specified_allow_list()
    pattern = AllowDenyPattern(allow=["foo.?", "foo.table"])
    assert not pattern.is_fully_specified_allow_list()


def test_is_allowed():
    pattern = AllowDenyPattern(allow=["foo.mytable"], deny=["foo.*"])
    assert pattern.get_allowed_list() == []
