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


def test_prefix_match():
    pattern = AllowDenyPattern(allow=["mytable"])
    assert pattern.allowed("mytable.foo")
    assert not pattern.allowed("foo.mytable")


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


def test_case_sensitivity():
    pattern = AllowDenyPattern(allow=["Foo.myTable"])
    assert pattern.allowed("foo.mytable")
    assert pattern.allowed("FOO.MYTABLE")
    assert pattern.allowed("Foo.MyTable")
    pattern = AllowDenyPattern(allow=["Foo.myTable"], ignoreCase=False)
    assert not pattern.allowed("foo.mytable")
    assert pattern.allowed("Foo.myTable")


def test_is_allow_all() -> None:
    assert AllowDenyPattern.allow_all().is_allow_all()
    assert AllowDenyPattern(allow=["^prod", ".*"]).is_allow_all()
    assert not AllowDenyPattern(deny=["^tmp_"]).is_allow_all()
    assert not AllowDenyPattern(allow=["^prod$"]).is_allow_all()
    assert not AllowDenyPattern(allow=[]).is_allow_all()


def test_is_allow_all_survives_the_regex_cache() -> None:
    # allowed() stores compiled regexes in __dict__, which is what __eq__
    # compares, so a used default stops equalling a fresh allow_all().
    pattern = AllowDenyPattern.allow_all()
    assert pattern.allowed("analytics.orders")
    assert pattern.is_allow_all()


def test_is_allow_all_ignores_ignore_case() -> None:
    assert AllowDenyPattern(allow=[".*"], ignoreCase=False).is_allow_all()
