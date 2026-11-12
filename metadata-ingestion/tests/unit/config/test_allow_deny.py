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


def test_with_deny_returns_new_instance_and_leaves_original_alone() -> None:
    p = AllowDenyPattern(deny=["a"])
    p2 = p.with_deny("b")
    assert p2 is not p
    assert p.deny == ["a"]
    assert p2.deny == ["a", "b"]


def test_with_deny_is_not_fooled_by_warm_cache() -> None:
    p = AllowDenyPattern()
    assert p.allowed("foo_bar")  # warms _compiled_deny
    p2 = p.with_deny("foo.*")
    assert not p2.allowed("foo_bar")
    assert p.allowed("foo_bar")


def test_with_deny_is_idempotent_and_dedupes_in_order() -> None:
    assert AllowDenyPattern().with_deny("a").with_deny("a").deny == ["a"]
    assert AllowDenyPattern().with_deny("a", "b", "a").deny == ["a", "b"]
    assert AllowDenyPattern(deny=["x", "a"]).with_deny("a", "y", "x").deny == [
        "x",
        "a",
        "y",
    ]
    # Duplicates already in the deny list (e.g. a recipe saved after the old
    # in-place append ran twice) collapse too.
    assert AllowDenyPattern(deny=["a", "a"]).with_deny("b").deny == ["a", "b"]


def test_with_deny_preserves_ignore_case_and_allow() -> None:
    p = AllowDenyPattern(allow=["foo.*"], ignoreCase=False).with_deny("FOO_BAD")
    assert p.ignoreCase is False
    assert p.allow == ["foo.*"]
    assert p.allowed("foo_bad")
    assert not p.allowed("FOO_BAD")


def test_with_deny_preserves_subclass() -> None:
    class Sub(AllowDenyPattern):
        pass

    assert type(Sub().with_deny("x")) is Sub
