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


def test_allow_literal_with_regex_metacharacters() -> None:
    # Database names like 'sales_(jan/feb)_data' contain '(' and ')' which are regex
    # metacharacters. When a user puts the exact name in the allow list, it should
    # be treated as a literal and match correctly.
    pattern = AllowDenyPattern(allow=["sales_(jan/feb)_data"])
    assert pattern.allowed("sales_(jan/feb)_data")
    assert not pattern.allowed("sales_other_data")


def test_allow_literal_case_insensitive_with_special_chars() -> None:
    pattern = AllowDenyPattern(allow=["PROD_(READ/WRITE)_DB"])
    assert pattern.allowed("prod_(read/write)_db")
    assert pattern.allowed("PROD_(READ/WRITE)_DB")


def test_allow_literal_case_sensitive_with_special_chars() -> None:
    pattern = AllowDenyPattern(allow=["Prod_(Read/Write)_Db"], ignoreCase=False)
    assert pattern.allowed("Prod_(Read/Write)_Db")
    assert not pattern.allowed("prod_(read/write)_db")


def test_deny_literal_with_regex_metacharacters() -> None:
    pattern = AllowDenyPattern(deny=["staging_(raw/agg)_db"])
    assert not pattern.allowed("staging_(raw/agg)_db")
    assert pattern.allowed("staging_other_db")


def test_deny_literal_case_sensitive_with_special_chars() -> None:
    pattern = AllowDenyPattern(deny=["Staging_(Raw/Agg)_Db"], ignoreCase=False)
    assert not pattern.allowed("Staging_(Raw/Agg)_Db")
    assert pattern.allowed("staging_(raw/agg)_db")


def test_allow_and_deny_literal_interaction() -> None:
    pattern = AllowDenyPattern(
        allow=["sales_(jan/feb)_data", "prod_(read/write)_db"],
        deny=["sales_(jan/feb)_data"],
    )
    assert not pattern.allowed("sales_(jan/feb)_data")
    assert pattern.allowed("prod_(read/write)_db")


def test_regex_and_literal_patterns_mixed() -> None:
    pattern = AllowDenyPattern(allow=["normal_db.*", "special_(a/b)_db"])
    assert pattern.allowed("normal_db_one")
    assert pattern.allowed("special_(a/b)_db")
    assert not pattern.allowed("other_db")


def test_regex_patterns_still_work_with_literal_fallback() -> None:
    pattern = AllowDenyPattern(allow=["flights.*"])
    assert pattern.allowed("flights-database")
    assert pattern.allowed("flights_other")
    assert not pattern.allowed("test-database")
