from unittest.mock import Mock

from datahub.configuration.common import AllowDenyFilterReport, AllowDenyPattern


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


def test_reporting_allowed():
    pattern = AllowDenyPattern(allow=["test.*"], deny=["test.exclude"])
    report = Mock(spec=AllowDenyFilterReport)
    pattern.set_report(report)

    # Test allowed pattern
    assert pattern.allowed("test.allowed")
    report.processed.assert_called_once_with("test.allowed")
    report.dropped.assert_not_called()
    report.reset_mock()

    # Test denied pattern
    assert not pattern.allowed("test.exclude")
    report.processed.assert_not_called()
    report.dropped.assert_called_once_with("test.exclude")
    report.reset_mock()

    # Test non-matching pattern
    assert not pattern.allowed("other")
    report.processed.assert_not_called()
    report.dropped.assert_called_once_with("other")


def test_reporting_denied():
    pattern = AllowDenyPattern(allow=["test.*"], deny=["test.exclude"])
    report = Mock(spec=AllowDenyFilterReport)
    pattern.set_report(report)

    # Test denied pattern
    assert pattern.denied("test.exclude")
    report.processed.assert_not_called()
    report.dropped.assert_called_once_with("test.exclude")
    report.reset_mock()

    # Test allowed pattern
    assert not pattern.denied("test.allowed")
    report.processed.assert_called_once_with("test.allowed")
    report.dropped.assert_not_called()
    report.reset_mock()

    # Test non-matching pattern
    assert not pattern.denied("other")
    report.processed.assert_called_once_with("other")
    report.dropped.assert_not_called()
