# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
