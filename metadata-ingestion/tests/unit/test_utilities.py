import sys

import pytest

from datahub.utilities.delayed_iter import delayed_iter
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.sql_parser import DefaultSQLParser


def test_delayed_iter():
    events = []

    def maker(n):
        for i in range(n):
            events.append(("add", i))
            yield i

    for i in delayed_iter(maker(4), 2):
        events.append(("remove", i))

    assert events == [
        ("add", 0),
        ("add", 1),
        ("add", 2),
        ("remove", 0),
        ("add", 3),
        ("remove", 1),
        ("remove", 2),
        ("remove", 3),
    ]

    events.clear()
    for i in delayed_iter(maker(2), None):
        events.append(("remove", i))

    assert events == [
        ("add", 0),
        ("add", 1),
        ("remove", 0),
        ("remove", 1),
    ]


def test_groupby_unsorted():
    grouped = groupby_unsorted("ABCAC", key=lambda x: x)

    assert list(grouped) == [
        ("A", ["A", "A"]),
        ("B", ["B"]),
        ("C", ["C", "C"]),
    ]


@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_default_sql_parser():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    tables_list = DefaultSQLParser(sql_query).get_tables()

    assert tables_list == ["foo", "bar"]
