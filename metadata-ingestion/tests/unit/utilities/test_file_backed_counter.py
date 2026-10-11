import threading
from typing import Iterator

import pytest

from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedCounter,
    GroupedItemCounter,
    InMemoryGroupedCounter,
)


def test_counter_cross_flush_accumulation():
    # batch_size=1 forces a flush per increment, exercising the cross-flush upsert.
    c = FileBackedCounter(batch_size=1)
    for _ in range(3):
        c.increment("g", "a", 1)
    c.increment("g", "b", 1)
    result = dict(c.most_common_by_group())
    c.close()
    assert result["g"] == [("a", 3), ("b", 1)]


def test_counter_shared_connection_not_closed_on_close():
    conn = ConnectionWrapper()
    try:
        c = FileBackedCounter(shared_connection=conn)
        c.increment("g", "a", 1)
        assert dict(c.most_common_by_group())["g"] == [("a", 1)]
        c.close()
        conn.execute("SELECT 1").fetchone()  # still usable
    finally:
        conn.close()


def test_counter_close_is_idempotent():
    c = FileBackedCounter()
    c.increment("g", "a", 1)
    c.close()
    c.close()


def test_counter_flushed_when_shared_connection_closes(tmp_path):
    # Closing a shared connection must flush a registered counter's buffered increments
    # before the connection is torn down, even if the counter wasn't close()d directly.
    db = tmp_path / "shared.db"
    conn = ConnectionWrapper(filename=db)
    counter = FileBackedCounter(shared_connection=conn, tablename="counts")
    counter.increment("g", "a", 3)
    # Deliberately do NOT call counter.close(); closing the connection should flush it.
    conn.close()

    reopened = ConnectionWrapper(filename=db)
    try:
        rows = reopened.execute(
            "SELECT group_key, item_key, cnt FROM counts"
        ).fetchall()
        assert [tuple(r) for r in rows] == [("g", "a", 3)]
    finally:
        reopened.close()


def test_counter_rejects_unsafe_tablename():
    # tablename is interpolated into SQL (SQLite can't bind identifiers), so it must be
    # a safe identifier, not attacker-controlled text.
    with pytest.raises(ValueError):
        FileBackedCounter(tablename="counts; DROP TABLE x --")


def test_counter_increment_is_thread_safe():
    # Concurrent increments must not lose counts or hit "dict changed size during
    # iteration" in _flush. A tiny batch_size forces flushes to interleave with the
    # increments running on other threads.
    c = FileBackedCounter(batch_size=5)
    items = [chr(ord("a") + i) for i in range(10)]
    n_threads, per_thread = 8, 500

    def work() -> None:
        for _ in range(per_thread):
            for item in items:
                c.increment("g", item, 1)

    threads = [threading.Thread(target=work) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    counts = dict(dict(c.most_common_by_group())["g"])
    c.close()
    assert all(counts[item] == n_threads * per_thread for item in items), counts


@pytest.fixture(params=["file", "memory"])
def counter(request: pytest.FixtureRequest) -> Iterator[GroupedItemCounter]:
    c: GroupedItemCounter = (
        FileBackedCounter(batch_size=1)  # batch_size=1 also exercises the flush path
        if request.param == "file"
        else InMemoryGroupedCounter()
    )
    yield c
    c.close()


def test_grouped_counter_most_common_by_group(counter):
    counter.increment("g1", "a", 3)
    counter.increment("g1", "b", 1)
    counter.increment("g1", "a", 2)  # a -> 5
    counter.increment("g2", "x", 1)
    result = dict(counter.most_common_by_group())
    assert result["g1"] == [("a", 5), ("b", 1)]
    assert result["g2"] == [("x", 1)]


def test_grouped_counter_tie_break_is_insertion_order(counter):
    counter.increment("g", "first", 1)
    counter.increment("g", "second", 1)
    counter.increment("g", "third", 1)
    assert dict(counter.most_common_by_group())["g"] == [
        ("first", 1),
        ("second", 1),
        ("third", 1),
    ]


def test_grouped_counter_zero_count_sentinel_creates_group(counter):
    counter.increment("g", "", 0)
    assert dict(counter.most_common_by_group()) == {"g": [("", 0)]}


def test_grouped_counter_groups_yielded_in_key_order(counter):
    counter.increment("g2", "x", 1)  # inserted before g1
    counter.increment("g1", "y", 1)
    assert [g for g, _ in counter.most_common_by_group()] == ["g1", "g2"]


def test_grouped_counter_increment_defaults_to_one(counter):
    counter.increment("g", "a")
    counter.increment("g", "a")
    assert dict(counter.most_common_by_group())["g"] == [("a", 2)]
