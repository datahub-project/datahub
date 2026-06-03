from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedCounter,
)


def test_counter_most_common_by_group():
    c = FileBackedCounter()
    c.increment("g1", "a", 3)
    c.increment("g1", "b", 1)
    c.increment("g1", "a", 2)  # a -> 5
    c.increment("g2", "x", 1)
    result = dict(c.most_common_by_group())
    c.close()
    assert result["g1"] == [("a", 5), ("b", 1)]
    assert result["g2"] == [("x", 1)]


def test_counter_cross_flush_accumulation():
    # batch_size=1 forces a flush per increment, exercising the cross-flush upsert.
    c = FileBackedCounter(batch_size=1)
    for _ in range(3):
        c.increment("g", "a", 1)
    c.increment("g", "b", 1)
    result = dict(c.most_common_by_group())
    c.close()
    assert result["g"] == [("a", 3), ("b", 1)]


def test_counter_tie_break_is_insertion_order():
    # Tied counts must break by insertion order (rowid ASC), deterministically,
    # mirroring Counter.most_common — across flushes too.
    c = FileBackedCounter(batch_size=1)
    c.increment("g", "first", 1)
    c.increment("g", "second", 1)
    c.increment("g", "third", 1)
    result = dict(c.most_common_by_group())
    c.close()
    assert result["g"] == [("first", 1), ("second", 1), ("third", 1)]


def test_counter_zero_count_sentinel_creates_group():
    # An item incremented by 0 still creates the group (used for existence tracking).
    c = FileBackedCounter()
    c.increment("g", "", 0)
    result = dict(c.most_common_by_group())
    c.close()
    assert result == {"g": [("", 0)]}


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


def test_counter_groups_yielded_in_key_order():
    c = FileBackedCounter()
    c.increment("g2", "x", 1)  # inserted before g1
    c.increment("g1", "y", 1)
    groups = [g for g, _ in c.most_common_by_group()]
    c.close()
    assert groups == ["g1", "g2"]


def test_counter_increment_defaults_to_one():
    c = FileBackedCounter()
    c.increment("g", "a")  # default count=1
    c.increment("g", "a")
    result = dict(c.most_common_by_group())
    c.close()
    assert result["g"] == [("a", 2)]
