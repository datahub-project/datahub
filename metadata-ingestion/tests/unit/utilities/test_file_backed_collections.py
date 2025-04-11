import dataclasses
import json
import pathlib
import random
import sqlite3
from dataclasses import dataclass
from typing import Counter, Dict
from unittest.mock import patch

import pytest

from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
    FileBackedList,
)


def test_set_use_sqlite_on_conflict():
    with patch("sqlite3.sqlite_version_info", (3, 24, 0)):
        cache = FileBackedDict[int](
            tablename="cache",
            cache_max_size=10,
            cache_eviction_batch_size=10,
        )
        assert cache._use_sqlite_on_conflict is True

    with pytest.raises(RuntimeError):
        with patch("sqlite3.sqlite_version_info", (3, 23, 1)):
            cache = FileBackedDict[int](
                tablename="cache",
                cache_max_size=10,
                cache_eviction_batch_size=10,
            )
            assert cache._use_sqlite_on_conflict is False

    with patch("sqlite3.sqlite_version_info", (3, 23, 1)), patch(
        "datahub.utilities.file_backed_collections.OVERRIDE_SQLITE_VERSION_REQUIREMENT",
        True,
    ):
        cache = FileBackedDict[int](
            tablename="cache",
            cache_max_size=10,
            cache_eviction_batch_size=10,
        )
        assert cache._use_sqlite_on_conflict is False


@pytest.mark.parametrize("use_sqlite_on_conflict", [True, False])
def test_file_dict(use_sqlite_on_conflict: bool) -> None:
    cache = FileBackedDict[int](
        tablename="cache",
        cache_max_size=10,
        cache_eviction_batch_size=10,
        _use_sqlite_on_conflict=use_sqlite_on_conflict,
    )

    for i in range(100):
        cache[f"key-{i}"] = i

    assert len(cache) == 100
    assert sorted(cache) == sorted([f"key-{i}" for i in range(100)])
    assert sorted(cache.items()) == sorted([(f"key-{i}", i) for i in range(100)])
    assert sorted(cache.values()) == sorted([i for i in range(100)])

    # Force eviction of everything.
    cache.flush()

    assert len(cache) == 100
    assert sorted(cache) == sorted([f"key-{i}" for i in range(100)])

    # Test getting a key.
    # This implicitly also tests that cache eviction happens.
    for i in range(100):
        assert cache[f"key-{i}"] == i

    # Make sure that the cache is being automatically evicted.
    assert len(cache._active_object_cache) <= 20
    assert len(cache) == 100

    # Test overwriting a key.
    cache["key-3"] = 3000
    assert cache["key-3"] == 3000

    # Test repeated overwrites.
    for i in range(100):
        cache["key-3"] = i
    assert cache["key-3"] == 99
    cache["key-3"] = 3

    # Test in operator
    assert "key-3" in cache
    assert "key-99" in cache
    assert "missing" not in cache
    assert "missing" not in cache

    # Test deleting keys, in and out of cache
    del cache["key-0"]
    del cache["key-99"]
    assert len(cache) == 98
    with pytest.raises(KeyError):
        cache["key-0"]
        cache["key-99"]

    # Test deleting a key that doesn't exist.
    with pytest.raises(KeyError):
        del cache["key-0"]

    # Test adding another key.
    cache["a"] = 1
    assert cache["a"] == 1
    assert len(cache) == 99
    assert sorted(cache) == sorted(["a"] + [f"key-{i}" for i in range(1, 99)])

    # Test deleting most things.
    for i in range(1, 99):
        assert cache[f"key-{i}"] == i
        del cache[f"key-{i}"]
    assert len(cache) == 1
    assert cache["a"] == 1

    # Test close.
    cache.close()
    with pytest.raises(AttributeError):
        cache["a"] = 1


@pytest.mark.parametrize("use_sqlite_on_conflict", [True, False])
def test_custom_serde(use_sqlite_on_conflict: bool) -> None:
    @dataclass(frozen=True)
    class Label:
        a: str
        b: int

    @dataclass
    class Main:
        x: int
        y: Dict[Label, float]

        def to_dict(self) -> Dict:
            d: Dict = {"x": self.x}
            str_y = {json.dumps(dataclasses.asdict(k)): v for k, v in self.y.items()}
            d["y"] = json.dumps(str_y)
            return d

        @classmethod
        def from_dict(cls, d: Dict) -> "Main":
            str_y = json.loads(d["y"])
            y = {}
            for k, v in str_y.items():
                k_str = json.loads(k)
                label = Label(k_str["a"], k_str["b"])
                y[label] = v

            return cls(d["x"], y)

    serializer_calls = 0
    deserializer_calls = 0

    def serialize(m: Main) -> str:
        nonlocal serializer_calls
        serializer_calls += 1
        print(serializer_calls, m)
        return json.dumps(m.to_dict())

    def deserialize(s: str) -> Main:
        nonlocal deserializer_calls
        deserializer_calls += 1
        return Main.from_dict(json.loads(s))

    cache = FileBackedDict[Main](
        serializer=serialize,
        deserializer=deserialize,
        # Disable the in-memory cache to force all reads/writes to the DB.
        cache_max_size=0,
        _use_sqlite_on_conflict=use_sqlite_on_conflict,
    )
    first = Main(3, {Label("one", 1): 0.1, Label("two", 2): 0.2})
    second = Main(-100, {Label("z", 26): 0.26})

    cache["first"] = first
    cache["second"] = second
    assert serializer_calls == 2
    assert deserializer_calls == 0

    assert cache["second"] == second
    assert cache["first"] == first
    assert serializer_calls == 2
    assert deserializer_calls == 2


def test_file_dict_stores_counter() -> None:
    cache = FileBackedDict[Counter[str]](
        serializer=json.dumps,
        deserializer=lambda s: Counter[str](json.loads(s)),
        cache_max_size=1,
    )

    n = 5

    # initialize
    in_memory_counters: Dict[int, Counter[str]] = {}
    for i in range(n):
        cache[str(i)] = Counter[str]()
        in_memory_counters[i] = Counter[str]()

    # increment the counters
    increments = [(i, j) for i in range(n) for j in range(n)]
    random.shuffle(increments)
    for i, j in increments:
        if i == j:
            cache[str(i)][str(j)] += 100 + j
            cache.mark_dirty(str(i))
            in_memory_counters[i][str(j)] += 100 + j
        else:
            cache.for_mutation(str(i))[str(j)] += j
            in_memory_counters[i][str(j)] += j

    for i in range(n):
        assert in_memory_counters[i] == cache[str(i)]
        assert in_memory_counters[i].most_common(2) == cache[str(i)].most_common(2)


@pytest.mark.parametrize("use_sqlite_on_conflict", [True, False])
def test_file_dict_ordering(use_sqlite_on_conflict: bool) -> None:
    """
    We require that FileBackedDict maintains insertion order, similar to Python's
    built-in dict. This test makes one of each and validates that they behave the same.
    """

    cache = FileBackedDict[int](
        serializer=str,
        deserializer=int,
        cache_max_size=1,
        _use_sqlite_on_conflict=use_sqlite_on_conflict,
    )
    data = {}

    num_items = 14

    for i in range(num_items):
        cache[str(i)] = i
        data[str(i)] = i

    assert list(cache.items()) == list(data.items())

    # Try some deletes.
    for i in range(3, num_items, 3):
        del cache[str(i)]
        del data[str(i)]

    assert list(cache.items()) == list(data.items())

    # And some updates + inserts.
    for i in range(2, num_items, 2):
        cache[str(i)] = i * 10
        data[str(i)] = i * 10

    assert list(cache.items()) == list(data.items())


@dataclass
class Pair:
    x: int
    y: str


@pytest.mark.parametrize("cache_max_size", [0, 1, 10])
@pytest.mark.parametrize("use_sqlite_on_conflict", [True, False])
def test_custom_column(cache_max_size: int, use_sqlite_on_conflict: bool) -> None:
    cache = FileBackedDict[Pair](
        extra_columns={
            "x": lambda m: m.x,
        },
        cache_max_size=cache_max_size,
        _use_sqlite_on_conflict=use_sqlite_on_conflict,
    )

    cache["first"] = Pair(3, "a")
    cache["second"] = Pair(100, "b")
    cache["third"] = Pair(27, "c")
    cache["fourth"] = Pair(100, "d")

    # Verify that the extra column is present and has the correct values.
    assert cache.sql_query(f"SELECT sum(x) FROM {cache.tablename}")[0][0] == 230

    # Verify that the extra column is updated when the value is updated.
    cache["first"] = Pair(4, "e")
    assert cache.sql_query(f"SELECT sum(x) FROM {cache.tablename}")[0][0] == 231

    # Test param binding.
    assert (
        cache.sql_query(
            f"SELECT sum(x) FROM {cache.tablename} WHERE x < ?", params=(50,)
        )[0][0]
        == 31
    )

    assert sorted(list(cache.items())) == [
        ("first", Pair(4, "e")),
        ("fourth", Pair(100, "d")),
        ("second", Pair(100, "b")),
        ("third", Pair(27, "c")),
    ]
    assert sorted(list(cache.items_snapshot())) == [
        ("first", Pair(4, "e")),
        ("fourth", Pair(100, "d")),
        ("second", Pair(100, "b")),
        ("third", Pair(27, "c")),
    ]
    assert sorted(list(cache.items_snapshot("x < 50"))) == [
        ("first", Pair(4, "e")),
        ("third", Pair(27, "c")),
    ]


@pytest.mark.parametrize("use_sqlite_on_conflict", [True, False])
def test_shared_connection(use_sqlite_on_conflict: bool) -> None:
    with ConnectionWrapper() as connection:
        cache1 = FileBackedDict[int](
            shared_connection=connection,
            tablename="cache1",
            extra_columns={
                "v": lambda v: v,
            },
            _use_sqlite_on_conflict=use_sqlite_on_conflict,
        )
        cache2 = FileBackedDict[Pair](
            shared_connection=connection,
            tablename="cache2",
            extra_columns={
                "x": lambda m: m.x,
                "y": lambda m: m.y,
            },
            _use_sqlite_on_conflict=use_sqlite_on_conflict,
        )

        cache1["a"] = 3
        cache1["b"] = 5
        cache2["ref-a-1"] = Pair(7, "a")
        cache2["ref-a-2"] = Pair(8, "a")
        cache2["ref-b-1"] = Pair(11, "b")

        assert len(cache1) == 2
        assert len(cache2) == 3

        # Test advanced SQL queries and sql_query_iterator.
        iterator = cache2.sql_query_iterator(
            f"SELECT y, sum(x) FROM {cache2.tablename} GROUP BY y ORDER BY y"
        )
        assert isinstance(iterator, sqlite3.Cursor)
        assert [tuple(r) for r in iterator] == [("a", 15), ("b", 11)]

        # Test joining between the two tables.
        rows = cache2.sql_query(
            f"""
                        SELECT cache2.y, sum(cache2.x * cache1.v) FROM {cache2.tablename} cache2
                        LEFT JOIN {cache1.tablename} cache1 ON cache1.key = cache2.y
                        GROUP BY cache2.y
                        ORDER BY cache2.y
                        """,
            refs=[cache1],
        )
        assert [tuple(row) for row in rows] == [("a", 45), ("b", 55)]

        assert list(cache2.items_snapshot('y = "a"')) == [
            ("ref-a-1", Pair(7, "a")),
            ("ref-a-2", Pair(8, "a")),
        ]

        cache2.close()

        # Check can still use cache1
        cache1["c"] = 7
        cache1.flush()
        assert cache1["c"] == 7
        cache1.close()

        # Check connection is still usable
        cur = connection.conn.execute("SELECT COUNT(*) FROM cache1")
        assert list(cur)[0][0] == 3


def test_file_list() -> None:
    my_list = FileBackedList[int](
        serializer=lambda x: x,
        deserializer=lambda x: x,
        cache_max_size=5,
        cache_eviction_batch_size=5,
    )

    # Test append + len + getitem
    for i in range(10):
        my_list.append(i)

    assert len(my_list) == 10
    assert my_list[0] == 0
    assert my_list[9] == 9

    # Test set item.
    my_list[0] = 100

    # Test iteration.
    assert list(my_list) == [100, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    # Test flush.
    my_list.flush()
    assert len(my_list) == 10
    assert list(my_list) == [100, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    # Run a SQL query.
    assert my_list.sql_query(f"SELECT sum(value) FROM {my_list.tablename}")[0][0] == 145

    # Verify error handling.
    with pytest.raises(IndexError):
        my_list[100]
    with pytest.raises(IndexError):
        my_list[-100]
    with pytest.raises(IndexError):
        my_list[100] = 100


def test_file_cleanup():
    cache = FileBackedDict[int]()
    filename = pathlib.Path(cache._conn.filename)

    cache["a"] = 3
    cache.flush()
    assert len(cache) == 1

    assert filename.exists()
    cache.close()
    assert not filename.exists()
