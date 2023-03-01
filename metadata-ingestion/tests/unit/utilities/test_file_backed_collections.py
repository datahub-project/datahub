import json
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Dict

import pytest

from datahub.utilities.file_backed_collections import FileBackedDict


def test_file_dict():
    cache = FileBackedDict[int](
        serializer=lambda x: x,
        deserializer=lambda x: x,
        cache_max_size=10,
        cache_eviction_batch_size=10,
    )

    for i in range(100):
        cache[f"key-{i}"] = i

    assert len(cache) == 100
    assert sorted(cache) == sorted([f"key-{i}" for i in range(100)])

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


def test_file_dict_serialization():
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
            str_y = {json.dumps(asdict(k)): v for k, v in self.y.items()}
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
        cache_max_size=0,
        cache_eviction_batch_size=0,
    )
    first = Main(3, {Label("one", 1): 0.1, Label("two", 2): 0.2})
    second = Main(-100, {Label("z", 26): 0.26})

    cache["first"] = first
    cache["second"] = second
    assert serializer_calls == 2
    assert deserializer_calls == 0

    assert cache["second"] == second
    assert cache["first"] == first
    assert serializer_calls == 4  # Items written to cache on every access
    assert deserializer_calls == 2


def test_file_dict_stores_counter():
    cache = FileBackedDict[Counter[str]](
        serializer=json.dumps,
        deserializer=lambda s: Counter(json.loads(s)),
        cache_max_size=1,
        cache_eviction_batch_size=0,
    )

    n = 5
    in_memory_counters: Dict[int, Counter[str]] = {}
    for i in range(n):
        cache[str(i)] = Counter()
        in_memory_counters[i] = Counter()
        for j in range(n):
            if i == j:
                cache[str(i)][str(j)] += 100
                in_memory_counters[i][str(j)] += 100
            cache[str(i)][str(j)] += j
            in_memory_counters[i][str(j)] += j

    for i in range(n):
        assert in_memory_counters[i] == cache[str(i)]
        assert in_memory_counters[i].most_common(2) == cache[str(i)].most_common(2)
