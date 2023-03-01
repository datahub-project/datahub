import pytest

from datahub.utilities.file_backed_collections import FileBackedDict


def test_file_dict():
    cache = FileBackedDict[int](cache_max_size=10, cache_eviction_batch_size=10)

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

    # Test deleting a key.
    del cache["key-0"]
    assert len(cache) == 99
    with pytest.raises(KeyError):
        cache["key-0"]

    # Test deleting a key that doesn't exist.
    with pytest.raises(KeyError):
        del cache["key-0"]

    # Test adding another key.
    cache["a"] = 1
    assert cache["a"] == 1
    assert len(cache) == 100
    assert sorted(cache) == sorted(["a"] + [f"key-{i}" for i in range(1, 100)])

    # Test deleting most things.
    for i in range(1, 100):
        assert cache[f"key-{i}"] == i
        del cache[f"key-{i}"]
    assert len(cache) == 1
    assert cache["a"] == 1
