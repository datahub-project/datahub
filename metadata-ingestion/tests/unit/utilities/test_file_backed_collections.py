import pytest

from datahub.utilities.file_backed_collections import FileBackedDict


def test_file_dict():
    cache = FileBackedDict[int]()

    for i in range(100):
        cache[f"key-{i}"] = i

    assert len(cache) == 100

    for i in range(100):
        assert cache[f"key-{i}"] == i

    del cache["key-0"]
    assert len(cache) == 99
    with pytest.raises(KeyError):
        cache["key-0"]

    cache["a"] = 1
    assert cache["a"] == 1
    assert len(cache) == 100
