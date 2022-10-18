from collections import defaultdict

from datahub.utilities import memory_footprint


def test_total_size_with_empty_dict():
    size = memory_footprint.total_size({})
    assert size == 64


def test_total_size_with_list():
    size = memory_footprint.total_size({"1": [1, 2, 3, 4]})
    assert size == 482


def test_total_size_with_none():
    size = memory_footprint.total_size(None)
    assert size == 16


def test_total_size_with_defaultdict():
    size = memory_footprint.total_size(defaultdict)
    assert size == 416
