import random
import re
import time

import pytest

from datahub.utilities.lossy_collections import LossyDict, LossyList, LossySet


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossylist_sampling(length, sampling):
    l_dict: LossyList[str] = LossyList()
    for i in range(0, length):
        l_dict.append(f"{i} Hello World")

    assert len(l_dict) == length
    assert l_dict.sampled is sampling
    if sampling:
        assert f"... sampled of {length} total elements" in str(l_dict)
    else:
        assert "sampled" not in str(l_dict)

    list_version = [int(i.split(" ")[0]) for i in l_dict]
    print(list_version)
    assert sorted(list_version) == list_version


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossyset_sampling(length, sampling):
    lossy_set: LossySet[str] = LossySet()
    for i in range(0, length):
        lossy_set.add(f"{i} Hello World")

    assert len(lossy_set) == min(10, length)
    assert lossy_set.sampled is sampling
    if sampling:
        assert f"... sampled with at most {length - 10} elements missing" in str(
            lossy_set
        )
    else:
        assert "sampled" not in str(lossy_set)

    list_version = [int(i.split(" ")[0]) for i in lossy_set]
    set_version = set(list_version)

    assert len(list_version) == len(set_version)
    assert len(list_version) == min(10, length)


@pytest.mark.parametrize(
    "length, sampling, sub_length", [(4, False, 4), (10, False, 14), (100, True, 1000)]
)
def test_lossydict_sampling(length, sampling, sub_length):
    lossy_dict: LossyDict[int, LossyList[str]] = LossyDict()
    elements_added = 0
    element_length_map = {}
    for i in range(0, length):
        list_length = random.choice(range(1, sub_length))
        element_length_map[i] = 0
        for _num_elements in range(0, list_length):
            if not lossy_dict.get(i):
                elements_added += 1
                # reset to 0 until we get it back
                element_length_map[i] = 0
            else:
                element_length_map[i] = len(lossy_dict[i])

            current_list = lossy_dict.get(i, LossyList())
            current_list.append(f"{i}:{round(time.time(), 2)} Hello World")
            lossy_dict[i] = current_list
            element_length_map[i] += 1

    assert len(lossy_dict) == min(lossy_dict.max_elements, length)
    assert lossy_dict.sampled is sampling
    if sampling:
        assert re.search("sampled of at most .* entries.", str(lossy_dict))
        assert (
            f"{lossy_dict.max_elements} sampled of at most {elements_added} entries."
            in str(lossy_dict)
        )
    else:
        # cheap way to determine that the dict isn't reporting sampled keys
        assert not re.search("sampled of at most .* entries.", str(lossy_dict))

    for k, v in lossy_dict.items():
        assert len(v) == element_length_map[k]


def test_lossylist_getitem_direct_indexing():
    """Test that direct indexing returns unwrapped items, not tuples.

    Regression test for issue where LossyList stored items as (index, item) tuples
    internally for reservoir sampling, but only unwrapped during iteration, not
    during direct indexing.
    """
    lossy_list: LossyList[str] = LossyList(max_elements=5)
    lossy_list.append("first")
    lossy_list.append("second")
    lossy_list.append("third")

    # Direct indexing should return the item, not a tuple
    assert lossy_list[0] == "first"
    assert lossy_list[1] == "second"
    assert lossy_list[2] == "third"
    assert lossy_list[-1] == "third"

    # Type should be str, not tuple
    assert isinstance(lossy_list[0], str)
    assert not isinstance(lossy_list[0], tuple)


def test_lossylist_getitem_slicing():
    """Test that slicing returns list of unwrapped items."""
    lossy_list: LossyList[str] = LossyList(max_elements=5)
    lossy_list.append("a")
    lossy_list.append("b")
    lossy_list.append("c")
    lossy_list.append("d")

    # Slicing should return unwrapped items
    assert lossy_list[0:2] == ["a", "b"]
    assert lossy_list[1:3] == ["b", "c"]
    assert lossy_list[::2] == ["a", "c"]
    assert lossy_list[:] == ["a", "b", "c", "d"]

    # Slice result should be list of items, not tuples
    slice_result = lossy_list[0:2]
    assert all(isinstance(item, str) for item in slice_result)
    assert not any(isinstance(item, tuple) for item in slice_result)


def test_lossylist_getitem_negative_indexing():
    """Test that negative indexing works correctly."""
    lossy_list: LossyList[int] = LossyList(max_elements=5)
    for i in range(5):
        lossy_list.append(i * 10)

    # Negative indexing
    assert lossy_list[-1] == 40
    assert lossy_list[-2] == 30
    assert lossy_list[-5] == 0

    # Negative slicing
    assert lossy_list[-3:] == [20, 30, 40]
    assert lossy_list[:-2] == [0, 10, 20]


def test_lossylist_getitem_with_reservoir_sampling():
    """Test that __getitem__ works correctly even after reservoir sampling kicks in."""
    lossy_list: LossyList[str] = LossyList(max_elements=5)

    # Add more items than max_elements to trigger reservoir sampling
    for i in range(20):
        lossy_list.append(f"item_{i}")

    # Should have sampled
    assert lossy_list.sampled is True
    assert len(lossy_list) == 20  # total_elements

    # Direct indexing should still return unwrapped items
    # We can't predict which items are sampled, but they should be strings, not tuples
    for i in range(min(5, len(list(lossy_list)))):
        item = list(lossy_list)[i]
        assert isinstance(item, str)
        assert not isinstance(item, tuple)
        assert item.startswith("item_")


def test_lossylist_iteration_consistency_with_indexing():
    """Test that iteration and indexing return the same items in the same order."""
    lossy_list: LossyList[str] = LossyList(max_elements=10)
    items = ["apple", "banana", "cherry", "date", "elderberry"]
    for item in items:
        lossy_list.append(item)

    # Items from iteration
    iterated_items = list(lossy_list)

    # Items from indexing
    indexed_items = [lossy_list[i] for i in range(len(iterated_items))]

    # Should be identical
    assert iterated_items == indexed_items
    assert iterated_items == items


def test_lossylist_getitem_with_structured_log_entries():
    """Test that __getitem__ works with complex types like StructuredLogEntry.

    This is a regression test for the actual bug that was discovered in
    Snowplow connector tests where report.failures[0] returned a tuple
    instead of a StructuredLogEntry.
    """
    from datahub.ingestion.api.source import StructuredLogEntry
    from datahub.utilities.lossy_collections import LossyList

    lossy_list: LossyList[StructuredLogEntry] = LossyList(max_elements=10)

    # Add some structured log entries
    context1: LossyList[str] = LossyList()
    context1.append("ctx1")
    entry1 = StructuredLogEntry(
        title="Error 1", message="Test error 1", context=context1
    )

    context2: LossyList[str] = LossyList()
    context2.append("ctx2")
    entry2 = StructuredLogEntry(
        title="Error 2", message="Test error 2", context=context2
    )

    context3: LossyList[str] = LossyList()
    context3.append("ctx3")
    entry3 = StructuredLogEntry(
        title="Error 3", message="Test error 3", context=context3
    )

    lossy_list.append(entry1)
    lossy_list.append(entry2)
    lossy_list.append(entry3)

    # Direct indexing should return StructuredLogEntry, not tuple
    retrieved = lossy_list[0]
    assert isinstance(retrieved, StructuredLogEntry)
    assert not isinstance(retrieved, tuple)
    assert retrieved.title == "Error 1"
    assert retrieved.message == "Test error 1"

    # Slicing should return list of StructuredLogEntry
    slice_result = lossy_list[0:2]
    assert len(slice_result) == 2
    assert all(isinstance(item, StructuredLogEntry) for item in slice_result)
    assert slice_result[0].title == "Error 1"
    assert slice_result[1].title == "Error 2"
