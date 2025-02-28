from datahub.utilities.ordered_set import OrderedSet


def test_ordered_set():
    # Test initialization
    ordered_set: OrderedSet[int] = OrderedSet()
    assert len(ordered_set) == 0

    # Test adding items
    ordered_set.add(1)
    ordered_set.add(2)
    ordered_set.add(3)
    assert len(ordered_set) == 3

    # Test adding duplicate item
    ordered_set.add(1)
    assert len(ordered_set) == 3

    # Test discarding item
    ordered_set.discard(2)
    assert len(ordered_set) == 2
    assert 2 not in ordered_set

    # Test updating with iterable
    ordered_set.update([4, 5, 6])
    assert len(ordered_set) == 5
    assert 4 in ordered_set
    assert 5 in ordered_set
    assert 6 in ordered_set

    # Test containment check
    assert 3 in ordered_set
    assert 7 not in ordered_set

    # Test iteration
    items = list(ordered_set)
    assert items == [1, 3, 4, 5, 6]

    # Test reverse iteration
    items = list(reversed(ordered_set))
    assert items == [6, 5, 4, 3, 1]

    # Test string representation
    assert repr(ordered_set) == "OrderedSet([1, 3, 4, 5, 6])"
