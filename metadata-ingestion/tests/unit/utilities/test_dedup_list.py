from datahub.utilities.dedup_list import deduplicate_list


def test_deduplicate_list_keeps_first_by_default():
    assert deduplicate_list([3, 1, 3, 2, 1]) == [3, 1, 2]


def test_deduplicate_list_keep_first_and_last_differ_in_value_not_position():
    items = [("a", "first"), ("b", "other"), ("a", "second")]

    assert deduplicate_list(items, key=lambda item: item[0]) == [
        ("a", "first"),
        ("b", "other"),
    ]
    assert deduplicate_list(items, key=lambda item: item[0], keep="last") == [
        ("a", "second"),
        ("b", "other"),
    ]


def test_deduplicate_list_keep_last_matches_dict_union():
    """keep="last" is the list equivalent of {**first, **second}."""
    first = {"a": 1, "b": 2}
    second = {"a": 9}

    merged = deduplicate_list(
        list(first.items()) + list(second.items()),
        key=lambda item: item[0],
        keep="last",
    )

    assert dict(merged) == {**first, **second}
