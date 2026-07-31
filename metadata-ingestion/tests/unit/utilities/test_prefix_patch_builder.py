import pytest

from datahub.utilities.prefix_batch_builder import PrefixGroup, build_prefix_batches


def test_build_prefix_batches_empty_input():
    assert build_prefix_batches([], 10, 5) == [[PrefixGroup(prefix="", names=[])]]


def test_build_prefix_batches_single_group():
    names = ["apple", "applet", "application"]
    expected = [[PrefixGroup(prefix="", names=names)]]
    assert build_prefix_batches(names, 10, 5) == expected


def test_build_prefix_batches_multiple_groups():
    names = ["apple", "applet", "banana", "band", "bandana"]
    expected = [
        [PrefixGroup(prefix="a", names=["apple", "applet"])],
        [PrefixGroup(prefix="b", names=["banana", "band", "bandana"])],
    ]
    assert build_prefix_batches(names, 4, 5) == expected


def test_build_prefix_batches_exceeds_max_batch_size():
    names = [
        "app",
        "apple",
        "applet",
        "application",
        "banana",
        "band",
        "bandana",
        "candy",
        "candle",
        "dog",
    ]
    expected = [
        [PrefixGroup(prefix="app", names=["app"], exact_match=True)],
        [PrefixGroup(prefix="appl", names=["apple", "applet", "application"])],
        [PrefixGroup(prefix="b", names=["banana", "band", "bandana"])],
        [
            PrefixGroup(prefix="c", names=["candle", "candy"]),
            PrefixGroup(prefix="d", names=["dog"]),
        ],
    ]
    assert build_prefix_batches(names, 3, 2) == expected


@pytest.mark.parametrize(
    "names,max_batch_size,max_groups_in_batch",
    [
        # The original Snowflake regression: A=681, B=55, C=55 packs as
        # [[A, B], [C]] under max_batch_size=1000. A caller that took only
        # batch[0] from each batch would silently lose every B-prefixed name.
        (
            [f"A{i:04d}" for i in range(681)]
            + [f"B{i:02d}" for i in range(55)]
            + [f"C{i:02d}" for i in range(55)],
            1000,
            1,
        ),
        # Smaller batch size forces deeper recursive splits.
        (
            [f"A{i:04d}" for i in range(681)]
            + [f"B{i:02d}" for i in range(55)]
            + [f"C{i:02d}" for i in range(55)],
            100,
            1,
        ),
        # Production column-fetch parameters.
        ([f"NAME_{i:05d}" for i in range(50000)], 10000, 6),
        # Pathological: many tiny groups that all want to pair up.
        ([f"{c}{i}" for c in "ABCDEFGHIJ" for i in range(3)], 10, 1),
        ([f"{c}{i}" for c in "ABCDEFGHIJ" for i in range(3)], 10, 3),
        # Single group degenerate case.
        (["only"], 10, 1),
        # Empty input.
        ([], 10, 1),
    ],
)
def test_build_prefix_batches_preserves_all_names(
    names, max_batch_size, max_groups_in_batch
):
    """Every input name must appear in exactly one PrefixGroup across all batches.

    This invariant — the union of all `group.names` equals the input set —
    must hold for every parameter combination, regardless of how callers
    iterate the output or how the packer chooses to bin groups. It is the
    contract anyone batching SHOW-statement queries relies on, and would
    have caught the silent view-drop bug fixed in the parent commit.
    """
    batches = build_prefix_batches(
        names, max_batch_size=max_batch_size, max_groups_in_batch=max_groups_in_batch
    )

    emitted = [name for batch in batches for group in batch for name in group.names]
    assert sorted(emitted) == sorted(names), (
        f"name set changed: missing={set(names) - set(emitted)}, "
        f"duplicated={[n for n in emitted if emitted.count(n) > 1][:5]}"
    )

    # Each group's names must actually share the group's declared prefix —
    # otherwise a caller issuing `LIKE '<prefix>%'` would fetch the wrong rows.
    for batch in batches:
        for group in batch:
            for name in group.names:
                assert name.startswith(group.prefix), (
                    f"name {name!r} does not start with group prefix {group.prefix!r}"
                )

    # No batch may exceed the declared max_groups_in_batch — protects against
    # the off-by-one that originally let `max=1` pack 2 groups per batch.
    for batch in batches:
        assert len(batch) <= max_groups_in_batch, (
            f"batch has {len(batch)} groups but max_groups_in_batch={max_groups_in_batch}"
        )
