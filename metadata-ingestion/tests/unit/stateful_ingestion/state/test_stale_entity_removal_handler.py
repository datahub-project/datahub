from typing import Dict, List, Tuple

import pytest

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)

OldNewEntLists = List[Tuple[List[str], List[str]]]

old_new_ent_tests: Dict[str, Tuple[OldNewEntLists, float]] = {
    "no_change_empty_old_and_new": ([([], [])], 0.0),
    "no_change_empty_old_and_non_empty_new": ([(["a"], [])], 0.0),
    "no_change_non_empty_old_new_equals_old": (
        [(["a", "b", "c"], ["c", "b", "a"])],
        0.0,
    ),
    "no_change_non_empty_old_new_superset_old": (
        [(["a", "b", "c", "d"], ["c", "b", "a"])],
        0.0,
    ),
    "change_25_percent_delta": ([(["a", "b", "c"], ["d", "c", "b", "a"])], 25.0),
    "change_50_percent_delta": (
        [
            (
                ["b", "a"],
                ["a", "b", "c", "d"],
            )
        ],
        50.0,
    ),
    "change_75_percent_delta": ([(["a"], ["a", "b", "c", "d"])], 75.0),
    "change_100_percent_delta_empty_new": ([([], ["a", "b", "c", "d"])], 100.0),
    "change_100_percent_delta_non_empty_new": ([(["e"], ["a", "b", "c", "d"])], 100.0),
}


@pytest.mark.parametrize(
    "new_old_entity_list, expected_percent_change",
    old_new_ent_tests.values(),
    ids=old_new_ent_tests.keys(),
)
def test_change_percent(
    new_old_entity_list: OldNewEntLists, expected_percent_change: float
) -> None:
    actual_percent_change = (
        StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            new_old_entity_list
        )
    )
    assert actual_percent_change == expected_percent_change
