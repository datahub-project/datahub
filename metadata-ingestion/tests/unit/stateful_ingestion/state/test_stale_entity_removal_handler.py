from typing import Dict, List, Tuple

import pytest

from datahub.ingestion.source.state.entity_removal_state import (
    compute_percent_entities_changed,
    filter_ignored_entity_types,
)

EntList = List[str]
OldNewEntLists = List[Tuple[List[str], List[str]]]

new_old_ent_tests: Dict[str, Tuple[EntList, EntList, float]] = {
    "no_change_empty_old_and_new": ([], [], 0.0),
    "no_change_empty_old_and_non_empty_new": (["a"], [], 0.0),
    "no_change_non_empty_old_new_equals_old": (
        ["a", "b", "c"],
        ["c", "b", "a"],
        0.0,
    ),
    "no_change_non_empty_old_new_superset_old": (
        ["a", "b", "c", "d"],
        ["c", "b", "a"],
        0.0,
    ),
    "change_25_percent_delta": (["a", "b", "c"], ["d", "c", "b", "a"], 25.0),
    "change_50_percent_delta": (
        ["b", "a"],
        ["a", "b", "c", "d"],
        50.0,
    ),
    "change_75_percent_delta": (["a"], ["a", "b", "c", "d"], 75.0),
    "change_100_percent_delta_empty_new": ([], ["a", "b", "c", "d"], 100.0),
    "change_100_percent_delta_non_empty_new": (["e"], ["a", "b", "c", "d"], 100.0),
}


@pytest.mark.parametrize(
    "new_entities, old_entities, expected_percent_change",
    new_old_ent_tests.values(),
    ids=new_old_ent_tests.keys(),
)
def test_change_percent(
    new_entities: EntList, old_entities: EntList, expected_percent_change: float
) -> None:
    actual_percent_change = compute_percent_entities_changed(
        new_entities=new_entities, old_entities=old_entities
    )
    assert actual_percent_change == expected_percent_change


def test_filter_ignored_entity_types():
    assert filter_ignored_entity_types(
        [
            "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset3,PROD)",
            "urn:li:dataProcessInstance:478810e859f870a54f72c681f41af619",
            "urn:li:query:query1",
        ]
    ) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,dummy_dataset3,PROD)",
    ]
