from typing import Dict, List
from unittest import mock

import pytest

from datahub.utilities.server_config_util import RestServiceConfig
from datahub.utilities.urn_alias.remote import (
    gms_maintains_urn_aliases,
    search_aliases,
)

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"


def _graph(*matches: str) -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_urns_by_filter.return_value = iter(matches)
    return graph


def _queried(graph: mock.MagicMock) -> Dict[str, List[str]]:
    """The key the search asked for, by the field it was asked under."""
    _, kwargs = graph.get_urns_by_filter.call_args
    rules = [group["and"][0] for group in kwargs["extra_or_filters"]]
    return {rule["field"]: rule["values"] for rule in rules}


# --- the search ------------------------------------------------------------------------


def test_the_search_asks_under_both_fields() -> None:
    # GMS writes aliases asynchronously, so one that has not landed yet is findable only
    # under `urn`. Both clauses are OR'd into a single query.
    graph = _graph(_UPPER)

    assert search_aliases(graph, _LOWER) == [_UPPER]
    assert _queried(graph) == {"lowercasedUrn": [_LOWER], "urn": [_LOWER]}
    graph.get_urns_by_filter.assert_called_once()


def test_a_repeated_urn_is_not_a_casing_collision() -> None:
    # Both fields can match the same entity, and two hits would read as two casings.
    assert search_aliases(_graph(_LOWER, _LOWER), _LOWER) == [_LOWER]


def test_a_name_the_server_does_not_hold_answers_empty() -> None:
    assert search_aliases(_graph(), _LOWER) == []


# --- the gate on the whole feature -----------------------------------------------------


def _server(version: str, cloud: bool = False) -> mock.MagicMock:
    """A graph reporting itself as DataHub `version`."""
    graph = mock.MagicMock()
    graph.server_config = RestServiceConfig(
        raw_config={
            "versions": {"acryldata/datahub": {"version": version}},
            "datahub": {"serverEnv": "prod" if cloud else ""},
        }
    )
    return graph


@pytest.mark.parametrize(
    ("version", "cloud", "supported"),
    [
        ("v1.8.0", False, True),
        ("v1.9.2", False, True),
        ("v1.7.0", False, False),
        ("v2.2.0", True, True),
        ("v2.1.0", True, False),
    ],
)
def test_the_feature_needs_a_server_that_maintains_aliases(
    version: str, cloud: bool, supported: bool
) -> None:
    # Cloud and OSS number separately, so each has its own floor.
    assert gms_maintains_urn_aliases(_server(version, cloud)) is supported


def test_a_server_that_reports_no_version_is_not_assumed_new() -> None:
    graph = mock.MagicMock()
    graph.server_config = RestServiceConfig(raw_config={})
    assert gms_maintains_urn_aliases(graph) is False
