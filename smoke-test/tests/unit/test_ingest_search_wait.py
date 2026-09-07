import json
from unittest.mock import MagicMock

import pytest

from tests.utilities.domains import Domain
from tests.utils import (
    entity_urns_from_ingest_file,
    searchable_ingest_urns,
    wait_for_browse_path_entities,
    wait_for_browse_path_entity,
    wait_for_ingested_urns_searchable,
)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.domain(Domain.PLATFORM)]

SNAPSHOT_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-3,PROD)"
MCP_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)"
ASSERTION_URN = "urn:li:assertion:test-assertion"
INCIDENT_URN = "urn:li:incident:test-incident"
PLATFORM_URN = "urn:li:dataPlatform:postgres"
ENTITY_TYPE_URN = "urn:li:entityType:dataset"
QUERY_URN = "urn:li:query:test-query"
POST_URN = "urn:li:post:test-post"
STRUCTURED_PROPERTY_URN = "urn:li:structuredProperty:io.datahub.test.formsSmokeProperty"
DPI_URN = "urn:li:dataProcessInstance:test-dpi"
DPI_PLATFORM_URN = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:airflow,prod)"


def test_entity_urns_from_snapshot_and_mcp(tmp_path):
    payload = [
        {
            "proposedSnapshot": {
                "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
                    "urn": SNAPSHOT_URN,
                    "aspects": [],
                }
            }
        },
        {"entityUrn": MCP_URN, "aspectName": "datasetProperties"},
        {"entityUrn": MCP_URN, "aspectName": "schemaMetadata"},
    ]
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps(payload))

    assert entity_urns_from_ingest_file(str(path)) == [SNAPSHOT_URN, MCP_URN]


def test_wait_for_ingested_urns_searchable_retries_until_found(tmp_path, monkeypatch):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    search_calls: list = []

    def fake_search(auth_session, urns):
        search_calls.append(list(urns))
        if len(search_calls) < 2:
            return set()
        return {MCP_URN}

    monkeypatch.setattr("tests.utils._search_results_contain_urns", fake_search)

    wait_for_ingested_urns_searchable(MagicMock(), str(path))

    assert search_calls == [[MCP_URN], [MCP_URN]]
    assert sleeps == [1]


def test_searchable_ingest_urns_skips_non_catalog_types():
    mixed = [
        MCP_URN,
        ASSERTION_URN,
        INCIDENT_URN,
        PLATFORM_URN,
        ENTITY_TYPE_URN,
        QUERY_URN,
        POST_URN,
        STRUCTURED_PROPERTY_URN,
        DPI_URN,
        DPI_PLATFORM_URN,
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD),col)",
    ]
    assert searchable_ingest_urns(mixed) == [MCP_URN]


def test_wait_for_ingested_urns_searchable_skips_non_searchable_types(
    tmp_path, monkeypatch
):
    path = tmp_path / "ingest.json"
    path.write_text(
        json.dumps(
            [
                {"entityUrn": ASSERTION_URN},
                {"entityUrn": INCIDENT_URN},
                {"entityUrn": PLATFORM_URN},
                {"entityUrn": ENTITY_TYPE_URN},
                {"entityUrn": MCP_URN},
            ]
        )
    )

    search_calls: list = []

    def fake_search(auth_session, urns):
        search_calls.append(list(urns))
        return {MCP_URN}

    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", lambda seconds: None)
    monkeypatch.setattr("tests.utils._search_results_contain_urns", fake_search)

    wait_for_ingested_urns_searchable(MagicMock(), str(path))

    assert search_calls == [[MCP_URN]]


def test_wait_for_ingested_urns_searchable_retries_when_graphql_errors(
    tmp_path, monkeypatch
):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        {"errors": [{"message": "boom"}]},
        {
            "data": {
                "searchAcrossEntities": {
                    "searchResults": [{"entity": {"urn": MCP_URN}}]
                }
            }
        },
    ]

    def fake_graphql(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_ingested_urns_searchable(MagicMock(), str(path))

    assert sleeps == [1]
    assert responses == []


def test_wait_for_ingested_urns_searchable_retries_when_graphql_raises(
    tmp_path, monkeypatch
):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        ConnectionError("network down"),
        {
            "data": {
                "searchAcrossEntities": {
                    "searchResults": [{"entity": {"urn": MCP_URN}}]
                }
            }
        },
    ]

    def fake_graphql(*args, **kwargs):
        next_response = responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_ingested_urns_searchable(MagicMock(), str(path))

    assert sleeps == [1]
    assert responses == []


def test_wait_for_ingested_urns_searchable_times_out_when_graphql_errors(
    tmp_path, monkeypatch
):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (0, 2))
    monkeypatch.setattr("tests.utils.time.sleep", lambda seconds: None)
    monkeypatch.setattr(
        "tests.utils.execute_graphql",
        lambda *args, **kwargs: {"errors": [{"message": "boom"}]},
    )

    with pytest.raises(AssertionError, match="not searchable"):
        wait_for_ingested_urns_searchable(MagicMock(), str(path))


def test_wait_for_ingested_urns_searchable_times_out(tmp_path, monkeypatch):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": MCP_URN}]))

    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (0, 2))
    monkeypatch.setattr("tests.utils.time.sleep", lambda seconds: None)
    monkeypatch.setattr(
        "tests.utils._search_results_contain_urns", lambda *args, **kwargs: set()
    )

    with pytest.raises(AssertionError, match="not searchable"):
        wait_for_ingested_urns_searchable(MagicMock(), str(path))


def test_wait_for_ingested_urns_searchable_empty_when_only_non_searchable(
    tmp_path, monkeypatch
):
    path = tmp_path / "ingest.json"
    path.write_text(json.dumps([{"entityUrn": ASSERTION_URN}]))

    def fail_search(*args, **kwargs):
        raise AssertionError("should not search non-searchable types")

    monkeypatch.setattr("tests.utils._search_results_contain_urns", fail_search)
    wait_for_ingested_urns_searchable(MagicMock(), str(path))


def test_wait_for_browse_path_entity_retries_until_found(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        {"data": {"browse": {"entities": []}}},
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}]}}},
    ]

    def fake_graphql(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_browse_path_entity(
        MagicMock(),
        path=["prod"],
        expected_urn=SNAPSHOT_URN,
        entity_type="DATASET",
    )
    assert sleeps == [1]
    assert responses == []


def test_wait_for_browse_path_entities_waits_for_all_urns(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    other = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-1,PROD)"
    responses = [
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}]}}},
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}, {"urn": other}]}}},
    ]

    def fake_graphql(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_browse_path_entities(
        MagicMock(),
        path=["prod", "kafka1"],
        expected_urns=[SNAPSHOT_URN, other],
        entity_type="DATASET",
    )
    assert sleeps == [1]
    assert responses == []


def test_wait_for_browse_path_entities_retries_on_graphql_errors(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        {"errors": [{"message": "boom"}]},
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}]}}},
    ]

    def fake_graphql(*args, **kwargs):
        assert kwargs.get("expect_errors") is True
        return responses.pop(0)

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_browse_path_entities(
        MagicMock(),
        path=["prod"],
        expected_urns=[SNAPSHOT_URN],
        entity_type="DATASET",
    )
    assert sleeps == [1]
    assert responses == []


def test_wait_for_browse_path_entities_retries_when_graphql_raises(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr("tests.utils.get_sleep_info", lambda: (1, 3))
    monkeypatch.setattr("tests.utils.time.sleep", sleeps.append)

    responses = [
        ConnectionError("network down"),
        {"data": {"browse": {"entities": [{"urn": SNAPSHOT_URN}]}}},
    ]

    def fake_graphql(*args, **kwargs):
        assert kwargs.get("expect_errors") is True
        next_response = responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response

    monkeypatch.setattr("tests.utils.execute_graphql", fake_graphql)

    wait_for_browse_path_entities(
        MagicMock(),
        path=["prod"],
        expected_urns=[SNAPSHOT_URN],
        entity_type="DATASET",
    )
    assert sleeps == [1]
    assert responses == []
