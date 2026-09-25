"""Unit tests for Dataplex identity primitives.

Mapping-oriented behavior (entry type -> DataHub entities) is tested in
``test_dataplex_mappers.py``; this module covers only the low-level identity
building blocks that live in ``dataplex_ids.py``.
"""

from typing import Optional

import pytest

from datahub.ingestion.source.dataplex.dataplex_ids import (
    BIGQUERY_TABLE_FQN_REGEX,
    PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM,
    PUBSUB_TOPIC_FQN_REGEX,
    VERTEX_AI_FEATURE_GROUP_FQN_REGEX,
    VERTEX_AI_MODEL_FQN_REGEX,
    DataplexBigQueryProject,
    DataplexCloudSqlMySqlDatabase,
    DataplexProjectId,
    extract_entry_type_short_name,
    instantiate_key,
    parse_with_regex,
)


@pytest.mark.parametrize(
    "entry_type,expected_short_name",
    [
        (
            "projects/655216118709/locations/global/entryTypes/bigquery-table",
            "bigquery-table",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/cloud-spanner-instance",
            "cloud-spanner-instance",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/pubsub-topic",
            "pubsub-topic",
        ),
    ],
)
def test_extract_entry_type_short_name(
    entry_type: str, expected_short_name: str
) -> None:
    assert extract_entry_type_short_name(entry_type) == expected_short_name


def test_extract_entry_type_short_name_invalid() -> None:
    assert extract_entry_type_short_name("bigquery-table") is None


def test_parse_with_regex_named_groups() -> None:
    assert parse_with_regex(BIGQUERY_TABLE_FQN_REGEX, "bigquery:proj.ds.tbl") == {
        "project_id": "proj",
        "dataset_id": "ds",
        "table_id": "tbl",
    }


def test_parse_with_regex_no_match() -> None:
    assert parse_with_regex(BIGQUERY_TABLE_FQN_REGEX, "bigquery:proj.ds") is None


def test_instantiate_key_ignores_unknown_fields() -> None:
    key = instantiate_key(
        DataplexBigQueryProject,
        {"project_id": "proj", "table_id": "ignored"},
    )
    assert isinstance(key, DataplexBigQueryProject)
    assert key.project_id == "proj"
    assert key.as_urn().startswith("urn:li:container:")


def test_schema_key_parent_chain_terminates_at_project() -> None:
    database_key = instantiate_key(
        DataplexCloudSqlMySqlDatabase,
        {
            "project_id": "proj",
            "location": "us-west2",
            "instance_id": "inst",
            "database_id": "db",
        },
    )
    instance_key = database_key.parent_key()
    assert instance_key is not None
    project_key = instance_key.parent_key()
    assert project_key is not None
    # Project key is the root of the platform-specific hierarchy.
    assert project_key.parent_key() is None


def test_project_schema_key_class_by_platform_covers_supported_platforms() -> None:
    assert set(PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM.keys()) == {
        "bigquery",
        "cloudsql",
        "spanner",
        "pubsub",
        "bigtable",
        "vertexai",
        "dataproc-metastore",
    }
    for key_class in PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM.values():
        assert issubclass(key_class, DataplexProjectId)


@pytest.mark.parametrize(
    "fqn,expected",
    [
        (
            "pubsub:topic:my-project.my-topic",
            {"project_id": "my-project", "topic_id": "my-topic"},
        ),
        # Topic ids may legally contain periods; project ids cannot, so the
        # first dot is still an unambiguous delimiter.
        (
            "pubsub:topic:my-project.orders.v2",
            {"project_id": "my-project", "topic_id": "orders.v2"},
        ),
        ("pubsub:topic:my-project", None),
    ],
)
def test_pubsub_topic_fqn_accepts_dotted_topic_ids(
    fqn: str, expected: Optional[dict]
) -> None:
    assert parse_with_regex(PUBSUB_TOPIC_FQN_REGEX, fqn) == expected


@pytest.mark.parametrize(
    "fqn,expected",
    [
        (
            "vertex_ai:featuregroup:my-project.us-west2.my_group",
            {
                "project_id": "my-project",
                "location": "us-west2",
                "feature_group_id": "my_group",
            },
        ),
        (
            "vertex_ai:featureonlinestore:my-project.us-west2.my_store",
            None,
        ),
    ],
)
def test_vertex_ai_feature_group_fqn(fqn: str, expected: Optional[dict]) -> None:
    assert parse_with_regex(VERTEX_AI_FEATURE_GROUP_FQN_REGEX, fqn) == expected


def test_vertex_ai_model_fqn_carries_the_version() -> None:
    assert parse_with_regex(
        VERTEX_AI_MODEL_FQN_REGEX, "vertex_ai:model:my-project.us-west2.123456.1"
    ) == {
        "project_id": "my-project",
        "location": "us-west2",
        "model_id": "123456",
        "version": "1",
    }
