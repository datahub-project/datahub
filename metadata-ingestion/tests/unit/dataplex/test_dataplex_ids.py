"""Unit tests for Dataplex identity and entry-type mapping utilities."""

import pytest

from datahub.ingestion.source.dataplex.dataplex_ids import (
    DATAPLEX_ENTRY_TYPE_MAPPINGS,
    DataplexBigQueryDataset,
    DataplexCloudSpannerDatabase,
    DataplexCloudSqlMySqlDatabase,
    DataplexProjectId,
    DataplexPubSubTopic,
    build_container_urn_from_fqn,
    build_dataset_urn_from_fqn,
    build_parent_container_urn,
    build_parent_schema_key,
    build_project_container_urn_from_fqn,
    build_project_schema_key_from_fqn,
    build_schema_key_from_fqn,
    extract_entry_type_short_name,
    parse_fully_qualified_name,
    parse_parent_entry,
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


def test_supported_entry_type_mapping_keys() -> None:
    assert set(DATAPLEX_ENTRY_TYPE_MAPPINGS.keys()) == {
        "bigquery-dataset",
        "bigquery-table",
        "cloudsql-mysql-instance",
        "cloudsql-mysql-database",
        "cloudsql-mysql-table",
        "cloud-spanner-instance",
        "cloud-spanner-database",
        "cloud-spanner-table",
        "pubsub-topic",
    }


def test_mapping_regex_groups_match_schema_key_fields() -> None:
    """Protect regex<->SchemaKey coupling in mapping definitions.

    Dataplex parsing relies on a strict contract: regex named groups map directly
    to SchemaKey constructor fields. If someone renames a key field or a regex
    group without updating the other side, parsing can silently degrade. This
    test fails fast for every mapping entry when that contract drifts.
    """
    for entry_type_short_name, mapping in DATAPLEX_ENTRY_TYPE_MAPPINGS.items():
        fqn_group_names = set(mapping.fqn_regex.groupindex.keys())
        schema_field_names = set(mapping.schema_key_class.model_fields.keys())
        assert fqn_group_names.issubset(schema_field_names), (
            f"{entry_type_short_name}: fqn_regex groups {fqn_group_names} "
            f"must be subset of {mapping.schema_key_class.__name__} fields "
            f"{schema_field_names}"
        )

        if mapping.parent_entry_regex is None:
            continue

        assert mapping.parent_schema_key_class is not None, (
            f"{entry_type_short_name}: parent_entry_regex exists but "
            "parent_schema_key_class is missing"
        )
        parent_group_names = set(mapping.parent_entry_regex.groupindex.keys())
        parent_schema_field_names = set(
            mapping.parent_schema_key_class.model_fields.keys()
        )
        assert parent_group_names.issubset(parent_schema_field_names), (
            f"{entry_type_short_name}: parent_entry_regex groups {parent_group_names} "
            f"must be subset of {mapping.parent_schema_key_class.__name__} fields "
            f"{parent_schema_field_names}"
        )


@pytest.mark.parametrize(
    "entry_type_short_name,fqn,expected",
    [
        (
            "bigquery-table",
            "bigquery:harshal-playground-306419.fivetran_smoke_test.destination_schema_metadata",
            {
                "project_id": "harshal-playground-306419",
                "dataset_id": "fivetran_smoke_test",
                "table_id": "destination_schema_metadata",
            },
        ),
        (
            "bigquery-dataset",
            "bigquery:harshal-playground-306419.big_tables",
            {
                "project_id": "harshal-playground-306419",
                "dataset_id": "big_tables",
            },
        ),
        (
            "cloud-spanner-table",
            "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal.ShoppingCarts",
            {
                "project_id": "harshal-playground-306419",
                "location": "us-west2",
                "instance_id": "sergio-test",
                "database_id": "cymbal",
                "table_id": "ShoppingCarts",
            },
        ),
        (
            "cloudsql-mysql-table",
            "cloudsql_mysql:harshal-playground-306419.us-west2.sergio-test.sergio-db.your_table",
            {
                "project_id": "harshal-playground-306419",
                "location": "us-west2",
                "instance_id": "sergio-test",
                "database_id": "sergio-db",
                "table_id": "your_table",
            },
        ),
        (
            "pubsub-topic",
            "pubsub:topic:acryl-staging.observe-staging-obs",
            {
                "project_id": "acryl-staging",
                "topic_id": "observe-staging-obs",
            },
        ),
    ],
)
def test_parse_fully_qualified_name_examples(
    entry_type_short_name: str, fqn: str, expected: dict[str, str]
) -> None:
    assert parse_fully_qualified_name(entry_type_short_name, fqn) == expected


def test_parse_fully_qualified_name_invalid() -> None:
    assert (
        parse_fully_qualified_name(
            "cloud-spanner-table", "spanner:missing.regional-us-west2.parts"
        )
        is None
    )


@pytest.mark.parametrize(
    "entry_type_short_name,parent_entry,expected",
    [
        (
            "bigquery-table",
            "projects/harshal-playground-306419/locations/us/entryGroups/@bigquery/entries/"
            "bigquery.googleapis.com/projects/harshal-playground-306419/datasets/fivetran_smoke_test",
            {
                "project_id": "harshal-playground-306419",
                "dataset_id": "fivetran_smoke_test",
            },
        ),
        (
            "cloud-spanner-table",
            "projects/harshal-playground-306419/locations/us-west2/entryGroups/@spanner/entries/"
            "spanner.googleapis.com/projects/harshal-playground-306419/instances/sergio-test/databases/cymbal",
            {
                "project_id": "harshal-playground-306419",
                "location": "us-west2",
                "instance_id": "sergio-test",
                "database_id": "cymbal",
            },
        ),
        (
            "cloudsql-mysql-table",
            "projects/harshal-playground-306419/locations/us-west2/entryGroups/@cloudsql/entries/"
            "cloudsql.googleapis.com/projects/harshal-playground-306419/locations/us-west2/instances/sergio-test/"
            "databases/sergio-db",
            {
                "project_id": "harshal-playground-306419",
                "location": "us-west2",
                "instance_id": "sergio-test",
                "database_id": "sergio-db",
            },
        ),
    ],
)
def test_parse_parent_entry_examples(
    entry_type_short_name: str, parent_entry: str, expected: dict[str, str]
) -> None:
    assert parse_parent_entry(entry_type_short_name, parent_entry) == expected


def test_build_schema_key_from_fqn_container_types() -> None:
    bq_key = build_schema_key_from_fqn(
        "bigquery-dataset",
        "bigquery:harshal-playground-306419.big_tables",
    )
    assert isinstance(bq_key, DataplexBigQueryDataset)
    assert bq_key.project_id == "harshal-playground-306419"
    assert bq_key.dataset_id == "big_tables"

    spanner_key = build_schema_key_from_fqn(
        "cloud-spanner-database",
        "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal",
    )
    assert isinstance(spanner_key, DataplexCloudSpannerDatabase)
    assert spanner_key.instance_id == "sergio-test"


def test_build_schema_key_from_fqn_dataset_types() -> None:
    mysql_key = build_schema_key_from_fqn(
        "cloudsql-mysql-table",
        "cloudsql_mysql:harshal-playground-306419.us-west2.sergio-test.sergio-db.your_table",
    )
    assert isinstance(mysql_key, DataplexCloudSqlMySqlDatabase)
    assert mysql_key.database_id == "sergio-db"

    pubsub_key = build_schema_key_from_fqn(
        "pubsub-topic",
        "pubsub:topic:acryl-staging.observe-staging-obs",
    )
    assert isinstance(pubsub_key, DataplexPubSubTopic)
    assert pubsub_key.topic_id == "observe-staging-obs"


def test_build_parent_schema_key() -> None:
    parent_key = build_parent_schema_key(
        "cloud-spanner-table",
        "projects/harshal-playground-306419/locations/us-west2/entryGroups/@spanner/entries/"
        "spanner.googleapis.com/projects/harshal-playground-306419/instances/sergio-test/databases/cymbal",
    )
    assert isinstance(parent_key, DataplexCloudSpannerDatabase)
    assert parent_key.database_id == "cymbal"


def test_build_dataset_urn_from_fqn() -> None:
    bq_urn = build_dataset_urn_from_fqn(
        "bigquery-table",
        "bigquery:harshal-playground-306419.fivetran_smoke_test.destination_schema_metadata",
        env="PROD",
    )
    assert bq_urn is not None
    assert "urn:li:dataPlatform:bigquery" in bq_urn
    assert (
        "harshal-playground-306419.fivetran_smoke_test.destination_schema_metadata"
        in bq_urn
    )

    pubsub_urn = build_dataset_urn_from_fqn(
        "pubsub-topic",
        "pubsub:topic:acryl-staging.observe-staging-obs",
        env="PROD",
    )
    assert pubsub_urn is not None
    assert "urn:li:dataPlatform:pubsub" in pubsub_urn
    assert "topic:acryl-staging.observe-staging-obs" in pubsub_urn


def test_build_container_urn_from_fqn() -> None:
    container_urn = build_container_urn_from_fqn(
        "cloudsql-mysql-instance",
        "cloudsql_mysql:harshal-playground-306419.us-west2.sergio-test",
    )
    assert container_urn is not None
    assert container_urn.startswith("urn:li:container:")

    assert (
        build_container_urn_from_fqn(
            "pubsub-topic", "pubsub:topic:acryl-staging.observe-staging-obs"
        )
        is None
    )


def test_build_parent_container_urn() -> None:
    parent_container_urn = build_parent_container_urn(
        "bigquery-table",
        "projects/harshal-playground-306419/locations/us/entryGroups/@bigquery/entries/"
        "bigquery.googleapis.com/projects/harshal-playground-306419/datasets/fivetran_smoke_test",
    )
    assert parent_container_urn is not None
    assert parent_container_urn.startswith("urn:li:container:")


def test_build_project_schema_key_from_fqn() -> None:
    project_key = build_project_schema_key_from_fqn(
        "cloud-spanner-table",
        "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal.ShoppingCarts",
    )
    assert project_key is not None
    assert isinstance(project_key, DataplexProjectId)
    assert project_key.project_id == "harshal-playground-306419"


def test_build_project_container_urn_from_fqn() -> None:
    project_urn = build_project_container_urn_from_fqn(
        "bigquery-table",
        "bigquery:harshal-playground-306419.fivetran_smoke_test.destination_schema_metadata",
    )
    assert project_urn is not None
    assert project_urn.startswith("urn:li:container:")
