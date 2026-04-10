"""Unit tests for Dataplex identity and entry-type mapping utilities."""

from typing import Any

import pytest

from datahub.ingestion.source.dataplex.dataplex_ids import (
    BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
    BIGQUERY_TABLE_FQN_REGEX,
    DATAPLEX_ENTRY_TYPE_MAPPINGS,
    PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM,
    DataplexBigQueryDataset,
    DataplexBigtableInstance,
    DataplexCloudSpannerDatabase,
    DataplexCloudSpannerInstance,
    DataplexCloudSqlMySqlInstance,
    DataplexEntryTypeMapping,
    DataplexProjectId,
    _extract_dataset_name_from_fqn,
    build_container_key_from_fqn,
    build_container_urn_from_fqn,
    build_dataset_urn_from_fqn,
    build_dataset_urn_from_fqn_only,
    build_parent_container_key,
    build_parent_container_urn,
    build_project_container_urn_from_fqn,
    build_project_schema_key_from_fqn,
    extract_datahub_dataset_name_from_fqn,
    extract_entry_type_short_name,
    is_supported_lineage_entry_type,
    parse_fully_qualified_name,
    parse_parent_entry,
)


def test_schema_key_parent_chain_for_project_has_no_duplicate_step() -> None:
    database_key = build_container_key_from_fqn(
        "cloudsql-mysql-database",
        "cloudsql_mysql:harshal-playground-306419.us-west2.sergio-test.sergio-db",
    )
    assert database_key is not None
    instance_key = database_key.parent_key()
    assert instance_key is not None
    project_key = instance_key.parent_key()
    assert project_key is not None
    # Project key should terminate immediately after one parent_key() call.
    assert project_key.parent_key() is None


@pytest.mark.parametrize(
    "entry_type,expected_short_name",
    [
        (
            "projects/655216118709/locations/global/entryTypes/bigquery-table",
            "bigquery-table",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/bigquery-view",
            "bigquery-view",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/cloud-spanner-instance",
            "cloud-spanner-instance",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/cloud-bigtable-instance",
            "cloud-bigtable-instance",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/cloud-bigtable-table",
            "cloud-bigtable-table",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/pubsub-topic",
            "pubsub-topic",
        ),
        (
            "projects/655216118709/locations/global/entryTypes/vertexai-dataset",
            "vertexai-dataset",
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
        "bigquery-view",
        "cloudsql-mysql-instance",
        "cloudsql-mysql-database",
        "cloudsql-mysql-table",
        "cloud-spanner-instance",
        "cloud-spanner-database",
        "cloud-spanner-table",
        "cloud-bigtable-instance",
        "cloud-bigtable-table",
        "pubsub-topic",
        "vertexai-dataset",
    }


@pytest.mark.parametrize(
    "mapping_kwargs,expected_error",
    [
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Dataset",
                "datahub_subtype": "table",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
                "container_key_class": None,
                "parent_container_key_class": DataplexBigQueryDataset,
                "datahub_dataset_name_format": None,
            },
            "Dataset mappings must define datahub_dataset_name_format",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Container",
                "datahub_subtype": "dataset",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": None,
                "container_key_class": DataplexBigQueryDataset,
                "parent_container_key_class": None,
                "datahub_dataset_name_format": "{project_id}.{dataset_id}",
            },
            "Container mappings must not define datahub_dataset_name_format",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Dataset",
                "datahub_subtype": "table",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
                "container_key_class": None,
                "parent_container_key_class": None,
                "datahub_dataset_name_format": "{project_id}.{dataset_id}.{table_id}",
            },
            "Mappings with parent_entry_regex must define parent_container_key_class",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Dataset",
                "datahub_subtype": "table",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": BIGQUERY_DATASET_PARENT_ENTRY_REGEX,
                "container_key_class": None,
                "parent_container_key_class": DataplexCloudSqlMySqlInstance,
                "datahub_dataset_name_format": "{project_id}.{dataset_id}.{table_id}",
            },
            "parent_entry_regex groups",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Container",
                "datahub_subtype": "dataset",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": None,
                "container_key_class": None,
                "parent_container_key_class": None,
                "datahub_dataset_name_format": None,
            },
            "Container mappings must define container_key_class",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Dataset",
                "datahub_subtype": "table",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": None,
                "container_key_class": DataplexBigQueryDataset,
                "parent_container_key_class": None,
                "datahub_dataset_name_format": "{project_id}.{dataset_id}.{table_id}",
            },
            "Dataset mappings must not define container_key_class",
        ),
        (
            {
                "datahub_platform": "bigquery",
                "datahub_entity_type": "Container",
                "datahub_subtype": "dataset",
                "fqn_regex": BIGQUERY_TABLE_FQN_REGEX,
                "parent_entry_regex": None,
                "container_key_class": DataplexBigQueryDataset,
                "parent_container_key_class": None,
                "datahub_dataset_name_format": None,
            },
            "fqn_regex groups",
        ),
    ],
)
def test_dataplex_entry_type_mapping_validation_errors(
    mapping_kwargs: dict[str, Any], expected_error: str
) -> None:
    with pytest.raises(ValueError, match=expected_error):
        # Intentionally bypass strict constructor typing to validate runtime guards.
        DataplexEntryTypeMapping(**mapping_kwargs)


@pytest.mark.parametrize(
    "entry_type_short_name", list(DATAPLEX_ENTRY_TYPE_MAPPINGS.keys())
)
def test_dataplex_entry_type_mapping_instances_are_valid(
    entry_type_short_name: str,
) -> None:
    mapping = DATAPLEX_ENTRY_TYPE_MAPPINGS[entry_type_short_name]
    assert mapping.datahub_entity_type in {"Dataset", "Container"}
    if mapping.datahub_entity_type == "Dataset":
        assert mapping.datahub_dataset_name_format is not None
    else:
        assert mapping.datahub_dataset_name_format is None


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
            "bigquery-view",
            "bigquery:harshal-playground-306419.fivetran_smoke_test.destination_schema_view",
            {
                "project_id": "harshal-playground-306419",
                "dataset_id": "fivetran_smoke_test",
                "table_id": "destination_schema_view",
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
            "cloud-bigtable-instance",
            "bigtable:trustedplatform-pl-production.feature-store",
            {
                "project_id": "trustedplatform-pl-production",
                "instance_id": "feature-store",
            },
        ),
        (
            "cloud-bigtable-table",
            "bigtable:trustedplatform-pl-production.feature-store.counts",
            {
                "project_id": "trustedplatform-pl-production",
                "instance_id": "feature-store",
                "table_id": "counts",
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
        (
            "vertexai-dataset",
            "vertex_ai:dataset:harshal-playground-306419.us-west2.5135361416504541184",
            {
                "project_id": "harshal-playground-306419",
                "location": "us-west2",
                "dataset_id": "5135361416504541184",
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
            "bigquery-view",
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
        (
            "cloud-bigtable-table",
            "projects/trustedplatform-pl-production/locations/global/entryGroups/@bigtable/entries/"
            "bigtable.googleapis.com/projects/trustedplatform-pl-production/instances/feature-store",
            {
                "project_id": "trustedplatform-pl-production",
                "instance_id": "feature-store",
            },
        ),
    ],
)
def test_parse_parent_entry_examples(
    entry_type_short_name: str, parent_entry: str, expected: dict[str, str]
) -> None:
    assert parse_parent_entry(entry_type_short_name, parent_entry) == expected


def test_build_container_key_from_fqn_container_types() -> None:
    bq_key = build_container_key_from_fqn(
        "bigquery-dataset",
        "bigquery:harshal-playground-306419.big_tables",
    )
    assert isinstance(bq_key, DataplexBigQueryDataset)
    assert bq_key.project_id == "harshal-playground-306419"
    assert bq_key.dataset_id == "big_tables"

    spanner_key = build_container_key_from_fqn(
        "cloud-spanner-database",
        "spanner:harshal-playground-306419.regional-us-west2.sergio-test.cymbal",
    )
    assert isinstance(spanner_key, DataplexCloudSpannerDatabase)
    assert spanner_key.instance_id == "sergio-test"

    bigtable_key = build_container_key_from_fqn(
        "cloud-bigtable-instance",
        "bigtable:trustedplatform-pl-production.feature-store",
    )
    assert isinstance(bigtable_key, DataplexBigtableInstance)
    assert bigtable_key.instance_id == "feature-store"


def test_build_container_key_from_fqn_dataset_types_returns_none() -> None:
    mysql_table_key = build_container_key_from_fqn(
        "cloudsql-mysql-table",
        "cloudsql_mysql:harshal-playground-306419.us-west2.sergio-test.sergio-db.your_table",
    )
    assert mysql_table_key is None

    pubsub_key = build_container_key_from_fqn(
        "pubsub-topic",
        "pubsub:topic:acryl-staging.observe-staging-obs",
    )
    assert pubsub_key is None


def test_build_parent_container_key() -> None:
    parent_key = build_parent_container_key(
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
    assert "acryl-staging.observe-staging-obs" in pubsub_urn
    assert "topic:acryl-staging.observe-staging-obs" not in pubsub_urn

    vertexai_urn = build_dataset_urn_from_fqn(
        "vertexai-dataset",
        "vertex_ai:dataset:harshal-playground-306419.us-west2.5135361416504541184",
        env="PROD",
    )
    assert vertexai_urn is not None
    assert "urn:li:dataPlatform:vertexai" in vertexai_urn
    assert "harshal-playground-306419.us-west2.5135361416504541184" in vertexai_urn
    assert "dataset:harshal-playground-306419.us-west2.5135361416504541184" not in (
        vertexai_urn
    )


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


def test_identity_helpers_return_none_for_unsupported_or_invalid_inputs() -> None:
    assert parse_fully_qualified_name("unsupported-type", "bigquery:p.ds.t") is None
    assert parse_parent_entry("unsupported-type", "projects/p/...") is None
    assert build_container_key_from_fqn("unsupported-type", "bigquery:p.ds") is None
    assert build_parent_container_key("unsupported-type", "projects/p/...") is None
    assert (
        build_parent_container_key(
            "bigquery-table", "projects/p/locations/us/entryGroups/g/entries/invalid"
        )
        is None
    )

    assert (
        build_dataset_urn_from_fqn("unsupported-type", "bigquery:p.ds.t", "PROD")
        is None
    )
    assert build_dataset_urn_from_fqn("bigquery-table", "bigquery", "PROD") is None
    assert build_container_urn_from_fqn("bigquery-table", "bigquery:p.ds.t") is None
    assert (
        build_container_urn_from_fqn("cloudsql-mysql-instance", "cloudsql_mysql")
        is None
    )
    assert (
        build_project_schema_key_from_fqn("unsupported-type", "bigquery:p.ds") is None
    )
    assert (
        build_project_container_urn_from_fqn("unsupported-type", "bigquery:p.ds")
        is None
    )
    assert build_parent_container_urn("bigquery-dataset", "projects/p/...") is None


def test_build_parent_container_key_for_dataset_entry_types() -> None:
    bq_parent = build_parent_container_key(
        "bigquery-table",
        "projects/harshal-playground-306419/locations/us/entryGroups/@bigquery/entries/"
        "bigquery.googleapis.com/projects/harshal-playground-306419/datasets/fivetran_smoke_test",
    )
    assert isinstance(bq_parent, DataplexBigQueryDataset)

    cloudsql_parent = build_parent_container_key(
        "cloudsql-mysql-table",
        "projects/harshal-playground-306419/locations/us-west2/entryGroups/@cloudsql/entries/"
        "cloudsql.googleapis.com/projects/harshal-playground-306419/locations/us-west2/instances/sergio-test/"
        "databases/sergio-db",
    )
    assert isinstance(cloudsql_parent, DataplexCloudSqlMySqlInstance)

    spanner_parent = build_parent_container_key(
        "cloud-spanner-database",
        "projects/harshal-playground-306419/locations/us-west2/entryGroups/@spanner/entries/"
        "spanner.googleapis.com/projects/harshal-playground-306419/instances/sergio-test",
    )
    assert isinstance(spanner_parent, DataplexCloudSpannerInstance)

    bigtable_parent = build_parent_container_key(
        "cloud-bigtable-table",
        "projects/trustedplatform-pl-production/locations/global/entryGroups/@bigtable/entries/"
        "bigtable.googleapis.com/projects/trustedplatform-pl-production/instances/feature-store",
    )
    assert isinstance(bigtable_parent, DataplexBigtableInstance)


def test_is_supported_lineage_entry_type_helper() -> None:
    assert is_supported_lineage_entry_type("bigquery-table")
    assert is_supported_lineage_entry_type("bigquery-view")
    assert is_supported_lineage_entry_type("cloudsql-mysql-table")
    assert is_supported_lineage_entry_type("cloud-spanner-table")
    assert is_supported_lineage_entry_type("cloud-bigtable-table")
    assert is_supported_lineage_entry_type("pubsub-topic")
    assert is_supported_lineage_entry_type("vertexai-dataset")
    assert not is_supported_lineage_entry_type("bigquery-dataset")
    assert not is_supported_lineage_entry_type("cloud-bigtable-instance")
    assert not is_supported_lineage_entry_type("cloudsql-mysql-database")
    assert not is_supported_lineage_entry_type("unknown-entry-type")


@pytest.mark.parametrize(
    "fully_qualified_name,expected_dataset_urn",
    [
        (
            "bigquery:test-project.analytics.customers",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.analytics.customers,PROD)",
        ),
        (
            "pubsub:topic:acryl-staging.observe-staging-obs",
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,acryl-staging.observe-staging-obs,PROD)",
        ),
        (
            "bigtable:trustedplatform-pl-production.feature-store.counts",
            "urn:li:dataset:(urn:li:dataPlatform:bigtable,trustedplatform-pl-production.feature-store.counts,PROD)",
        ),
        (
            "vertex_ai:dataset:harshal-playground-306419.us-west2.5135361416504541184",
            "urn:li:dataset:(urn:li:dataPlatform:vertexai,harshal-playground-306419.us-west2.5135361416504541184,PROD)",
        ),
        ("unknown:project.dataset.table", None),
        ("invalid", None),
    ],
)
def test_build_dataset_urn_from_fqn_only_cross_platform(
    fully_qualified_name: str, expected_dataset_urn: str | None
) -> None:
    assert (
        build_dataset_urn_from_fqn_only(fully_qualified_name, env="PROD")
        == expected_dataset_urn
    )


def test_extract_datahub_dataset_name_from_fqn_uses_mapping_format() -> None:
    dataset_name = extract_datahub_dataset_name_from_fqn(
        "vertexai-dataset",
        "vertex_ai:dataset:harshal-playground-306419.us-west2.5135361416504541184",
    )
    assert dataset_name == "harshal-playground-306419.us-west2.5135361416504541184"


def test_extract_dataset_name_from_fqn_returns_none_for_container_mapping() -> None:
    assert (
        _extract_dataset_name_from_fqn(
            "bigquery-dataset", "bigquery:test-project.analytics"
        )
        is None
    )


def test_extract_datahub_dataset_name_from_fqn_handles_format_key_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Dataset",
        datahub_subtype="table",
        fqn_regex=BIGQUERY_TABLE_FQN_REGEX,
        parent_entry_regex=None,
        container_key_class=None,
        parent_container_key_class=None,
        datahub_dataset_name_format="{missing_field}",
    )
    monkeypatch.setitem(DATAPLEX_ENTRY_TYPE_MAPPINGS, "test-key-error", mapping)

    assert (
        extract_datahub_dataset_name_from_fqn(
            "test-key-error",
            "bigquery:test-project.analytics.customers",
        )
        is None
    )


def test_extract_datahub_dataset_name_from_fqn_handles_missing_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = DataplexEntryTypeMapping(
        datahub_platform="bigquery",
        datahub_entity_type="Dataset",
        datahub_subtype="table",
        fqn_regex=BIGQUERY_TABLE_FQN_REGEX,
        parent_entry_regex=None,
        container_key_class=None,
        parent_container_key_class=None,
        datahub_dataset_name_format="{project_id}.{dataset_id}.{table_id}",
    )
    object.__setattr__(mapping, "datahub_dataset_name_format", None)
    monkeypatch.setitem(DATAPLEX_ENTRY_TYPE_MAPPINGS, "test-missing-format", mapping)

    assert (
        extract_datahub_dataset_name_from_fqn(
            "test-missing-format",
            "bigquery:test-project.analytics.customers",
        )
        is None
    )


def test_build_project_schema_key_from_fqn_returns_none_when_project_class_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delitem(PROJECT_SCHEMA_KEY_CLASS_BY_PLATFORM, "bigquery")

    assert (
        build_project_schema_key_from_fqn(
            "bigquery-table",
            "bigquery:test-project.analytics.customers",
        )
        is None
    )
