"""
Tests for `DremioToDataHubSourceTypeMapping`.

These cover the table-driven mapping/category behavior that downstream
URN construction depends on. The intent is to lock in canonical Dremio
source-type strings (as documented in Dremio's Catalog API reference)
against their DataHub platform names — so adding a new entry can't
silently break categorization (database vs. file_object_storage).
"""

import pytest

from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)


@pytest.mark.parametrize(
    "dremio_type,expected_platform",
    [
        ("BIGQUERY", "bigquery"),
        # Covers Polaris OSS, Nessie+REST, AWS Glue Iceberg REST, S3 Tables,
        # Confluent Tableflow, Microsoft OneLake.
        ("RESTCATALOG", "iceberg"),
        ("SAPHANA", "hana"),
        ("SNOWFLAKEOPENCATALOG", "iceberg"),
        ("UNITY", "databricks"),
    ],
)
def test_new_source_types_map_to_expected_platforms(
    dremio_type: str, expected_platform: str
) -> None:
    assert (
        DremioToDataHubSourceTypeMapping.get_datahub_platform(dremio_type)
        == expected_platform
    )


@pytest.mark.parametrize(
    "dremio_type",
    ["BIGQUERY", "RESTCATALOG", "SAPHANA", "SNOWFLAKEOPENCATALOG", "UNITY"],
)
def test_new_source_types_categorized_as_database(dremio_type: str) -> None:
    # These all use dot-notation database/schema/table paths in Dremio,
    # not slash-notation file paths.
    assert DremioToDataHubSourceTypeMapping.get_category(dremio_type) == "database"


def test_case_insensitive_lookup() -> None:
    # Dremio returns SOURCE TYPES uppercase, but recipes / user input may
    # not. Already covered by the implementation but worth pinning.
    assert (
        DremioToDataHubSourceTypeMapping.get_datahub_platform("restcatalog")
        == "iceberg"
    )
    assert DremioToDataHubSourceTypeMapping.get_category("restcatalog") == "database"


def test_unknown_source_type_falls_back_to_lowercase_name() -> None:
    # Behavior contract: unknown types pass through as the lowercased
    # Dremio name with an unknown category — used by ARP / custom sources.
    assert (
        DremioToDataHubSourceTypeMapping.get_datahub_platform("FUTURE_SOURCE")
        == "future_source"
    )
    assert DremioToDataHubSourceTypeMapping.get_category("FUTURE_SOURCE") == "unknown"
