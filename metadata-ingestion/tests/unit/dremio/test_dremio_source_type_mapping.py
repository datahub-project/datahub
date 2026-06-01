"""Pin canonical Dremio source-type strings against DataHub platform names
and categories that URN construction depends on."""

import pytest

from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)


@pytest.mark.parametrize(
    "dremio_type,expected_platform",
    [
        ("BIGQUERY", "bigquery"),
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
    # Dot-notation path sources, not slash-notation file paths.
    assert DremioToDataHubSourceTypeMapping.get_category(dremio_type) == "database"


def test_case_insensitive_lookup() -> None:
    assert (
        DremioToDataHubSourceTypeMapping.get_datahub_platform("restcatalog")
        == "iceberg"
    )
    assert DremioToDataHubSourceTypeMapping.get_category("restcatalog") == "database"


def test_unknown_source_type_falls_back_to_lowercase_name() -> None:
    # ARP / custom sources rely on this lowercase+unknown fallback.
    assert (
        DremioToDataHubSourceTypeMapping.get_datahub_platform("FUTURE_SOURCE")
        == "future_source"
    )
    assert DremioToDataHubSourceTypeMapping.get_category("FUTURE_SOURCE") == "unknown"


def test_azure_storage_resolves_to_database_despite_dual_membership() -> None:
    # AZURE_STORAGE is in both sets; "database" wins via get_category check order.
    assert DremioToDataHubSourceTypeMapping.get_category("AZURE_STORAGE") == "database"


@pytest.fixture
def restore_mapping_state():
    # add_mapping mutates class state — snapshot/restore to isolate tests.
    snapshot_map = dict(DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING)
    snapshot_db = set(DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES)
    snapshot_fs = set(DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES)
    yield
    DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING.clear()
    DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING.update(snapshot_map)
    DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES.clear()
    DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES.update(snapshot_db)
    DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES.clear()
    DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES.update(snapshot_fs)


class TestAddMapping:
    def test_database_category_registers_for_dot_dispatch(self, restore_mapping_state):
        DremioToDataHubSourceTypeMapping.add_mapping("MYDB", "mydb", "database")
        assert DremioToDataHubSourceTypeMapping.get_datahub_platform("MYDB") == "mydb"
        assert DremioToDataHubSourceTypeMapping.get_category("MYDB") == "database"

    def test_file_object_storage_category_registers_for_slash_dispatch(
        self, restore_mapping_state
    ):
        DremioToDataHubSourceTypeMapping.add_mapping(
            "MYSTORE", "mystore", "file_object_storage"
        )
        assert (
            DremioToDataHubSourceTypeMapping.get_category("MYSTORE")
            == "file_object_storage"
        )

    def test_none_category_leaves_type_uncategorized(self, restore_mapping_state):
        # category=None: platform resolves, get_category returns "unknown".
        DremioToDataHubSourceTypeMapping.add_mapping("MYARP", "myarp")
        assert DremioToDataHubSourceTypeMapping.get_datahub_platform("MYARP") == "myarp"
        assert DremioToDataHubSourceTypeMapping.get_category("MYARP") == "unknown"
