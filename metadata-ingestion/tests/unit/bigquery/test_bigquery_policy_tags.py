"""Unit tests for the optimized BigQuery policy tag extraction logic."""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import Row

from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigQuerySchemaApi,
    _parse_taxonomy_id,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def create_row(d: Dict[str, Any]) -> Row:
    values = []
    field_to_index = {}
    for i, (k, v) in enumerate(d.items()):
        field_to_index[k] = i
        values.append(v)
    return Row(tuple(values), field_to_index)


def _make_column_row(
    table_name: str = "my_table",
    column_name: str = "my_col",
    ordinal_position: int = 1,
    field_path: str = "my_col",
    is_nullable: str = "YES",
    data_type: str = "STRING",
    comment: Optional[str] = None,
    is_hidden: str = "NO",
    is_partitioning_column: str = "NO",
    clustering_ordinal_position: Optional[int] = None,
    policy_tags: Optional[List[str]] = None,
) -> Row:
    return create_row(
        {
            "table_catalog": "my_project",
            "table_schema": "my_dataset",
            "table_name": table_name,
            "column_name": column_name,
            "ordinal_position": ordinal_position,
            "field_path": field_path,
            "policy_tags": policy_tags,
            "is_nullable": is_nullable,
            "data_type": data_type,
            "comment": comment,
            "is_hidden": is_hidden,
            "is_partitioning_column": is_partitioning_column,
            "clustering_ordinal_position": clustering_ordinal_position,
        }
    )


def _make_policy_tag(name: str, display_name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, display_name=display_name)


def _make_schema_api(
    datacatalog_client: Optional[MagicMock] = None,
) -> BigQuerySchemaApi:
    return BigQuerySchemaApi(
        report=BigQueryV2Report().schema_api_perf,
        client=MagicMock(),
        projects_client=MagicMock(),
        datacatalog_client=datacatalog_client,
    )


# ---------------------------------------------------------------------------
# _parse_taxonomy_id
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        (
            "projects/my-project/locations/us/taxonomies/12345/policyTags/67890",
            "projects/my-project/locations/us/taxonomies/12345",
        ),
        (
            "projects/proj/locations/europe-west1/taxonomies/abc/policyTags/xyz",
            "projects/proj/locations/europe-west1/taxonomies/abc",
        ),
        ("not-a-resource-name", None),
        ("", None),
        ("projects/p/locations/l/taxonomies/t", None),  # missing /policyTags/ segment
    ],
)
def test_parse_taxonomy_id(resource_name: str, expected: Optional[str]) -> None:
    assert _parse_taxonomy_id(resource_name) == expected


# ---------------------------------------------------------------------------
# build_policy_tag_display_name_mapping
# ---------------------------------------------------------------------------


def test_build_mapping_empty_input() -> None:
    api = _make_schema_api(datacatalog_client=MagicMock())
    result = api.build_policy_tag_display_name_mapping(set(), BigQueryV2Report())
    assert result == {}


def test_build_mapping_resolves_display_names() -> None:
    dc_client = MagicMock()
    tag_pii = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag1", "PII - Email"
    )
    tag_ssn = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag2", "PII - SSN"
    )
    dc_client.list_policy_tags.return_value = [tag_pii, tag_ssn]

    api = _make_schema_api(datacatalog_client=dc_client)
    resource_names = {tag_pii.name, tag_ssn.name}

    result = api.build_policy_tag_display_name_mapping(
        resource_names, BigQueryV2Report()
    )

    assert result == {tag_pii.name: "PII - Email", tag_ssn.name: "PII - SSN"}
    dc_client.list_policy_tags.assert_called_once_with(
        parent="projects/p/locations/us/taxonomies/t1"
    )


def test_build_mapping_deduplicates_taxonomies() -> None:
    """Multiple tags from the same taxonomy trigger only one API call."""
    dc_client = MagicMock()
    tags = [
        _make_policy_tag(
            f"projects/p/locations/us/taxonomies/t1/policyTags/tag{i}", f"Tag {i}"
        )
        for i in range(5)
    ]
    dc_client.list_policy_tags.return_value = tags

    api = _make_schema_api(datacatalog_client=dc_client)
    resource_names = {t.name for t in tags}

    result = api.build_policy_tag_display_name_mapping(
        resource_names, BigQueryV2Report()
    )

    assert len(result) == 5
    assert dc_client.list_policy_tags.call_count == 1


def test_build_mapping_multiple_taxonomies() -> None:
    """Tags from different taxonomies trigger one API call per taxonomy."""
    dc_client = MagicMock()
    tag_t1 = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag1", "T1 Tag"
    )
    tag_t2 = _make_policy_tag(
        "projects/p/locations/eu/taxonomies/t2/policyTags/tag1", "T2 Tag"
    )

    def _list_tags(parent: str) -> List[SimpleNamespace]:
        if "t1" in parent:
            return [tag_t1]
        return [tag_t2]

    dc_client.list_policy_tags.side_effect = _list_tags

    api = _make_schema_api(datacatalog_client=dc_client)
    result = api.build_policy_tag_display_name_mapping(
        {tag_t1.name, tag_t2.name}, BigQueryV2Report()
    )

    assert result[tag_t1.name] == "T1 Tag"
    assert result[tag_t2.name] == "T2 Tag"
    assert dc_client.list_policy_tags.call_count == 2


def test_build_mapping_caches_across_calls() -> None:
    """Second call for the same taxonomy does not trigger an additional API call."""
    dc_client = MagicMock()
    tag = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag1", "Cached Tag"
    )
    dc_client.list_policy_tags.return_value = [tag]

    api = _make_schema_api(datacatalog_client=dc_client)

    result1 = api.build_policy_tag_display_name_mapping({tag.name}, BigQueryV2Report())
    result2 = api.build_policy_tag_display_name_mapping({tag.name}, BigQueryV2Report())

    assert result1 == result2 == {tag.name: "Cached Tag"}
    assert dc_client.list_policy_tags.call_count == 1  # cached after first call


def test_build_mapping_malformed_resource_name_skipped() -> None:
    dc_client = MagicMock()
    dc_client.list_policy_tags.return_value = []

    api = _make_schema_api(datacatalog_client=dc_client)
    malformed = "not/a/valid/resource/name"
    report = BigQueryV2Report()

    result = api.build_policy_tag_display_name_mapping({malformed}, report)

    assert result == {}
    dc_client.list_policy_tags.assert_not_called()
    assert len(report.warnings) > 0


def test_build_mapping_api_failure_uses_resource_name() -> None:
    """When list_policy_tags fails, resource names are stored as-is (graceful degradation)."""
    dc_client = MagicMock()
    dc_client.list_policy_tags.side_effect = Exception("403 Permission denied")

    api = _make_schema_api(datacatalog_client=dc_client)
    resource_name = "projects/p/locations/us/taxonomies/t1/policyTags/tag1"
    report = BigQueryV2Report()

    result = api.build_policy_tag_display_name_mapping({resource_name}, report)

    assert result == {resource_name: resource_name}
    assert len(report.warnings) > 0


def test_build_mapping_deleted_tag_falls_back_to_resource_name() -> None:
    """A tag not returned by list_policy_tags (e.g. deleted) falls back to resource name."""
    dc_client = MagicMock()
    existing_tag = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/existing", "Existing Tag"
    )
    dc_client.list_policy_tags.return_value = [existing_tag]

    api = _make_schema_api(datacatalog_client=dc_client)
    deleted_name = "projects/p/locations/us/taxonomies/t1/policyTags/deleted"

    result = api.build_policy_tag_display_name_mapping(
        {existing_tag.name, deleted_name}, BigQueryV2Report()
    )

    assert result[existing_tag.name] == "Existing Tag"
    assert result[deleted_name] == deleted_name  # fallback


# ---------------------------------------------------------------------------
# get_columns_for_dataset with policy tags
# ---------------------------------------------------------------------------


@patch.object(BigQuerySchemaApi, "get_query_result")
def test_get_columns_for_dataset_with_policy_tags(query_mock: MagicMock) -> None:
    """Policy tags are resolved from INFORMATION_SCHEMA data via batch API calls."""
    tag_name = "projects/p/locations/us/taxonomies/t1/policyTags/tag1"
    policy_tag = _make_policy_tag(tag_name, "PII - Email")

    dc_client = MagicMock()
    dc_client.list_policy_tags.return_value = [policy_tag]

    row = _make_column_row(
        table_name="tbl",
        column_name="email",
        policy_tags=[tag_name],
    )
    query_mock.return_value = [row]

    api = _make_schema_api(datacatalog_client=dc_client)
    result = api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds",
        column_limit=1000,
        report=BigQueryV2Report(),
        extract_policy_tags_from_catalog=True,
    )

    assert result is not None
    cols = result["tbl"]
    assert len(cols) == 1
    assert cols[0].policy_tags == ["PII - Email"]
    dc_client.list_policy_tags.assert_called_once_with(
        parent="projects/p/locations/us/taxonomies/t1"
    )


@patch.object(BigQuerySchemaApi, "get_query_result")
def test_get_columns_for_dataset_no_policy_tags_flag(query_mock: MagicMock) -> None:
    """When extract_policy_tags_from_catalog is False, no Data Catalog calls are made."""
    tag_name = "projects/p/locations/us/taxonomies/t1/policyTags/tag1"
    dc_client = MagicMock()

    row = _make_column_row(
        table_name="tbl",
        column_name="email",
        policy_tags=[tag_name],
    )
    query_mock.return_value = [row]

    api = _make_schema_api(datacatalog_client=dc_client)
    result = api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds",
        column_limit=1000,
        report=BigQueryV2Report(),
        extract_policy_tags_from_catalog=False,
    )

    assert result is not None
    assert result["tbl"][0].policy_tags == []
    dc_client.list_policy_tags.assert_not_called()


@patch.object(BigQuerySchemaApi, "get_query_result")
def test_get_columns_for_dataset_empty_policy_tags(query_mock: MagicMock) -> None:
    """Columns without policy tags produce an empty list, not None."""
    dc_client = MagicMock()
    dc_client.list_policy_tags.return_value = []

    row = _make_column_row(table_name="tbl", column_name="id", policy_tags=None)
    query_mock.return_value = [row]

    api = _make_schema_api(datacatalog_client=dc_client)
    result = api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds",
        column_limit=1000,
        report=BigQueryV2Report(),
        extract_policy_tags_from_catalog=True,
    )

    assert result is not None
    assert result["tbl"][0].policy_tags == []
    dc_client.list_policy_tags.assert_not_called()


@patch.object(BigQuerySchemaApi, "get_query_result")
def test_get_columns_for_dataset_deduplicates_taxonomy_calls(
    query_mock: MagicMock,
) -> None:
    """Multiple columns in the same dataset sharing a taxonomy trigger only one API call."""
    dc_client = MagicMock()
    tag1 = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag1", "Tag One"
    )
    tag2 = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag2", "Tag Two"
    )
    dc_client.list_policy_tags.return_value = [tag1, tag2]

    rows = [
        _make_column_row("tbl", "col1", policy_tags=[tag1.name]),
        _make_column_row("tbl", "col2", ordinal_position=2, policy_tags=[tag2.name]),
    ]
    query_mock.return_value = rows

    api = _make_schema_api(datacatalog_client=dc_client)
    result = api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds",
        column_limit=1000,
        report=BigQueryV2Report(),
        extract_policy_tags_from_catalog=True,
    )

    assert result is not None
    assert result["tbl"][0].policy_tags == ["Tag One"]
    assert result["tbl"][1].policy_tags == ["Tag Two"]
    assert dc_client.list_policy_tags.call_count == 1


@patch.object(BigQuerySchemaApi, "get_query_result")
def test_get_columns_for_dataset_cache_reused_across_datasets(
    query_mock: MagicMock,
) -> None:
    """The taxonomy cache persists across multiple get_columns_for_dataset calls."""
    dc_client = MagicMock()
    tag = _make_policy_tag(
        "projects/p/locations/us/taxonomies/t1/policyTags/tag1", "Sensitive"
    )
    dc_client.list_policy_tags.return_value = [tag]

    row = _make_column_row("tbl", "col", policy_tags=[tag.name])
    query_mock.return_value = [row]

    api = _make_schema_api(datacatalog_client=dc_client)
    report = BigQueryV2Report()

    # Call for dataset1
    api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds1",
        column_limit=1000,
        report=report,
        extract_policy_tags_from_catalog=True,
    )
    # Call for dataset2 — same taxonomy, should reuse cache
    query_mock.return_value = [row]
    api.get_columns_for_dataset(
        project_id="p",
        dataset_name="ds2",
        column_limit=1000,
        report=report,
        extract_policy_tags_from_catalog=True,
    )

    # Only one list_policy_tags call despite two datasets
    assert dc_client.list_policy_tags.call_count == 1
