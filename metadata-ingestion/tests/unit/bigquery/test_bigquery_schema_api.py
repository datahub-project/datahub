"""BigQuerySchemaApi: translating BigQuery client / INFORMATION_SCHEMA responses into
the connector's dataclasses, and how per-row failures are reported."""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from google.cloud.bigquery.table import Row

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQuerySchemaApiPerfReport,
    BigQueryV2Report,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryDataset,
    BigQuerySchemaApi,
    PartitionInfo,
    query_project_list,
    query_project_list_from_labels,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryFilter

PROJECT = "test-project"
DATASET = "test_dataset"
NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def make_row(**fields: Any) -> Row:
    """A google.cloud.bigquery Row, which is what INFORMATION_SCHEMA queries yield:
    columns are reachable as attributes and via .get()."""
    return Row(tuple(fields.values()), {name: i for i, name in enumerate(fields)})


def make_api() -> BigQuerySchemaApi:
    return BigQuerySchemaApi(
        BigQuerySchemaApiPerfReport(),
        client=MagicMock(),
        projects_client=MagicMock(),
    )


def bq_client(api: BigQuerySchemaApi) -> MagicMock:
    assert isinstance(api.bq_client, MagicMock)
    return api.bq_client


def projects_client(api: BigQuerySchemaApi) -> MagicMock:
    assert isinstance(api.projects_client, MagicMock)
    return api.projects_client


def table_row(
    name: str, table_type: str = "BASE TABLE", ddl: Optional[str] = None
) -> Row:
    return make_row(
        table_name=name,
        created=NOW,
        table_type=table_type,
        comment=None,
        ddl=ddl,
    )


def view_row(name: str) -> Row:
    return make_row(
        table_name=name,
        created=NOW,
        comment=None,
        view_definition="SELECT 1",
        table_type="VIEW",
        labels=None,
    )


def snapshot_row(name: str) -> Row:
    return make_row(
        table_name=name,
        created=NOW,
        comment=None,
        ddl="CREATE SNAPSHOT",
        snapshot_time=NOW,
        base_table_catalog=PROJECT,
        base_table_schema=DATASET,
        base_table_name="base",
    )


def constraint_row(
    table: str, ctype: str, column: str, referenced: Optional[Dict[str, str]] = None
) -> Row:
    ref = referenced or {}
    return make_row(
        table_name=table,
        constraint_name=f"{ctype}_{column}",
        table_catalog=PROJECT,
        table_schema=DATASET,
        constraint_type=ctype,
        column_name=column,
        referenced_catalog=ref.get("catalog"),
        referenced_schema=ref.get("schema"),
        referenced_table=ref.get("table"),
        referenced_column=ref.get("column"),
    )


class TestProjects:
    def test_projects_with_labels_are_filtered_by_project_id_pattern(self) -> None:
        api = make_api()
        projects_client(api).search_projects.return_value = [
            SimpleNamespace(project_id="keep-me", display_name="Keep"),
            SimpleNamespace(project_id="drop-me", display_name="Drop"),
        ]
        report = BigQueryV2Report()
        config = BigQueryV2Config.model_validate(
            {
                "project_labels": ["team:data"],
                "project_id_pattern": {"deny": ["drop.*"]},
            }
        )

        projects = list(
            query_project_list_from_labels(api, report, BigQueryFilter(config, report))
        )

        assert [p.id for p in projects] == ["keep-me"]
        assert (
            "labels.team:data"
            in projects_client(api).search_projects.call_args.kwargs["query"]
        )

    def test_label_search_failure_is_reported_as_no_projects(self) -> None:
        api = make_api()
        projects_client(api).search_projects.side_effect = RuntimeError("forbidden")
        report = BigQueryV2Report()
        config = BigQueryV2Config.model_validate({"project_labels": ["team:data"]})

        projects = list(
            query_project_list_from_labels(api, report, BigQueryFilter(config, report))
        )

        assert projects == []
        assert len(report.failures) == 1

    def test_project_listing_exception_is_reported_as_no_projects(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        config = BigQueryV2Config.model_validate({})
        with patch.object(api, "get_projects", side_effect=RuntimeError("boom")):
            projects = list(
                query_project_list(api, report, BigQueryFilter(config, report))
            )

        assert projects == []
        assert len(report.failures) == 1


class TestDatasets:
    def test_filtered_out_datasets_skip_metadata_enrichment(self) -> None:
        api = make_api()
        bq_client(api).list_datasets.return_value = [
            SimpleNamespace(
                dataset_id="keep",
                labels={},
                _properties={"location": "US", "type": "DEFAULT"},
            ),
            SimpleNamespace(dataset_id="skip", labels={}, _properties={}),
        ]
        with patch.object(
            api, "_fetch_dataset_metadata_batch", return_value={}
        ) as fetch:
            datasets = api.get_datasets_for_project_id(
                PROJECT, dataset_filter=lambda name: name == "keep"
            )

        assert [d.name for d in datasets] == ["keep"]
        assert datasets[0].location == "US"
        # Missing from SCHEMATA: no enrichment, but not an error either.
        assert datasets[0].comment is None
        fetch.assert_called_once_with(PROJECT, "US", {"keep"})

    def test_everything_filtered_out_means_no_metadata_query(self) -> None:
        api = make_api()
        bq_client(api).list_datasets.return_value = [
            SimpleNamespace(dataset_id="skip", labels={}, _properties={})
        ]
        with patch.object(api, "_fetch_dataset_metadata_batch") as fetch:
            datasets = api.get_datasets_for_project_id(
                PROJECT, dataset_filter=lambda name: False
            )

        assert datasets == []
        fetch.assert_not_called()
        assert api.report.num_datasets_missing_type == 0

    @pytest.mark.parametrize(
        "location,expected",
        [
            ("US", True),
            ("aws-us-east-1", False),
            ("azure-eastus2", False),
            (None, True),
        ],
    )
    def test_biglake_datasets_do_not_support_constraints_or_partitions(
        self, location: Optional[str], expected: bool
    ) -> None:
        dataset = BigqueryDataset(name=DATASET, location=location)
        assert dataset.supports_table_constraints() is expected
        assert dataset.supports_table_partitions() is expected


class TestTables:
    def test_broken_row_is_reported_and_remaining_rows_still_yielded(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        broken = make_row(table_name="broken")  # missing every other column
        with patch.object(
            api,
            "get_query_result",
            return_value=[table_row("ok"), broken, table_row("ok2")],
        ):
            tables = list(
                api.get_tables_for_dataset(
                    PROJECT, DATASET, {}, report, with_partitions=False
                )
            )

        assert [t.name for t in tables] == ["ok", "ok2"]
        assert len(report.warnings) == 1
        assert f"{PROJECT}.{DATASET}.broken" in str(report.warnings[0].context)

    def test_table_list_item_properties_are_merged_in(self) -> None:
        item = MagicMock()
        item.labels = {"env": "prod"}
        item.clustering_fields = ["a"]
        item.expires = NOW
        item.time_partitioning = None
        item._properties = {"rangePartitioning": {"field": "bucket"}}

        table = BigQuerySchemaApi._make_bigquery_table(
            table_row(
                "events_20240101",
                table_type="EXTERNAL",
                ddl="CREATE EXTERNAL TABLE t OPTIONS(format='CSV', uris=['gs://b/x'])",
            ),
            item,
        )

        assert table.labels == {"env": "prod"}
        assert table.clustering_fields == ["a"]
        assert table.expires == NOW
        assert table.max_shard_id == "20240101"
        assert table.external and table.external_source_format == "CSV"
        assert table.external_source_uris == ["gs://b/x"]
        assert table.partition_info == PartitionInfo(fields=("bucket",), type="RANGE")

    def test_unrepresentable_expiration_is_dropped(self) -> None:
        item = MagicMock()
        type(item).expires = PropertyMock(
            side_effect=OverflowError("year out of range")
        )
        item.time_partitioning = None
        item._properties = {}
        item.labels = None
        item.clustering_fields = None

        table = BigQuerySchemaApi._make_bigquery_table(table_row("t"), item)

        assert table.expires is None


class TestViewsAndSnapshots:
    def test_broken_view_row_is_reported(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        with patch.object(
            api,
            "get_query_result",
            return_value=[make_row(table_name="broken"), view_row("v")],
        ):
            views = list(api.get_views_for_dataset(PROJECT, DATASET, False, report))

        assert [v.name for v in views] == ["v"]
        assert len(report.warnings) == 1

    def test_broken_snapshot_row_is_reported(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        with patch.object(
            api,
            "get_query_result",
            return_value=[make_row(table_name="broken"), snapshot_row("s")],
        ):
            snapshots = list(
                api.get_snapshots_for_dataset(PROJECT, DATASET, True, report)
            )

        assert len(snapshots) == 1
        assert snapshots[0].base_table_identifier is not None
        assert snapshots[0].base_table_identifier.table == "base"
        assert len(report.warnings) == 1


class TestConstraints:
    def test_constraints_are_grouped_by_table_with_references_only_for_foreign_keys(
        self,
    ) -> None:
        api = make_api()
        report = BigQueryV2Report()
        rows = [
            constraint_row(
                "orders",
                "PRIMARY KEY",
                "id",
                {"catalog": "ignored", "table": "ignored"},
            ),
            constraint_row(
                "orders",
                "FOREIGN KEY",
                "customer_id",
                {
                    "catalog": PROJECT,
                    "schema": DATASET,
                    "table": "customers",
                    "column": "id",
                },
            ),
            constraint_row("customers", "PRIMARY KEY", "id"),
        ]
        with patch.object(api, "get_query_result", return_value=rows):
            constraints = api.get_table_constraints_for_dataset(
                PROJECT, DATASET, report
            )

        assert constraints is not None
        assert set(constraints) == {"orders", "customers"}
        pk, fk = constraints["orders"]
        assert pk.referenced_table_name is None
        assert fk.referenced_table_name == "customers"
        assert fk.referenced_column_name == "id"

    def test_constraint_query_failure_is_reported_and_returns_none(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        with patch.object(api, "get_query_result", side_effect=RuntimeError("denied")):
            assert (
                api.get_table_constraints_for_dataset(PROJECT, DATASET, report) is None
            )
        assert len(report.warnings) == 1


class TestColumns:
    def _column_row(
        self,
        table: str,
        name: str,
        position: int,
        is_partitioning_column: str = "NO",
        **extra: Any,
    ) -> Row:
        return make_row(
            table_name=table,
            column_name=name,
            ordinal_position=position,
            field_path=name,
            is_nullable="YES",
            data_type="STRING",
            comment=None,
            is_partitioning_column=is_partitioning_column,
            clustering_ordinal_position=None,
            **extra,
        )

    def test_rows_are_grouped_per_table(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        rows = [self._column_row("wide", f"c{i}", i) for i in range(3)] + [
            self._column_row("narrow", "c0", 1, is_partitioning_column="YES")
        ]
        with patch.object(api, "get_query_result", return_value=rows):
            columns = api.get_columns_for_dataset(
                PROJECT, DATASET, column_limit=10, report=report
            )

        assert columns is not None
        assert [c.name for c in columns["wide"]] == ["c0", "c1", "c2"]
        assert columns["narrow"][0].is_partition_column is True

    def test_column_query_failure_is_reported_and_returns_none(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        with patch.object(api, "get_query_result", side_effect=RuntimeError("denied")):
            assert (
                api.get_columns_for_dataset(
                    PROJECT, DATASET, column_limit=10, report=report
                )
                is None
            )
        assert len(report.warnings) == 1

    def test_unreadable_policy_tags_column_disables_policy_tag_extraction(self) -> None:
        api = make_api()
        report = BigQueryV2Report()
        # An INFORMATION_SCHEMA without the policy_tags column surfaces as a
        # non-iterable value; extraction must degrade rather than abort the dataset.
        rows = [self._column_row("t", "c0", 1, policy_tags=123)]
        with (
            patch.object(api, "get_query_result", return_value=rows),
            patch.object(api, "build_policy_tag_display_name_mapping") as build_mapping,
        ):
            columns = api.get_columns_for_dataset(
                PROJECT,
                DATASET,
                column_limit=10,
                report=report,
                extract_policy_tags_from_catalog=True,
            )

        assert columns is not None and columns["t"][0].policy_tags == []
        build_mapping.assert_not_called()
        assert len(report.warnings) == 1


def test_partition_info_rejects_blank_field_names() -> None:
    with pytest.raises(ValueError):
        PartitionInfo(fields=("dt", " "))


def test_dataset_metadata_batch_groups_by_location() -> None:
    api = make_api()
    datasets = [
        BigqueryDataset(name="a", location="EU"),
        BigqueryDataset(name="b", location=None),
    ]
    calls: List[Dict[str, Any]] = []

    def fetch(project_id: str, location: str, names: Any) -> Dict[str, Dict[str, Any]]:
        calls.append({"location": location, "names": set(names)})
        return {name: {"description": f"{name} doc"} for name in names}

    with patch.object(api, "_fetch_dataset_metadata_batch", side_effect=fetch):
        api._enrich_datasets_with_metadata(PROJECT, datasets)

    assert calls == [
        {"location": "EU", "names": {"a"}},
        {"location": "US", "names": {"b"}},
    ]
    assert [d.comment for d in datasets] == ["a doc", "b doc"]
