from types import SimpleNamespace
from typing import Any, Dict, Iterator, Sequence
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryDataset,
    BigQuerySchemaApi,
)
from datahub.ingestion.source.bigquery_v2.bigquery_test_connection import (
    BigQueryTestConnection,
)
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from tests.unit.bigquery.test_bigquery_audit_log_api import v1_query_entry


def _bq_client(
    query_error: Any = None, projects: Sequence[str] = ("proj-a",)
) -> MagicMock:
    client = MagicMock()
    client.query.return_value = SimpleNamespace(error_result=query_error)
    client.list_projects.return_value = [
        SimpleNamespace(project_id=p) for p in projects
    ]
    return client


@pytest.fixture
def mocked_google_clients() -> Iterator[Dict[str, MagicMock]]:
    """Replace every google client the connection test can reach for."""
    logging_client = MagicMock()
    logging_client.list_entries.return_value = []
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client") as bq,
        patch.object(BigQueryV2Config, "get_projects_client"),
        patch.object(
            BigQueryV2Config, "make_gcp_logging_client", return_value=logging_client
        ),
    ):
        bq.return_value = _bq_client()
        yield {"bigquery": bq, "logging": logging_client}


class TestConnectivity:
    def test_successful_probe_query(self) -> None:
        report = BigQueryTestConnection.connectivity_test(_bq_client())
        assert report.capable

    def test_failed_probe_query_reports_reason(self) -> None:
        client = _bq_client(query_error={"message": "permission denied"})
        report = BigQueryTestConnection.connectivity_test(client)
        assert not report.capable
        assert report.failure_reason


class TestMetadataReadCapability:
    def _config(self) -> BigQueryV2Config:
        return BigQueryV2Config()

    def test_project_without_datasets_is_not_capable(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        with patch.object(
            BigQuerySchemaApi, "get_datasets_for_project_id", return_value=[]
        ):
            report = BigQueryTestConnection.metadata_read_capability_test(
                ["proj-a"], self._config()
            )
        assert not report.capable

    def test_dataset_without_tables_is_not_capable(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        with (
            patch.object(
                BigQuerySchemaApi,
                "get_datasets_for_project_id",
                return_value=[BigqueryDataset(name="ds")],
            ),
            patch.object(BigQuerySchemaApi, "get_tables_for_dataset", return_value=[]),
        ):
            report = BigQueryTestConnection.metadata_read_capability_test(
                ["proj-a"], self._config()
            )
        assert not report.capable

    def test_api_error_is_reported_not_raised(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        with patch.object(
            BigQuerySchemaApi,
            "get_datasets_for_project_id",
            side_effect=RuntimeError("403 forbidden"),
        ):
            report = BigQueryTestConnection.metadata_read_capability_test(
                ["proj-a"], self._config()
            )
        assert not report.capable
        assert "403 forbidden" in (report.failure_reason or "")

    def test_readable_tables_are_capable(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        with (
            patch.object(
                BigQuerySchemaApi,
                "get_datasets_for_project_id",
                return_value=[BigqueryDataset(name="ds")],
            ),
            patch.object(
                BigQuerySchemaApi, "get_tables_for_dataset", return_value=[object()]
            ),
        ):
            report = BigQueryTestConnection.metadata_read_capability_test(
                ["proj-a", "proj-b"], self._config()
            )
        assert report.capable


class TestLineageAndUsageCapability:
    def test_lineage_is_capable_when_audit_log_is_readable(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        config = BigQueryV2Config()
        report = BigQueryTestConnection.lineage_capability_test(
            config, ["proj-a"], BigQueryV2Report()
        )
        assert report.capable
        # Only one log entry is requested for the probe.
        assert (
            mocked_google_clients["logging"].list_entries.call_args.kwargs[
                "max_results"
            ]
            == 1
        )

    def test_lineage_logging_error_is_reported(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        mocked_google_clients["logging"].list_entries.side_effect = RuntimeError(
            "logging api disabled"
        )
        report = BigQueryTestConnection.lineage_capability_test(
            BigQueryV2Config(), ["proj-a"], BigQueryV2Report()
        )
        assert not report.capable
        assert "logging api disabled" in (report.failure_reason or "")

    def test_usage_is_capable_when_an_entry_parses(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        mocked_google_clients["logging"].list_entries.return_value = [
            v1_query_entry(destination="t")
        ]
        report = BigQueryTestConnection.usage_capability_test(
            BigQueryV2Config(), ["proj-a"], BigQueryV2Report()
        )
        assert report.capable

    def test_usage_logging_error_is_reported(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        mocked_google_clients["logging"].list_entries.side_effect = RuntimeError(
            "no permission"
        )
        report = BigQueryTestConnection.usage_capability_test(
            BigQueryV2Config(), ["proj-a"], BigQueryV2Report()
        )
        assert not report.capable
        assert "no permission" in (report.failure_reason or "")

    def test_usage_failure_recorded_in_report_is_not_capable(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        def record_failure(self: BigQueryUsageExtractor, project_id: str) -> None:
            self.report.failure(message="audit log query failed", context=project_id)

        with patch.object(BigQueryUsageExtractor, "test_capability", record_failure):
            report = BigQueryTestConnection.usage_capability_test(
                BigQueryV2Config(), ["proj-a"], BigQueryV2Report()
            )
        assert not report.capable


class TestFullConnectionReport:
    def test_reports_every_enabled_capability(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        mocked_google_clients["bigquery"].return_value = _bq_client(
            projects=["proj-a", "proj-ignored"]
        )
        with (
            patch.object(
                BigQuerySchemaApi,
                "get_datasets_for_project_id",
                return_value=[BigqueryDataset(name="ds")],
            ) as datasets,
            patch.object(
                BigQuerySchemaApi, "get_tables_for_dataset", return_value=[object()]
            ),
        ):
            report = BigQueryTestConnection.test_connection(
                {
                    "project_id_pattern": {"allow": ["proj-a"]},
                    "include_table_lineage": True,
                    "include_usage_statistics": True,
                }
            )

        assert report.basic_connectivity and report.basic_connectivity.capable
        assert report.capability_report is not None
        assert {
            SourceCapability.SCHEMA_METADATA,
            SourceCapability.LINEAGE_COARSE,
            SourceCapability.USAGE_STATS,
        } <= set(report.capability_report)
        assert all(c.capable for c in report.capability_report.values())
        # The project pattern limits which projects are probed.
        probed_projects = [call.args[0] for call in datasets.call_args_list]
        assert probed_projects == ["proj-a"]

    def test_disabled_features_are_not_reported(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        with (
            patch.object(
                BigQuerySchemaApi,
                "get_datasets_for_project_id",
                return_value=[BigqueryDataset(name="ds")],
            ),
            patch.object(
                BigQuerySchemaApi, "get_tables_for_dataset", return_value=[object()]
            ),
        ):
            report = BigQueryTestConnection.test_connection(
                {"include_table_lineage": False, "include_usage_statistics": False}
            )

        assert report.capability_report is not None
        assert set(report.capability_report) == {SourceCapability.SCHEMA_METADATA}

    def test_client_construction_failure_is_basic_connectivity_failure(
        self, mocked_google_clients: Dict[str, MagicMock]
    ) -> None:
        mocked_google_clients["bigquery"].side_effect = RuntimeError("bad credential")

        report = BigQueryTestConnection.test_connection({})

        assert report.basic_connectivity is not None
        assert not report.basic_connectivity.capable
        assert "bad credential" in (report.basic_connectivity.failure_reason or "")
        assert report.capability_report is None
