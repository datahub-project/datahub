from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.quicksight.quicksight import QuickSightSource
from datahub.ingestion.source.source_registry import source_registry


def _patched_api(pages=None):
    """Patch the boto3-backed clients so the source can be constructed offline.

    ``pages`` maps a paginator operation name to the list of page dicts it
    should return (defaults to empty for any operation not provided).
    """
    pages = pages or {}
    quicksight_client = mock.MagicMock()

    def get_paginator(operation_name):
        paginator = mock.MagicMock()
        paginator.paginate.return_value = pages.get(operation_name, [{}])
        return paginator

    quicksight_client.get_paginator.side_effect = get_paginator

    sts_client = mock.MagicMock()
    sts_client.get_caller_identity.return_value = {"Account": "064369473231"}
    return quicksight_client, sts_client


def _create_source(
    quicksight_client: mock.MagicMock, sts_client: mock.MagicMock
) -> QuickSightSource:
    with (
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config.QuickSightSourceConfig.get_quicksight_client",
            return_value=quicksight_client,
        ),
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config.QuickSightSourceConfig.get_sts_client",
            return_value=sts_client,
        ),
    ):
        return QuickSightSource.create(
            {"aws_region": "us-east-1"}, PipelineContext(run_id="test")
        )


def test_source_registered_in_plugin_registry():
    source_class = source_registry.get("quicksight")
    assert source_class is QuickSightSource


def test_basic_run_emits_containers_and_builds_data_source_map():
    quicksight_client, sts_client = _patched_api(
        pages={
            "list_namespaces": [{"Namespaces": [{"Name": "default"}]}],
            "list_folders": [{}],
            "list_data_sources": [
                {
                    "DataSources": [
                        {
                            "Arn": "arn:aws:quicksight:us-east-1:064369473231:datasource/ds-1",
                            "DataSourceId": "ds-1",
                            "Name": "Athena DS",
                            "Type": "ATHENA",
                        }
                    ]
                }
            ],
        }
    )
    quicksight_client.describe_data_source.return_value = {
        "DataSource": {
            "DataSourceParameters": {"AthenaParameters": {"WorkGroup": "primary"}}
        }
    }

    source = _create_source(quicksight_client, sts_client)
    workunits = list(source.get_workunits_internal())

    # No account/namespace containers by default; emits the data source dataset.
    assert workunits
    # The lineage map is populated with the resolved upstream platform.
    arn = "arn:aws:quicksight:us-east-1:064369473231:datasource/ds-1"
    assert source.data_source_map[arn].platform == "athena"
    assert source.data_source_map[arn].dialect == "athena"


def _run_test_connection(quicksight_client, sts_client):
    with (
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config.QuickSightSourceConfig.get_quicksight_client",
            return_value=quicksight_client,
        ),
        mock.patch(
            "datahub.ingestion.source.quicksight.quicksight_config.QuickSightSourceConfig.get_sts_client",
            return_value=sts_client,
        ),
    ):
        return QuickSightSource.test_connection({"aws_region": "us-east-1"})


def test_test_connection_success():
    quicksight_client, sts_client = _patched_api()
    quicksight_client.get_paginator.return_value.paginate.return_value = [{}]

    report = _run_test_connection(quicksight_client, sts_client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True


def test_test_connection_sts_failure_blames_credentials():
    quicksight_client, sts_client = _patched_api()
    # Credentials layer fails before any QuickSight call is attempted.
    sts_client.get_caller_identity.side_effect = Exception("ExpiredToken")

    report = _run_test_connection(quicksight_client, sts_client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    # Diagnostic must point at the credentials layer, not QuickSight permissions.
    assert "sts:GetCallerIdentity" in report.basic_connectivity.failure_reason


def test_test_connection_list_namespaces_failure_blames_reader_role():
    quicksight_client, sts_client = _patched_api()

    def paginate(operation_name):
        paginator = mock.MagicMock()
        if operation_name == "list_namespaces":
            paginator.paginate.side_effect = Exception("AccessDeniedException")
        else:
            paginator.paginate.return_value = [{}]
        return paginator

    quicksight_client.get_paginator.side_effect = paginate

    report = _run_test_connection(quicksight_client, sts_client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    # QuickSight blames IAM even when the QS user role is the real cause, so the
    # diagnostic must call out the READER-role possibility.
    assert "READER" in report.basic_connectivity.failure_reason


def test_test_connection_list_dashboards_failure_reports_dashboards_layer():
    quicksight_client, sts_client = _patched_api()

    def paginate(operation_name):
        paginator = mock.MagicMock()
        if operation_name == "list_dashboards":
            paginator.paginate.side_effect = Exception("AccessDeniedException")
        else:
            paginator.paginate.return_value = [{}]
        return paginator

    quicksight_client.get_paginator.side_effect = paginate

    report = _run_test_connection(quicksight_client, sts_client)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "quicksight:ListDashboards" in report.basic_connectivity.failure_reason
