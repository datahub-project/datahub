from datetime import datetime
from unittest import mock
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline

from datahub.ingestion.source.powerbi_report_server.report_server_domain import (
    PowerBiReport,
    Report,
)
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-07-08 07:00:00"

POWERBI_REPORT_SERVER_API = "datahub.ingestion.source.powerbi_report_server.report_server.PowerBiReportServerAPI"
POWERBI_REPORT_SERVER_USER_DAO_GET_OWNER = "datahub.ingestion.source.powerbi_report_server.report_server.UserDao.get_owner_by_name"


@freeze_time(FROZEN_TIME)
@patch(POWERBI_REPORT_SERVER_USER_DAO_GET_OWNER)
def test_powerbi_report_server_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    # Set the datasource mapping dictionary
    with mock.patch(POWERBI_REPORT_SERVER_API) as mock_sdk:
        mock_sdk.return_value = mocked_client
        # Mock the PowerBi Dashboard API response
        mocked_client.get_all_reports.return_value = [
            PowerBiReport(
                Id="c2259cbb-472f-4ee4-897a-97cf24d943db",
                Name="6B536811-2E00-4961-9845-8559BA16C122",
                Description=None,
                Path="/path/to/6B536811-2E00-4961-9845-8559BA16C122",
                Type="PowerBIReport",
                Hidden=False,
                Size=1010101,
                ModifiedBy="TEST_USER",
                ModifiedDate=datetime.now(),
                CreatedBy="TEST_USER",
                CreatedDate=datetime.now(),
                ParentFolderId="47495172-89ab-455f-a446-fffd3cf239cb",
                ContentType=None,
                Content="",
                IsFavorite=False,
                UserInfo=None,
                DisplayName="TEST_USER",
                HasDataSources=True,
                DataSources=[],
            ),
            Report(
                Id="ee56dc21-248a-4138-a446-ee5ab1fc938b",
                Name="Test_rdl",
                Description=None,
                Path="/path/to/Test_rdl",
                Type="Report",
                Hidden=False,
                Size=1010101,
                ModifiedBy="TEST_USER",
                ModifiedDate=datetime.now(),
                CreatedBy="TEST_USER",
                CreatedDate=datetime.now(),
                ParentFolderId="47495172-89ab-455f-a446-fffd3cf239cb",
                ContentType=None,
                Content="",
                IsFavorite=False,
                UserInfo=None,
                DisplayName="TEST_USER",
                HasDataSources=True,
                DataSources=[],
                HasSharedDataSets=False,
                HasParameters=False,
            ),
        ]

        test_resources_dir = (
            pytestconfig.rootpath / "tests/integration/powerbi_report_server"
        )

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-report-server-test",
                "source": {
                    "type": "powerbi-report-server",
                    "config": {
                        "username": "foo",
                        "password": "bar",
                        "workstation_name": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
                        "host_port": "64ED5CAD-7C10-4684-8180-826122881108",
                        "server_alias": "64ED5CAD",
                        "graphql_url": "http://localhost:8080/api/graphql",
                        "report_virtual_directory_name": "Reports",
                        "report_server_virtual_directory_name": "ReportServer",
                        "dataset_type_mapping": {
                            "PostgreSql": "postgres",
                            "Oracle": "oracle",
                        },
                        "env": "DEV",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/powerbi_report_server_mces.json",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "powerbi_report_server_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )
