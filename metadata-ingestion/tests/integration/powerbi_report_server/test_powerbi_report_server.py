from datetime import datetime
from unittest import mock
from unittest.mock import patch

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi_report_server import (
    CorpUser,
    PowerBiReport,
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
                Id="ee56dc21-248a-4138-a446-ee5ab1fc938b",
                Name="Test",
                Description=None,
                Path="/path/to/Test",
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
                UserInfo=CorpUser(
                    active=True,
                    display_name="User1",
                    email="User1@foo.com",
                    title=None,
                    manager=None,
                    department_id=None,
                    department_name=None,
                    first_name=None,
                    last_name=None,
                    full_name=None,
                    country_code=None,
                ),
                DisplayName="TEST_USER",
                HasDataSources=True,
                DataSources=[],
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
                        "workstation_name": "workstation",
                        "host_port": "host_port",
                        "server_alias": "server_alias",
                        "graphql_url": "http://localhost:8080/api/graphql",
                        "report_virtual_directory_name": "Reports",
                        "report_server_virtual_directory_name": "ReportServer",
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
