from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi import PowerBiAPI
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"

POWERBI_API = "datahub.ingestion.source.powerbi.PowerBiAPI"


@freeze_time(FROZEN_TIME)
def test_powerbi_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    # Set the datasource mapping dictionary
    with mock.patch(POWERBI_API) as mock_sdk:
        mock_sdk.return_value = mocked_client
        # Mock the PowerBi Dashboard API response
        mocked_client.get_workspace.return_value = PowerBiAPI.Workspace(
            id="64ED5CAD-7C10-4684-8180-826122881108",
            name="demo-workspace",
            state="Active",
            datasets={},
            dashboards=[
                PowerBiAPI.Dashboard(
                    id="7D668CAD-7FFC-4505-9215-655BCA5BEBAE",
                    displayName="test_dashboard",
                    isReadOnly=True,
                    embedUrl="https://localhost/dashboards/embed/1",
                    webUrl="https://localhost/dashboards/web/1",
                    workspace_id="4A378B07-FAA2-4EA2-9383-CBA91AD9681C",
                    workspace_name="foo",
                    users=[],
                    tiles=[
                        PowerBiAPI.Tile(
                            id="B8E293DC-0C83-4AA0-9BB9-0A8738DF24A0",
                            title="test_tiles",
                            embedUrl="https://localhost/tiles/embed/1",
                            createdFrom=PowerBiAPI.Tile.CreatedFrom.DATASET,
                            report=None,
                            dataset=PowerBiAPI.Dataset(
                                id="05169CD2-E713-41E6-9600-1D8066D95445",
                                name="library-dataset",
                                workspace_id="64ED5CAD-7C10-4684-8180-826122881108",
                                webUrl="http://localhost/groups/64ED5CAD-7C10-4684-8180-826122881108/datasets/05169CD2-E713-41E6-9600-1D8066D95445",
                                tables=[
                                    PowerBiAPI.Dataset.Table(
                                        name="issue_book", schema_name="public"
                                    ),
                                    PowerBiAPI.Dataset.Table(
                                        name="member", schema_name="public"
                                    ),
                                    PowerBiAPI.Dataset.Table(
                                        name="issue_history", schema_name="public"
                                    ),
                                ],
                                datasource=PowerBiAPI.DataSource(
                                    id="DCE90B40-84D6-467A-9A5C-648E830E72D3",
                                    database="library_db",
                                    type="PostgreSql",
                                    server="gs-library.postgres.database.azure.com",
                                    metadata=PowerBiAPI.DataSource.MetaData(
                                        is_relational=True
                                    ),
                                ),
                            ),
                        )
                    ],
                )
            ],
        )

        # Mock the PowerBi User API response
        mocked_client.get_dashboard_users.return_value = [
            PowerBiAPI.User(
                id="User1@foo.com",
                displayName="User1",
                emailAddress="User1@foo.com",
                dashboardUserAccessRight="ReadWrite",
                principalType="User",
                graphId="C9EE53F2-88EA-4711-A173-AF0515A3CD46",
            ),
            PowerBiAPI.User(
                id="User2@foo.com",
                displayName="User2",
                emailAddress="User2@foo.com",
                dashboardUserAccessRight="ReadWrite",
                principalType="User",
                graphId="5ED26AA7-FCD2-42C5-BCE8-51E0AAD0682B",
            ),
        ]

        test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "powerbi",
                    "config": {
                        "client_id": "foo",
                        "client_secret": "bar",
                        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
                        "workspace_id": "64ED5CAD-7C10-4684-8180-826122881108",
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
                        "filename": f"{tmp_path}/powerbi_mces.json",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "powerbi_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )
