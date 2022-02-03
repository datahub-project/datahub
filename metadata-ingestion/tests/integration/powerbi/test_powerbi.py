from unittest import mock

from freezegun import freeze_time


from datahub.ingestion.source.powerbi import PowerBiAPI

from datahub.ingestion.run.pipeline import Pipeline

from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"

POWERBI_API = 'datahub.ingestion.source.powerbi.PowerBiAPI'


@freeze_time(FROZEN_TIME)
def test_powerbi_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch(POWERBI_API) as mock_sdk:
        mock_sdk.return_value = mocked_client
        # Mock the PowerBi Dashboard API response 
        mocked_client.get_dashboards.return_value = [
            PowerBiAPI.Dashboard(
                
                id="7D668CAD-7FFC-4505-9215-655BCA5BEBAE",
                displayName="test_dashboard",
                isReadOnly=True,
                embedUrl="https://localhost/dashboards/embed/1",
                webUrl="https://localhost/dashboards/web/1",
                workspace_id="4A378B07-FAA2-4EA2-9383-CBA91AD9681C",
                tiles=[]
            )
        ]

        # Mock the PowerBi Tile API response 
        mocked_client.get_tiles.return_value = [
            PowerBiAPI.Tile(
                id="B8E293DC-0C83-4AA0-9BB9-0A8738DF24A0",
                title="test_tiles",
                embedUrl="https://localhost/tiles/embed/1",
                dataset=PowerBiAPI.Dataset( 
                    id="05169CD2-E713-41E6-9600-1D8066D95445",
                    name="test_dataset"
                )
            )
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
                        "workspace_id": "74DDC90A-E3AD-499E-8F8F-9ACFA1295CA2"
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


