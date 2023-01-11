from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-02-03 07:00:00"


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def acquire_token_for_client(self, *args, **kwargs):
            return {
                "access_token": "dummy",
            }

    return MsalClient()


def register_mock_api(request_mock, pytestconfig):
    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified": {
            "method": "GET",
            "status_code": 200,
            "json": [
                {"id": "4674efd1-603c-4129-8d82-03cf2be05aff"},
            ],
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/4674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "status": "SUCCEEDED",
            },
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/4674efd1-603c-4129-8d82-03cf2be05aff": {
            "method": "GET",
            "status_code": 200,
            "json": mce_helpers.load_json_file(
                pytestconfig.rootpath
                / "tests/integration/powerbiv2/mock_powerbi_scan_result.json"
            ),
        },
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 202,
            "json": {
                "id": "4674efd1-603c-4129-8d82-03cf2be05aff",
            },
        },
    }

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


TEST_POWERBIV2_CONFIG = {
    "client_id": "foo",
    "client_secret": "bar",
    "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
    "workspace_pattern": {"allow": ["4674efd1-603c-4129-8d82-03cf2be05aff"]},
    "modified_since": None,
    "ownership": {
        "enabled": True,
        "use_powerbi_email": True,
        "remove_email_suffix": True,
    },
    "ingest_catalog_container": True,
    "extract_tile": True,
    "extract_report": True,
    "extract_dataset": True,
    "extract_dashboard": True,
}


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.parametrize(
    "test_name,update_test_config,test_suffix",
    [
        ("standard", {}, "_1"),
        (
            "no_ownership",
            {
                "ownership": {
                    "enabled": False,
                    "use_powerbi_email": False,
                    "remove_email_suffix": False,
                }
            },
            "_2",
        ),
        (
            "with_modifiedsince_no_dashboard_no_dataset",
            {
                "modified_since": "2022-01-29T00:00:00.0000000Z",
                "extract_dashboard": False,
                "extract_dataset": False,
            },
            "_3",
        ),
    ],
)
def test_powerbi_ingest(
    mock_msal,
    pytestconfig,
    tmp_path,
    mock_time,
    requests_mock,
    test_name,
    update_test_config,
    test_suffix,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbiv2"

    register_mock_api(request_mock=requests_mock, pytestconfig=pytestconfig)
    final_test_config = {**TEST_POWERBIV2_CONFIG, **update_test_config}
    pipeline = Pipeline.create(
        {
            "run_id": f"powerbiv2-test-{test_name}",
            "source": {
                "type": "powerbiv2",
                "config": final_test_config,
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbiv2_mces{test_suffix}.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = f"powerbiv2_mces_golden{test_suffix}.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / f"powerbiv2_mces{test_suffix}.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )
