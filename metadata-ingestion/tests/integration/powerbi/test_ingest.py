import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

pytestmark = pytest.mark.integration_batch_2
FROZEN_TIME = "2022-02-03 07:00:00"


def mock_msal_cca(*args, **kwargs):
    class MsalClient:
        def __init__(self):
            self.call_num = 0
            self.token: Dict[str, Any] = {
                "access_token": "dummy",
            }

        def acquire_token_for_client(self, *args, **kwargs):
            self.call_num += 1
            return self.token

        def reset(self):
            self.call_num = 0

    return MsalClient()


def scan_init_response(request, context):
    workspace_id_list = request.text.replace("&", "").split("workspaces=")

    workspace_id = "||".join(workspace_id_list[1:])

    w_id_vs_response: Dict[str, Any] = {
        "64ed5cad-7c10-4684-8180-826122881108": {
            "id": "4674efd1-603c-4129-8d82-03cf2be05aff"
        }
    }

    return w_id_vs_response[workspace_id]


def read_mock_data(path: Union[Path, str]) -> dict:
    with open(path) as p:
        return json.load(p)


def register_mock_api(
    pytestconfig: pytest.Config, request_mock: Any, override_data: Optional[dict] = None
) -> None:
    default_mock_data_path = (
        pytestconfig.rootpath
        / "tests/integration/powerbi/mock_data/mysql_mock_response.json"
    )

    api_vs_response = {
        "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
            "method": "POST",
            "status_code": 200,
            "json": scan_init_response,
        },
    }

    api_vs_response.update(read_mock_data(default_mock_data_path))

    api_vs_response.update(override_data or {})

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url].get("json"),
            text=api_vs_response[url].get("text"),
            status_code=api_vs_response[url]["status_code"],
        )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_mysql_ingest(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(
        request_mock=requests_mock,
        pytestconfig=pytestconfig,
        override_data=read_mock_data(
            pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/mysql_mock_response.json"
        ),
    )

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    "tenant_id": "0b0c960b-fcdf-4d0f-8c45-2e03bb59ddeb",
                    "client_id": "a8d655a6-f521-477e-8c22-255018583bf4",
                    "client_secret": "ababa~cdcdcdcdcdcdcdcdcdcd-abcd.defghijk",
                    "extract_ownership": True,
                    "extract_endorsements_to_tags": True,
                    "extract_app": True,
                    "extract_column_level_lineage": True,
                    "workspace_name_pattern": {"allow": ["^Employees$"]},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mysql_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_mysql.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mysql_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_mysql_odbc_ingest(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(
        request_mock=requests_mock,
        pytestconfig=pytestconfig,
        override_data=read_mock_data(
            pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/mysql_odbc_mock_response.json"
        ),
    )

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    "tenant_id": "0b0c960b-fcdf-4d0f-8c45-2e03bb59ddeb",
                    "client_id": "a8d655a6-f521-477e-8c22-255018583bf4",
                    "client_secret": "ababa~cdcdcdcdcdcdcdcdcdcd-abcd.defghijk",
                    "extract_ownership": True,
                    "extract_endorsements_to_tags": True,
                    "extract_app": True,
                    "extract_column_level_lineage": True,
                    "workspace_name_pattern": {"allow": ["^Employees$"]},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mysql_odbc_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_mysql_odbc.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mysql_odbc_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )
