"""Request mocks and the base recipe shared by the Power BI integration tests."""

import json
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

import pytest


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
    # Request mock is passing POST input in the form of workspaces=<workspace_id>
    # If we scan 2 or more, it get messy like this. 'workspaces=64ED5CAD-7C10-4684-8180-826122881108&workspaces=64ED5CAD-7C22-4684-8180-826122881108'
    workspace_id_list = request.text.replace("&", "").split("workspaces=")

    workspace_id = "||".join(workspace_id_list[1:])

    w_id_vs_response: Dict[str, Any] = {
        "64ED5CAD-7C10-4684-8180-826122881108": {
            "id": "4674efd1-603c-4129-8d82-03cf2be05aff"
        },
        "64ED5CAD-7C22-4684-8180-826122881108": {
            "id": "a674efd1-603c-4129-8d82-03cf2be05aff"
        },
        "64ED5CAD-7C10-4684-8180-826122881108||64ED5CAD-7C22-4684-8180-826122881108": {
            "id": "a674efd1-603c-4129-8d82-03cf2be05aff"
        },
        "A8D655A6-F521-477E-8C22-255018583BF4": {
            "id": "62DAF926-0B18-4FF1-982C-2A3EB6B8F0E4"
        },
        "C5DA6EA8-625E-4AB1-90B6-CAEA0BF9F492": {
            "id": "81B02907-E2A3-45C3-B505-3781839C8CAA",
        },
        "8F756DE6-26AD-45FF-A201-44276FF1F561": {
            "id": "6147FCEB-7531-4449-8FB6-1F7A5431BF2D",
        },
    }

    return w_id_vs_response[workspace_id]


def read_mock_data(path: Union[Path, str]) -> dict:
    with open(path) as p:
        return json.load(p)


def register_mock_api(
    pytestconfig: pytest.Config,
    request_mock: Any,
    override_data: Optional[dict] = None,
    *,
    replace_hostname: Optional[str] = None,
) -> None:
    default_mock_data_path = (
        pytestconfig.rootpath
        / "tests/integration/powerbi/mock_data/default_mock_response.json"
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
        if replace_hostname:
            mock_url = urlparse(url)._replace(netloc=replace_hostname).geturl()
        else:
            mock_url = url

        request_mock.register_uri(
            api_vs_response[url]["method"],
            mock_url,
            json=api_vs_response[url].get("json"),
            text=api_vs_response[url].get("text"),
            status_code=api_vs_response[url]["status_code"],
        )


def default_source_config():
    return {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "workspace_id": "64ED5CAD-7C10-4684-8180-826122881108",
        "extract_lineage": False,
        "extract_column_level_lineage": False,
        "extract_reports": False,
        "extract_ownership": True,
        "convert_lineage_urns_to_lowercase": False,
        "workspace_id_pattern": {"allow": ["64ED5CAD-7C10-4684-8180-826122881108"]},
        "dataset_type_mapping": {
            "PostgreSql": "postgres",
            "Oracle": "oracle",
        },
        "env": "DEV",
        "extract_workspaces_to_containers": False,
        "enable_advance_lineage_sql_construct": False,
        # Explicitly set to True to maintain backward compatibility with golden files
        # The new default is False, but tests rely on existing golden files
        "ownership": {
            "create_corp_user": True,
        },
    }
