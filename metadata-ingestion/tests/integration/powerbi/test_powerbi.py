import datetime
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.ingestion.api.source import StructuredLogLevel
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Page,
    Report,
    ReportType,
    Workspace,
)
from tests.test_helpers import mce_helpers, test_connection_helpers

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
    pytestconfig: pytest.Config, request_mock: Any, override_data: Optional[dict] = None
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

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
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
    }


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_ingest(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
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
    golden_file = "golden_test_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_workspace_type_filter(
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
            / "tests/integration/powerbi/mock_data/workspace_type_filter.json"
        ),
    )

    default_config: dict = default_source_config()

    del default_config["workspace_id"]
    del default_config["workspace_id_pattern"]

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_config,
                    "extract_workspaces_to_containers": True,
                    "workspace_type_filter": [
                        "PersonalGroup",
                    ],
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
    golden_file = "golden_test_personal_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_ingest_patch_disabled(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "patch_metadata": False,
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
    golden_file = "golden_test_ingest_patch_disabled.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_test_connection_success(mock_msal):
    report = test_connection_helpers.run_test_connection(
        PowerBiDashboardSource, default_source_config()
    )
    test_connection_helpers.assert_basic_connectivity_success(report)


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_powerbi_test_connection_failure():
    report = test_connection_helpers.run_test_connection(
        PowerBiDashboardSource, default_source_config()
    )
    test_connection_helpers.assert_basic_connectivity_failure(
        report, "Unable to get authority configuration"
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_platform_instance_ingest(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    output_path: str = f"{tmp_path}/powerbi_platform_instance_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "platform_instance": "aws-ap-south-1",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_platform_instance_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_ingest_urn_lower_case(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "env": "PROD",
                    "platform_instance": "myPlatformInstance",
                    "convert_urns_to_lowercase": True,
                    "convert_lineage_urns_to_lowercase": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_lower_case_urn_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_lower_case_urn_ingest.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_lower_case_urn_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_override_ownership(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_ownership": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mces_disabled_ownership.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_disabled_ownership.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces_disabled_ownership.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_scan_all_workspaces(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": False,
                    "extract_ownership": False,
                    "workspace_id_pattern": {
                        "deny": ["64ED5CAD-7322-4684-8180-826122881108"],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mces_scan_all_workspaces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    golden_file = "golden_test_scan_all_workspaces.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces_scan_all_workspaces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_extract_reports(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_report_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_report.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_report_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_extract_lineage(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-lineage-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_lineage": True,
                    "dataset_type_mapping": {
                        "PostgreSql": {"platform_instance": "operational_instance"},
                        "Oracle": {
                            "platform_instance": "high_performance_production_unit"
                        },
                        "Sql": {"platform_instance": "reporting-db"},
                        "Snowflake": {"platform_instance": "sn-2"},
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_lineage_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_lineage.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_lineage_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_extract_endorsements(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": False,
                    "extract_endorsements_to_tags": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_endorsement_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_endorsement.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_endorsement_mces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_admin_access_is_not_allowed(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data={
            "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo": {
                "method": "POST",
                "status_code": 403,
                "json": {},
            },
        },
    )

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-admin-api-disabled-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_lineage": True,
                    "dataset_type_mapping": {
                        "PostgreSql": {"platform_instance": "operational_instance"},
                        "Oracle": {
                            "platform_instance": "high_performance_production_unit"
                        },
                        "Sql": {"platform_instance": "reporting-db"},
                        "Snowflake": {"platform_instance": "sn-2"},
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/golden_test_admin_access_not_allowed_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_admin_access_not_allowed.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/golden_test_admin_access_not_allowed_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_workspace_container(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "workspace_id_pattern": {
                        "deny": ["64ED5CAD-7322-4684-8180-826122881108"],
                    },
                    "extract_workspaces_to_containers": True,
                    "extract_datasets_to_containers": True,
                    "extract_reports": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_container_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    mce_out_file = "golden_test_container.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_container_mces.json",
        golden_path=f"{test_resources_dir}/{mce_out_file}",
    )


def test_access_token_expiry_with_long_expiry(
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    mock_msal = mock_msal_cca()

    with mock.patch("msal.ConfidentialClientApplication", return_value=mock_msal):
        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "powerbi",
                    "config": {
                        **default_source_config(),
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/powerbi_access_token_mces.json",
                    },
                },
            }
        )

    # for long expiry, the token should only be requested once.
    mock_msal.token = {
        "access_token": "dummy2",
        "expires_in": 3600,
    }
    mock_msal.reset()

    pipeline.run()
    # We expect the token to be requested twice (once for AdminApiResolver and one for RegularApiResolver)
    assert mock_msal.call_num == 2


def test_access_token_expiry_with_short_expiry(
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    mock_msal = mock_msal_cca()
    with mock.patch("msal.ConfidentialClientApplication", return_value=mock_msal):
        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "powerbi",
                    "config": {
                        **default_source_config(),
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/powerbi_access_token_mces.json",
                    },
                },
            }
        )

    # for short expiry, the token should be requested when expires.
    mock_msal.token = {
        "access_token": "dummy",
        "expires_in": 0,
    }
    mock_msal.reset()

    pipeline.run()
    assert mock_msal.call_num > 2


def dataset_type_mapping_set_to_all_platform(pipeline: Pipeline) -> None:
    source_config: PowerBiDashboardSourceConfig = cast(
        PowerBiDashboardSource, pipeline.source
    ).source_config

    assert source_config.dataset_type_mapping is not None

    # Generate default dataset_type_mapping and compare it with source_config.dataset_type_mapping
    default_dataset_type_mapping: dict = {}
    for item in SupportedDataPlatform:
        default_dataset_type_mapping[item.value.powerbi_data_platform_name] = (
            item.value.datahub_data_platform_name
        )

    assert default_dataset_type_mapping == source_config.dataset_type_mapping


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_dataset_type_mapping_should_set_to_all(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    """
    Here we don't need to run the pipeline. We need to verify dataset_type_mapping is set to default dataplatform
    """
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    new_config: dict = {**default_source_config()}

    del new_config["dataset_type_mapping"]

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **new_config,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_lower_case_urn_mces.json",
                },
            },
        }
    )

    dataset_type_mapping_set_to_all_platform(pipeline)


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_dataset_type_mapping_error(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    """
    Here we don't need to run the pipeline. We need to verify if both dataset_type_mapping and server_to_platform_instance
    are set then value error should get raised
    """
    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    with pytest.raises(Exception, match=r"dataset_type_mapping is deprecated"):
        Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "powerbi",
                    "config": {
                        **default_source_config(),
                        "server_to_platform_instance": {
                            "localhost": {
                                "platform_instance": "test",
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/powerbi_lower_case_urn_mces.json",
                    },
                },
            }
        )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_server_to_platform_map(
    mock_msal, pytestconfig, tmp_path, mock_time, requests_mock
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"
    new_config: dict = {
        **default_source_config(),
        "extract_lineage": True,
        "convert_lineage_urns_to_lowercase": True,
    }

    del new_config["dataset_type_mapping"]

    new_config["server_to_platform_instance"] = {
        "hp123rt5.ap-southeast-2.fakecomputing.com": {
            "platform_instance": "snowflake_production_instance",
            "env": "PROD",
        },
        "my-test-project": {
            "platform_instance": "bigquery-computing-dev-account",
            "env": "QA",
        },
        "localhost:1521": {"platform_instance": "oracle-sales-instance", "env": "PROD"},
    }

    register_mock_api(pytestconfig=pytestconfig, request_mock=requests_mock)

    output_path: str = f"{tmp_path}/powerbi_server_to_platform_instance_mces.json"

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": new_config,
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file_path: str = (
        f"{test_resources_dir}/golden_test_server_to_platform_instance.json"
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_file_path,
    )
    # As server_to_platform_instance map is provided, the old dataset_type_mapping
    # should be set to all supported platform
    # to process all available upstream lineage even if mapping for platform instance is
    # not provided in server_to_platform_instance map
    dataset_type_mapping_set_to_all_platform(pipeline)


def validate_pipeline(pipeline: Pipeline) -> None:
    mock_workspace: Workspace = Workspace(
        id="64ED5CAD-7C10-4684-8180-826122881108",
        name="demo-workspace",
        type="Workspace",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )
    # Fetch actual reports
    reports: Dict[str, Report] = cast(
        PowerBiDashboardSource, pipeline.source
    ).powerbi_client.get_reports(workspace=mock_workspace)

    assert len(reports) == 2
    # Generate expected reports using mock reports
    mock_reports: List[Dict] = [
        {
            "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
            "id": "5b218778-e7a5-4d73-8187-f10824047715",
            "name": "SalesMarketing",
            "description": "Acryl sales marketing report",
            "pages": [
                {
                    "name": "ReportSection",
                    "displayName": "Regional Sales Analysis",
                    "order": "0",
                },
                {
                    "name": "ReportSection1",
                    "displayName": "Geographic Analysis",
                    "order": "1",
                },
            ],
        },
        {
            "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
            "id": "e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
            "name": "Product",
            "description": "Acryl product report",
            "pages": [],
        },
    ]
    expected_reports: Dict[str, Report] = {
        report[Constant.ID]: Report(
            id=report[Constant.ID],
            name=report[Constant.NAME],
            type=ReportType.PowerBIReport,
            webUrl="",
            embedUrl="",
            description=report[Constant.DESCRIPTION],
            pages=[
                Page(
                    id="{}.{}".format(
                        report[Constant.ID], page[Constant.NAME].replace(" ", "_")
                    ),
                    name=page[Constant.NAME],
                    displayName=page[Constant.DISPLAY_NAME],
                    order=page[Constant.ORDER],
                )
                for page in report["pages"]
            ],
            users=[],
            tags=[],
            dataset_id=report[Constant.DATASET_ID],
            dataset=mock_workspace.datasets.get(report[Constant.DATASET_ID]),
        )
        for report in mock_reports
    }
    # Compare actual and expected reports
    for i in range(2):
        report_id = mock_reports[i][Constant.ID]
        assert reports[report_id].id == expected_reports[report_id].id
        assert reports[report_id].name == expected_reports[report_id].name
        assert reports[report_id].description == expected_reports[report_id].description
        assert reports[report_id].dataset == expected_reports[report_id].dataset
        assert reports[report_id].pages == expected_reports[report_id].pages


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_reports_with_failed_page_request(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    """
    Test that all reports are fetched even if a single page request fails
    """
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data={
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "value": [
                        {
                            "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                            "id": "5b218778-e7a5-4d73-8187-f10824047715",
                            "reportType": "PowerBIReport",
                            "name": "SalesMarketing",
                            "description": "Acryl sales marketing report",
                            "webUrl": "https://app.powerbi.com/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715",
                            "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=5b218778-e7a5-4d73-8187-f10824047715&groupId=64ED5CAD-7C10-4684-8180-826122881108",
                        },
                        {
                            "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                            "id": "e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
                            "reportType": "PaginatedReport",
                            "name": "Product",
                            "description": "Acryl product report",
                            "webUrl": "https://app.powerbi.com/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
                            "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=e9fd6b0b-d8c8-4265-8c44-67e183aebf97&groupId=64ED5CAD-7C10-4684-8180-826122881108",
                        },
                    ]
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                    "id": "5b218778-e7a5-4d73-8187-f10824047715",
                    "name": "SalesMarketing",
                    "reportType": "PowerBIReport",
                    "description": "Acryl sales marketing report",
                    "webUrl": "https://app.powerbi.com/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715",
                    "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=5b218778-e7a5-4d73-8187-f10824047715&groupId=64ED5CAD-7C10-4684-8180-826122881108",
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/e9fd6b0b-d8c8-4265-8c44-67e183aebf97": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "datasetId": "05169CD2-E713-41E6-9600-1D8066D95445",
                    "id": "e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
                    "reportType": "PowerBIReport",
                    "name": "Product",
                    "description": "Acryl product report",
                    "webUrl": "https://app.powerbi.com/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/e9fd6b0b-d8c8-4265-8c44-67e183aebf97",
                    "embedUrl": "https://app.powerbi.com/reportEmbed?reportId=e9fd6b0b-d8c8-4265-8c44-67e183aebf97&groupId=64ED5CAD-7C10-4684-8180-826122881108",
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/5b218778-e7a5-4d73-8187-f10824047715/pages": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "value": [
                        {
                            "displayName": "Regional Sales Analysis",
                            "name": "ReportSection",
                            "order": "0",
                        },
                        {
                            "displayName": "Geographic Analysis",
                            "name": "ReportSection1",
                            "order": "1",
                        },
                    ]
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/reports/e9fd6b0b-d8c8-4265-8c44-67e183aebf97/pages": {
                "method": "GET",
                "status_code": 400,
                "json": {
                    "error": {
                        "code": "InvalidRequest",
                        "message": "Request is currently not supported for RDL reports",
                    }
                },
            },
        },
    )

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_reports": True,
                    "platform_instance": "aws-ap-south-1",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}powerbi_reports_with_failed_page_request_mces.json",
                },
            },
        }
    )

    validate_pipeline(pipeline)


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_independent_datasets_extraction(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data={
            "https://api.powerbi.com/v1.0/myorg/groups?%24skip=0&%24top=1000": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "value": [
                        {
                            "id": "64ED5CAD-7C10-4684-8180-826122881108",
                            "isReadOnly": True,
                            "name": "demo-workspace",
                            "type": "Workspace",
                        },
                    ],
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups?%24skip=1000&%24top=1000": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "value": [],
                },
            },
            "https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/4674efd1-603c-4129-8d82-03cf2be05aff": {
                "method": "GET",
                "status_code": 200,
                "json": {
                    "workspaces": [
                        {
                            "id": "64ED5CAD-7C10-4684-8180-826122881108",
                            "name": "demo-workspace",
                            "type": "Workspace",
                            "state": "Active",
                            "datasets": [
                                {
                                    "id": "91580e0e-1680-4b1c-bbf9-4f6764d7a5ff",
                                    "tables": [
                                        {
                                            "name": "employee_ctc",
                                            "source": [
                                                {
                                                    "expression": "dummy",
                                                }
                                            ],
                                        }
                                    ],
                                },
                            ],
                        },
                    ]
                },
            },
            "https://api.powerbi.com/v1.0/myorg/groups/64ED5CAD-7C10-4684-8180-826122881108/dashboards": {
                "method": "GET",
                "status_code": 200,
                "json": {"value": []},
            },
        },
    )

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_source_config(),
                    "extract_independent_datasets": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_independent_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_independent_datasets.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_independent_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_cll_extraction(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
    )

    default_conf: dict = default_source_config()

    del default_conf[
        "dataset_type_mapping"
    ]  # delete this key so that connector set it to default (all dataplatform)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **default_conf,
                    "extract_lineage": True,
                    "extract_column_level_lineage": True,
                    "enable_advance_lineage_sql_construct": True,
                    "native_query_parsing": True,
                    "extract_independent_datasets": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_cll_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_cll.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_cll_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
def test_cll_extraction_flags(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
    )

    default_conf: dict = default_source_config()
    pattern: str = re.escape(
        "Enable all these flags in recipe: ['native_query_parsing', 'enable_advance_lineage_sql_construct', 'extract_lineage']"
    )

    with pytest.raises(Exception, match=pattern):
        Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "powerbi",
                    "config": {
                        **default_conf,
                        "extract_column_level_lineage": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/powerbi_cll_mces.json",
                    },
                },
            }
        )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_cross_workspace_reference_info_message(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data=read_mock_data(
            path=pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/cross_workspace_mock_response.json"
        ),
    )

    config = default_source_config()

    del config["workspace_id"]

    config["workspace_id_pattern"] = {
        "allow": [
            "A8D655A6-F521-477E-8C22-255018583BF4",
            "C5DA6EA8-625E-4AB1-90B6-CAEA0BF9F492",
        ]
    }

    config["include_workspace_name_in_dataset_urn"] = True

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **config,
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

    assert isinstance(pipeline.source, PowerBiDashboardSource)  # to silent the lint

    info_entries: dict = pipeline.source.reporter._structured_logs._entries.get(
        StructuredLogLevel.INFO, {}
    )  # type :ignore

    is_entry_present: bool = False
    # Printing INFO entries
    for entry in info_entries.values():
        if entry.title == "Missing Lineage For Tile":
            is_entry_present = True
            break

    assert is_entry_present, (
        'Info message "Missing Lineage For Tile" should be present in reporter'
    )

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    golden_file = "golden_test_cross_workspace_dataset.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


def common_app_ingest(
    pytestconfig: pytest.Config,
    requests_mock: Any,
    output_mcp_path: str,
    override_config: dict = {},
) -> Pipeline:
    register_mock_api(
        pytestconfig=pytestconfig,
        request_mock=requests_mock,
        override_data=read_mock_data(
            path=pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/workspace_with_app_mock_response.json"
        ),
    )

    config = default_source_config()

    del config["workspace_id"]

    config["workspace_id_pattern"] = {
        "allow": [
            "8F756DE6-26AD-45FF-A201-44276FF1F561",
        ]
    }

    config.update(override_config)

    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    **config,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": output_mcp_path,
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    return pipeline


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_app_ingest(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    common_app_ingest(
        pytestconfig=pytestconfig,
        requests_mock=requests_mock,
        output_mcp_path=f"{tmp_path}/powerbi_mces.json",
        override_config={
            "extract_app": True,
        },
    )

    golden_file = "golden_test_app_ingest.json"

    test_resources_dir = pytestconfig.rootpath / "tests/integration/powerbi"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_powerbi_app_ingest_info_message(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    pipeline = common_app_ingest(
        pytestconfig=pytestconfig,
        requests_mock=requests_mock,
        output_mcp_path=f"{tmp_path}/powerbi_mces.json",
    )

    assert isinstance(pipeline.source, PowerBiDashboardSource)  # to silent the lint

    info_entries: dict = pipeline.source.reporter._structured_logs._entries.get(
        StructuredLogLevel.INFO, {}
    )  # type :ignore

    is_entry_present: bool = False
    # Printing INFO entries
    for entry in info_entries.values():
        if entry.title == "App Ingestion Is Disabled":
            is_entry_present = True
            break

    assert is_entry_present, (
        "The extract_app flag should be set to false by default. We need to keep this flag as false until all GMS instances are updated to the latest release."
    )
