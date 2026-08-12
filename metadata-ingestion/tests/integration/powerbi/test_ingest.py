import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)
from datahub.testing import mce_helpers
from tests.test_helpers.graph_helpers import MockDataHubGraph

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


@time_machine.travel(FROZEN_TIME, tick=False)
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
                    "ownership": {"create_corp_user": True},
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


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_mysql_odbc_datasource_ingest(
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
            / "tests/integration/powerbi/mock_data/mysql_odbc_datasource_mock_response.json"
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
                    "ownership": {"create_corp_user": True},
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
    golden_file = "golden_test_mysql_odbc_datasource.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mysql_odbc_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_mysql_odbc_query_ingest(
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
            / "tests/integration/powerbi/mock_data/mysql_odbc_query_mock_response.json"
        ),
    )
    pipeline = Pipeline.create(
        {
            "run_id": "powerbi-test",
            "source": {
                "type": "powerbi",
                "config": {
                    "client_id": "foo",
                    "client_secret": "bar",
                    "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
                    "workspace_id_pattern": {
                        "allow": ["64ed5cad-7c10-4684-8180-826122881108"]
                    },
                    "dsn_to_platform_name": {"testdb01": "mysql"},
                    "dsn_to_database_schema": {"testdb01": "employees"},
                    "extract_lineage": True,
                    "extract_column_level_lineage": False,
                    "extract_reports": False,
                    "extract_independent_datasets": True,
                    "convert_urns_to_lowercase": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_mysql_odbc_query_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_mysql_odbc_query.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_mysql_odbc_query_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_soft_reference_mode_no_user_entities(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    """
    Test create_corp_user=False (default mode) - soft references only.

    When create_corp_user=False:
    - Ownership URNs should still be emitted in dashboard/report ownership aspects
    - NO corpuser entities (corpUserKey, corpUserInfo) should be created
    - This is the recommended pattern for LDAP/Okta user management
    """
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
                    # Explicitly set to False (or omit - False is default)
                    "ownership": {"create_corp_user": False},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_soft_ref_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "golden_test_soft_reference_mode.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_soft_ref_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_empty_owner_criteria_includes_all_users(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    tmp_path: str,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    """
    Test owner_criteria=[] (empty list) behaves same as None.

    This validates the fix for the bug where empty owner_criteria list
    would filter out ALL users instead of including all users.

    When owner_criteria=[]:
    - Should behave same as owner_criteria=None (no filtering)
    - All users with principalType='User' should be included as owners
    - Uses same golden file as test_mysql_ingest (users are created)
    """
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
                    "ownership": {
                        "create_corp_user": True,
                        # Empty list should behave same as None (include all users)
                        "owner_criteria": [],
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/powerbi_empty_criteria_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Uses same golden file as test_mysql_ingest since all users should be included
    golden_file = "golden_test_mysql.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/powerbi_empty_criteria_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


MYSQL_EMPLOYEES_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:mysql,employees.employees,PROD)"
)


class _OfflineGraph(MockDataHubGraph):
    def get_config(self) -> Dict[str, Any]:
        # PipelineContext asks the server about URN casing while constructing;
        # there is no server behind the mock.
        return {}


def _mysql_employees_schema(column_names: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="employees",
        platform="urn:li:dataPlatform:mysql",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=column_name,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
            )
            for column_name in column_names
        ],
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@mock.patch("msal.ConfidentialClientApplication", side_effect=mock_msal_cca)
@pytest.mark.integration
def test_upstream_column_lineage_uses_the_upstream_casing(
    mock_msal: MagicMock,
    pytestconfig: pytest.Config,
    mock_time: datetime.datetime,
    requests_mock: Any,
) -> None:
    """Column-level lineage must point at the columns as the upstream spells them.

    The PowerBI model here reports `emp_no`; the warehouse in DataHub stores
    `Emp_No`. schemaField URNs are compared as exact strings, so emitting
    PowerBI's casing produces an edge to a field that does not exist and the
    column-level lineage silently disappears in the UI.
    """
    register_mock_api(
        request_mock=requests_mock,
        pytestconfig=pytestconfig,
        override_data=read_mock_data(
            pytestconfig.rootpath
            / "tests/integration/powerbi/mock_data/mysql_mock_response.json"
        ),
    )

    upstream_columns = [
        "Emp_No",
        "Birth_Date",
        "First_Name",
        "Last_Name",
        "Gender",
        "Hire_Date",
    ]
    graph = _OfflineGraph(
        {
            MYSQL_EMPLOYEES_URN: {
                "schemaMetadata": _mysql_employees_schema(upstream_columns)
            }
        }
    )

    source = PowerBiDashboardSource(
        config=PowerBiDashboardSourceConfig.parse_obj(
            {
                "tenant_id": "0b0c960b-fcdf-4d0f-8c45-2e03bb59ddeb",
                "client_id": "a8d655a6-f521-477e-8c22-255018583bf4",
                "client_secret": "ababa~cdcdcdcdcdcdcdcdcdcd-abcd.defghijk",
                "extract_column_level_lineage": True,
                "workspace_name_pattern": {"allow": ["^Employees$"]},
            }
        ),
        ctx=PipelineContext(run_id="powerbi-test", graph=graph),
    )

    emitted_upstream_columns = set()
    for work_unit in source.get_workunits():
        lineage = work_unit.get_aspect_of_type(UpstreamLineageClass)
        if lineage is None:
            continue
        for fine_grained in lineage.fineGrainedLineages or []:
            for upstream in fine_grained.upstreams or []:
                if MYSQL_EMPLOYEES_URN in upstream:
                    emitted_upstream_columns.add(upstream.rsplit(",", 1)[1].rstrip(")"))

    # Exact equality, so this also catches column edges being dropped rather
    # than merely mis-cased.
    assert emitted_upstream_columns == set(upstream_columns), (
        f"expected upstream casing for every column, got "
        f"{sorted(emitted_upstream_columns)}"
    )
