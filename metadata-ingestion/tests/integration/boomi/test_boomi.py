import contextlib
import copy
import json
import re
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2026-11-02 20:00:00"
ACCOUNT_ID = "acme-AB1CD2"
BASE_URL = f"https://api.boomi.com/api/rest/v1/{ACCOUNT_ID}"
GOLDEN_DIR = Path(__file__).parent / "golden"
# Responses captured from a Boomi trial account and anonymised: the account id
# and user emails are replaced and encrypted passwords zeroed.
FIXTURES_DIR = Path(__file__).parent / "fixtures"

ORDERS_EXTRACT = "e1b56fb6-0dae-4c33-9a9d-e5af9b8aedbc"
CATALOG_SYNC = "6363d3f8-93fa-4c4d-a399-45bde722ee63"

# The ids below are not in the captures. Their records are copied from
# captured ones, so the shape stays real, and cover cases the trial account
# does not have.
INVOICE_EXPORT = "5b1e0c2a-6f0d-4c1e-9a43-000000000001"
SHIPMENT_NOTIFY = "5b1e0c2a-6f0d-4c1e-9a43-000000000002"
SANDBOX_PROBE = "5b1e0c2a-6f0d-4c1e-9a43-000000000003"
ORDERS_EXTRACT_TEST = "5b1e0c2a-6f0d-4c1e-9a43-000000000004"
SANDBOX_FOLDER = "Rjo4OTQzNDk5"
SQLSERVER_CONNECTION = "5b1e0c2a-6f0d-4c1e-9a43-000000000011"
INVOICES_GET = "5b1e0c2a-6f0d-4c1e-9a43-000000000012"
INVOICES_PROFILE = "5b1e0c2a-6f0d-4c1e-9a43-000000000013"
SNOWFLAKE_CONNECTION = "5b1e0c2a-6f0d-4c1e-9a43-000000000014"
INVOICE_FACTS_UPSERT = "5b1e0c2a-6f0d-4c1e-9a43-000000000015"
NETSUITE_CONNECTION = "5b1e0c2a-6f0d-4c1e-9a43-000000000016"
NETSUITE_UPSERT = "5b1e0c2a-6f0d-4c1e-9a43-000000000017"
SNOWFLAKE_JDBC_CONNECTION = "5b1e0c2a-6f0d-4c1e-9a43-000000000018"
ORDERS_GET = "5b1e0c2a-6f0d-4c1e-9a43-000000000019"
ORDER_LINES_QUERY = "5b1e0c2a-6f0d-4c1e-9a43-000000000020"
SNOWFLAKE_SUBTYPE = "officialboomi-X3979C-snowfl-prod"
DB_V2_SUBTYPE = "officialboomi-X3979C-dbv2da-prod"
# Captured components that synthetic ones are derived from.
HTTP_CONNECTION = "aff32f06-3c23-4012-8d05-e488bd3490e2"
GET_POSTS_OPERATION = "d311ffe2-9cb5-42f4-951f-83d07a606d92"
POSTGRES_CONNECTION = "1a5d3fed-78e7-48d6-83e3-886669a42439"
RNA_DATABASES_GET = "b8cb6962-9950-4dfa-9bcb-d5670375ca82"
RNA_DATABASES_PROFILE = "9b842e54-05ff-435a-a87d-209d92a690cb"
ENSEMBL_CONNECTION = "64f429a7-f94a-420d-a5d0-03385e9c3fc8"
COORD_SYSTEM_GET = "60a37dba-4e38-42b2-b451-2fe2dab0109c"
ENSEMBL_URL_FIELD = (
    '<field id="url" type="string" '
    'value="jdbc:mysql://ensembldb.ensembl.org:3306/homo_sapiens_core_116_38"/>'
)
COORD_SYSTEM_QUERY_FIELD = (
    '<field id="query" type="string" value="select * from coord_system"/>'
)
SNOWFLAKE_JDBC_URL = (
    "jdbc:snowflake://acct.snowflakecomputing.com/"
    "?db=ANALYTICS&amp;schema=PUBLIC&amp;warehouse=WH"
)
DEV_ENVIRONMENT = "9c0a7e51-2d4b-4f3e-8a61-000000000001"

START_STOP_SHAPES = (
    '<shape image="start" name="shape1" shapetype="start" userlabel="" x="48.0" y="48.0">'
    "<configuration><noaction/></configuration><dragpoints>"
    '<dragpoint name="shape1.dragpoint1" toShape="shape2" x="257.0" y="56.0"/>'
    "</dragpoints></shape>"
    '<shape image="stop_icon" name="shape2" shapetype="stop" userlabel="" x="273.0" y="48.0">'
    '<configuration><stop continue="true"/></configuration><dragpoints/></shape>'
)

_COMPONENT_ID_RE = re.compile(f'componentId="{ORDERS_EXTRACT}"')
_NAME_RE = re.compile(re.escape(' name="DH Test - Orders Extract"'))
_DESCRIPTION_RE = re.compile("<bns:description>.*?</bns:description>", re.S)
_SHAPES_RE = re.compile("<shapes>.*</shapes>", re.S)


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


def _records(name: str) -> List[Dict[str, object]]:
    return json.loads(_fixture(name))["result"]


def _copy(record: Dict[str, object], **overrides: object) -> Dict[str, object]:
    copied = copy.deepcopy(record)
    copied.update(overrides)
    return copied


def _splice(xml: str, pattern: "re.Pattern[str]", replacement: str) -> str:
    # Fail loudly if a fixture refresh moves a target, rather than letting the
    # golden file silently pick up the base process's values.
    spliced, count = pattern.subn(lambda _: replacement, xml)
    assert count == 1, f"{pattern.pattern!r} matched {count} times in the fixture"
    return spliced


def _process_xml(
    component_id: str, name: str, description: str, shapes: str = START_STOP_SHAPES
) -> str:
    xml = _fixture(f"components/{ORDERS_EXTRACT}.xml")
    xml = _splice(xml, _COMPONENT_ID_RE, f'componentId="{component_id}"')
    xml = _splice(xml, _NAME_RE, f' name="{name}"')
    xml = _splice(
        xml, _DESCRIPTION_RE, f"<bns:description>{description}</bns:description>"
    )
    return _splice(xml, _SHAPES_RE, f"<shapes>{shapes}</shapes>")


FOLDERS = _records("folder_query.json")
FOLDERS.extend(
    [
        _copy(
            FOLDERS[-1],
            id=SANDBOX_FOLDER,
            name="Sandbox",
            fullPath="Test Account/DataHub Test/Sandbox",
        ),
        _copy(
            FOLDERS[-1],
            id="Rjo4OTQzNDk4",
            name="Archive",
            fullPath="Test Account/DataHub Test/Archive",
            deleted=True,
        ),
    ]
)

ENVIRONMENTS = _records("environment_query.json")
ENVIRONMENTS.append(_copy(ENVIRONMENTS[0], id=DEV_ENVIRONMENT, name="DH Test Dev Env"))

DEPLOYMENTS = _records("deployed_package_query.json")
DEPLOYMENTS.extend(
    [
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000001",
            componentId=INVOICE_EXPORT,
            packageVersion="2.0",
        ),
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000002",
            componentId=SANDBOX_PROBE,
        ),
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000003",
            componentId=ORDERS_EXTRACT_TEST,
        ),
        # Only deployed to an environment denied by environment_pattern.
        _copy(
            DEPLOYMENTS[-1],
            deploymentId="0d1e2f30-0000-4000-8000-000000000004",
            componentId=SHIPMENT_NOTIFY,
            environmentId=DEV_ENVIRONMENT,
        ),
    ]
)

PROCESSES_PAGE_1 = _records("component_metadata_query.json")
_PROCESS_TEMPLATE = PROCESSES_PAGE_1[0]
PROCESSES_PAGE_2 = [
    _copy(
        _PROCESS_TEMPLATE,
        componentId=INVOICE_EXPORT,
        name="DH Test - Invoice Export",
        version=["Long", 3],
        modifiedBy="dev@example.com",
        modifiedDate="2026-11-02T19:30:00Z",
    ),
    # A stale branch copy of Orders Extract; the newer copy on page 1 wins.
    _copy(
        _PROCESS_TEMPLATE,
        version=4,
        modifiedBy="dev@example.com",
        modifiedDate="2026-10-20T10:00:00Z",
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=SHIPMENT_NOTIFY,
        name="DH Test - Shipment Notify",
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=SANDBOX_PROBE,
        name="DH Test - Sandbox Probe",
        folderName="Sandbox",
        folderId=SANDBOX_FOLDER,
    ),
    _copy(
        _PROCESS_TEMPLATE,
        componentId=ORDERS_EXTRACT_TEST,
        name="DH Test - Orders Extract Test",
    ),
]

EXECUTIONS_PAGE_1 = _records("execution_record_query.json")
_EXECUTION_TEMPLATE = EXECUTIONS_PAGE_1[-1]


def _execution(
    execution_id: str,
    process_id: str,
    time: str,
    status: str,
    duration: Optional[int] = 1000,
    execution_type: str = "exec_sched",
    top_level_execution_id: Optional[str] = None,
) -> Dict[str, object]:
    record = _copy(
        _EXECUTION_TEMPLATE,
        executionId=execution_id,
        processId=process_id,
        executionTime=time,
        recordedDate=time,
        status=status,
        executionType=execution_type,
        nodeId=f"atom02[{execution_id}]",
    )
    for key in ("message", "processName", "topLevelExecutionId"):
        record.pop(key, None)
    if duration is None:
        record.pop("executionDuration")
    else:
        record["executionDuration"] = ["Long", duration]
    if top_level_execution_id:
        record["topLevelExecutionId"] = top_level_execution_id
    return record


EXECUTIONS_PAGE_2 = [
    _execution(
        "execution-ie-1", INVOICE_EXPORT, "2026-11-02T08:00:00Z", "COMPLETE_WARN"
    ),
    _execution("execution-ie-2", INVOICE_EXPORT, "2026-11-02T09:00:00Z", "ABORTED"),
    _execution(
        "execution-ie-3", INVOICE_EXPORT, "2026-11-02T10:00:00Z", "discarded", 0
    ),
    _execution(
        "execution-ie-4", INVOICE_EXPORT, "2026-11-02T19:55:00Z", "INPROCESS", None
    ),
    _execution(
        "execution-sn-1", SHIPMENT_NOTIFY, "2026-11-02T19:58:00Z", "STARTED", None
    ),
    _execution(
        "execution-sn-2", SHIPMENT_NOTIFY, "2026-11-02T19:59:00Z", "ENQUEUED", None
    ),
    _execution(
        "execution-sn-3",
        SHIPMENT_NOTIFY,
        "2026-11-02T19:00:00Z",
        "ABORTED_RECOVERING",
        None,
    ),
    _execution("execution-sn-4", SHIPMENT_NOTIFY, "2026-11-01T07:00:00Z", "PAUSED"),
    # Called by a run that is older than the window, so it has no caller to
    # link to and keeps its own trigger.
    _execution(
        "execution-cs-sub",
        CATALOG_SYNC,
        "2026-11-01T06:00:00Z",
        "COMPLETE",
        execution_type="sub_process",
        top_level_execution_id="execution-outside-window",
    ),
    # A process that is filtered out.
    _execution("execution-sp-1", SANDBOX_PROBE, "2026-11-02T08:00:00Z", "COMPLETE"),
]


def _derive_component(
    source_id: str, component_id: str, name: str, replacements: Dict[str, str]
) -> str:
    xml = _fixture(f"components/{source_id}.xml")
    xml = xml.replace(f'componentId="{source_id}"', f'componentId="{component_id}"')
    xml = re.sub(' name="[^"]*" type=', f' name="{name}" type=', xml, count=1)
    for old, new in replacements.items():
        assert old in xml, old
        xml = xml.replace(old, new)
    return xml


def _connector_shape(
    name: str,
    label: str,
    action_type: str,
    connection_id: str,
    connector_type: str,
    operation_id: str = "",
) -> str:
    return (
        f'<shape image="connectoraction_icon" name="{name}" shapetype="connectoraction" userlabel="{label}" x="273.0" y="48.0">'
        f'<configuration><connectoraction actionType="{action_type}" allowDynamicCredentials="NONE" '
        f'connectionId="{connection_id}" connectorType="{connector_type}" hideSettings="false" '
        f'operationId="{operation_id}"><parameters/><dynamicProperties/></connectoraction></configuration>'
        "<dragpoints/></shape>"
    )


INVOICE_EXPORT_SHAPES = START_STOP_SHAPES + "".join(
    [
        _connector_shape(
            "shape3", "Receive invoices", "Listen", HTTP_CONNECTION, "http"
        ),
        _connector_shape(
            "shape4",
            "Get invoices",
            "Get",
            SQLSERVER_CONNECTION,
            "database",
            INVOICES_GET,
        ),
        _connector_shape(
            "shape5",
            "Upsert invoice facts",
            "UPSERT",
            SNOWFLAKE_CONNECTION,
            SNOWFLAKE_SUBTYPE,
            INVOICE_FACTS_UPSERT,
        ),
        _connector_shape(
            "shape8",
            "Get orders",
            "Get",
            SNOWFLAKE_JDBC_CONNECTION,
            SNOWFLAKE_SUBTYPE,
            ORDERS_GET,
        ),
        _connector_shape(
            "shape9",
            "Query order lines",
            "Get",
            SNOWFLAKE_JDBC_CONNECTION,
            SNOWFLAKE_SUBTYPE,
            ORDER_LINES_QUERY,
        ),
        _connector_shape(
            "shape6",
            "Upsert customers",
            "UPSERT",
            NETSUITE_CONNECTION,
            "netsuitesdk",
            NETSUITE_UPSERT,
        ),
        '<shape image="processcall_icon" name="shape7" shapetype="processcall" userlabel="Call test copy" x="723.0" y="48.0">'
        f'<configuration><processcall abort="false" processId="{ORDERS_EXTRACT_TEST}" wait="true">'
        "<parameters/><returnpaths/></processcall></configuration><dragpoints/></shape>",
    ]
)

# Connectors the trial account cannot run, derived from captured components.
SYNTHETIC_COMPONENT_XML: Dict[str, str] = {
    SQLSERVER_CONNECTION: _derive_component(
        POSTGRES_CONNECTION,
        SQLSERVER_CONNECTION,
        "DH Test - ERP SQL Server",
        {
            'className="org.postgresql.Driver"': 'className=""',
            'dbname="pfmegrnargs"': 'dbname="my_db"',
            'driverId="custom"': 'driverId="sqlserver"',
            'host="hh-pgsql-public.ebi.ac.uk"': 'host="db.example.com"',
            'port="5432"': 'port="1433"',
            ' urlFormat="jdbc:postgresql://{0}:{1}/{2}{3}"': "",
        },
    ),
    INVOICES_GET: _derive_component(
        RNA_DATABASES_GET,
        INVOICES_GET,
        "DH Test - Invoices Get",
        {RNA_DATABASES_PROFILE: INVOICES_PROFILE},
    ),
    # A profile that names its table but has no SQL.
    INVOICES_PROFILE: _derive_component(
        RNA_DATABASES_PROFILE,
        INVOICES_PROFILE,
        "DH Test - Invoices Profile",
        {
            'tableName=""': 'tableName="dbo.Invoices"',
            "<sql>SELECT id, descr, display_name FROM rnc_database ORDER BY id</sql>": "",
        },
    ),
    SNOWFLAKE_CONNECTION: _derive_component(
        ENSEMBL_CONNECTION,
        SNOWFLAKE_CONNECTION,
        "DH Test - Snowflake",
        {
            f'subType="{DB_V2_SUBTYPE}"': f'subType="{SNOWFLAKE_SUBTYPE}"',
            ENSEMBL_URL_FIELD: (
                f'<field id="url" type="string" value="{SNOWFLAKE_JDBC_URL}"/>'
            ),
        },
    ),
    # Unmapped: the platform, database and schema come from the JDBC URL,
    # which sits in a field the source plugin has no id for.
    SNOWFLAKE_JDBC_CONNECTION: _derive_component(
        ENSEMBL_CONNECTION,
        SNOWFLAKE_JDBC_CONNECTION,
        "DH Test - Snowflake JDBC",
        {
            f'subType="{DB_V2_SUBTYPE}"': f'subType="{SNOWFLAKE_SUBTYPE}"',
            ENSEMBL_URL_FIELD: (
                '<field id="connectionString" type="string" '
                f'value="{SNOWFLAKE_JDBC_URL}"/>'
            ),
            'value="com.mysql.cj.jdbc.Driver"': (
                'value="net.snowflake.client.jdbc.SnowflakeDriver"'
            ),
            'value="homo_sapiens_core_116_38"': 'value=""',
        },
    ),
    ORDERS_GET: _derive_component(
        COORD_SYSTEM_GET,
        ORDERS_GET,
        "DH Test - Orders Get",
        {
            f'subType="{DB_V2_SUBTYPE}"': f'subType="{SNOWFLAKE_SUBTYPE}"',
            'objectTypeId="coord_system"': 'objectTypeId="ORDERS"',
            COORD_SYSTEM_QUERY_FIELD: "",
        },
    ),
    ORDER_LINES_QUERY: _derive_component(
        COORD_SYSTEM_GET,
        ORDER_LINES_QUERY,
        "DH Test - Order Lines Query",
        {
            f'subType="{DB_V2_SUBTYPE}"': f'subType="{SNOWFLAKE_SUBTYPE}"',
            'objectTypeId="coord_system"': 'objectTypeId="ORDER_LINES"',
            COORD_SYSTEM_QUERY_FIELD: (
                '<field id="sqlStatement" type="string" value="SELECT l.order_id, '
                'l.amount FROM order_lines l JOIN orders o ON o.id = l.order_id"/>'
            ),
        },
    ),
    INVOICE_FACTS_UPSERT: _derive_component(
        COORD_SYSTEM_GET,
        INVOICE_FACTS_UPSERT,
        "DH Test - Invoice Facts Upsert",
        {
            f'subType="{DB_V2_SUBTYPE}"': f'subType="{SNOWFLAKE_SUBTYPE}"',
            'customOperationType="GET" objectTypeId="coord_system" operationType="EXECUTE"': (
                'customOperationType="UPSERT" objectTypeId="INVOICE_FACTS" operationType="UPSERT"'
            ),
            COORD_SYSTEM_QUERY_FIELD: "",
        },
    ),
    # No field holds a JDBC URL, so the connection stays unresolved.
    NETSUITE_CONNECTION: _derive_component(
        ENSEMBL_CONNECTION,
        NETSUITE_CONNECTION,
        "DH Test - NetSuite",
        {
            f'subType="{DB_V2_SUBTYPE}"': 'subType="netsuitesdk"',
            ENSEMBL_URL_FIELD: (
                '<field id="url" type="string" '
                'value="https://acct.suitetalk.api.example.com"/>'
            ),
        },
    ),
    NETSUITE_UPSERT: _derive_component(
        COORD_SYSTEM_GET,
        NETSUITE_UPSERT,
        "DH Test - NetSuite Customer Upsert",
        {f'subType="{DB_V2_SUBTYPE}"': 'subType="netsuitesdk"'},
    ),
}

COMPONENT_XML: Dict[str, str] = {
    path.stem: path.read_text()
    for path in sorted((FIXTURES_DIR / "components").glob("*.xml"))
}
COMPONENT_XML.update(SYNTHETIC_COMPONENT_XML)
COMPONENT_XML[INVOICE_EXPORT] = _process_xml(
    INVOICE_EXPORT,
    "DH Test - Invoice Export",
    "Exports posted invoices to the finance warehouse.",
    shapes=INVOICE_EXPORT_SHAPES,
)
COMPONENT_XML[SHIPMENT_NOTIFY] = _process_xml(
    SHIPMENT_NOTIFY, "DH Test - Shipment Notify", ""
)


@pytest.fixture(autouse=True)
def _no_rate_limit() -> Iterator[None]:
    # The clock is frozen, so the real limiter would sleep a full period for
    # every request over the cap.
    with patch(
        "datahub.ingestion.source.boomi.client.RateLimiter",
        lambda max_calls: contextlib.nullcontext(),
    ):
        yield


def _page(
    records: List[Dict[str, object]], token: Optional[str] = None
) -> Dict[str, object]:
    page: Dict[str, object] = {
        "@type": "QueryResult",
        "result": records,
        "numberOfResults": len(records),
    }
    if token:
        page["queryToken"] = token
    return page


def _register_mocks(
    mock: requests_mock.Mocker, deployments: List[Dict[str, object]] = DEPLOYMENTS
) -> None:
    mock.post(f"{BASE_URL}/Folder/query", json=_page(FOLDERS))
    mock.post(f"{BASE_URL}/Environment/query", json=_page(ENVIRONMENTS))
    mock.post(f"{BASE_URL}/DeployedPackage/query", json=_page(deployments))
    mock.post(
        f"{BASE_URL}/ComponentMetadata/query",
        json=_page(PROCESSES_PAGE_1, token="page-2-token"),
    )
    mock.post(
        f"{BASE_URL}/ComponentMetadata/queryMore",
        json=_page(PROCESSES_PAGE_2),
    )
    mock.post(
        f"{BASE_URL}/ExecutionRecord/query",
        json=_page(EXECUTIONS_PAGE_1, token="executions-page-2-token"),
    )
    mock.post(f"{BASE_URL}/ExecutionRecord/queryMore", json=_page(EXECUTIONS_PAGE_2))
    for component_id, xml in COMPONENT_XML.items():
        mock.get(f"{BASE_URL}/Component/{component_id}", text=xml)


def _recipe(output_path: Path) -> Dict[str, object]:
    return {
        "run_id": "boomi-test",
        "source": {
            "type": "boomi",
            "config": {
                "account_id": ACCOUNT_ID,
                "username": "svc@example.com",
                "api_token": "dummy-token",
                "branch_name": "main",
                "folder_pattern": {"deny": [".*/Sandbox$"]},
                "process_pattern": {"deny": [".* Test$"]},
                "environment_pattern": {"deny": [".* Dev Env$"]},
                "include_undeployed_processes": True,
                "max_requests_per_second": 10,
                "max_executions_per_process": 4,
                "connection_to_platform": {
                    SNOWFLAKE_CONNECTION: {
                        "platform": "snowflake",
                        "platform_instance": "snow_prod",
                        "database": "ANALYTICS",
                        "default_schema": "FINANCE",
                    }
                },
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_ingestion(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    output_path = tmp_path / "boomi_mces.json"
    with requests_mock.Mocker() as mock:
        _register_mocks(mock)
        pipeline = Pipeline.create(_recipe(output_path))
        pipeline.run()
        pipeline.raise_from_status()

        query_more = [r for r in mock.request_history if r.path.endswith("/querymore")]
        assert [r.text for r in query_more] == [
            "page-2-token",
            "executions-page-2-token",
        ]
        assert query_more[0].headers["Content-Type"] == "text/plain"
        component_gets = [
            r.path.rsplit("/", 1)[-1]
            for r in mock.request_history
            if "/component/" in r.path
        ]
        # Orders Extract and Salesforce Accounts share the Postgres connection.
        assert component_gets.count(POSTGRES_CONNECTION) == 1
        # Get orders and Query order lines share the Snowflake JDBC connection.
        assert component_gets.count(SNOWFLAKE_JDBC_CONNECTION) == 1
        # Operations of skipped connectors are never read.
        assert GET_POSTS_OPERATION not in component_gets
        execution_query = next(
            r for r in mock.request_history if r.path.endswith("/executionrecord/query")
        )
        assert execution_query.json()["QueryFilter"]["expression"]["argument"] == [
            "2026-10-26T20:00:00Z"
        ]

    report = pipeline.source.get_report()
    assert isinstance(report, BoomiSourceReport)
    assert report.folders_emitted == 5
    assert report.processes_emitted == 7
    assert report.processes_duplicate_branch_copies == 1
    assert list(report.processes_undeployed_skipped) == []
    assert list(report.environments_filtered) == ["DH Test Dev Env"]
    assert sorted(report.processes_filtered) == [
        "DH Test - Orders Extract Test",
        "DH Test - Sandbox Probe",
    ]
    # Orders Extract has five runs in the captures; the oldest is over the cap.
    assert report.executions_scanned == 20
    assert report.executions_emitted == 18
    assert report.executions_over_cap == 1
    assert report.executions_process_not_emitted == 1
    assert report.executions_unknown_status == 1
    assert report.lineage_edges_resolved == 11
    assert dict(report.lineage_edges_unresolved) == {"unsupported_connector": 1}
    assert dict(report.lineage_skipped_subtypes) == {
        "http": 1,
        "disk-sdk": 3,
        "officialboomi-X3979C-sftpvx-prod": 1,
    }
    assert report.lineage_listen_shapes_skipped == 1
    assert report.process_calls_resolved == 1
    assert report.process_calls_child_not_emitted == 1
    assert sorted(w.title or "" for w in report.warnings) == [
        "Unknown Boomi execution status",
        "Unsupported Boomi connector for lineage",
    ]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_DIR / "boomi_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_stateful_removes_undeployed_process(
    tmp_path: Path, mock_datahub_graph: MagicMock
) -> None:
    recipe = _recipe(tmp_path / "run1.json")
    source_config = recipe["source"]["config"]  # type: ignore[index]
    source_config["include_undeployed_processes"] = False
    source_config["stateful_ingestion"] = {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": "http://localhost:8080"}},
        },
    }
    recipe["pipeline_name"] = "boomi_stateful_test"

    with (
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
        requests_mock.Mocker() as mock,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        _register_mocks(mock)
        pipeline_run1 = run_and_get_pipeline(recipe)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        # Catalog Sync loses its only deployment between runs.
        _register_mocks(
            mock,
            deployments=[d for d in DEPLOYMENTS if d["componentId"] != CATALOG_SYNC],
        )
        recipe["sink"] = {
            "type": "file",
            "config": {"filename": str(tmp_path / "run2.json")},
        }
        pipeline_run2 = run_and_get_pipeline(recipe)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    report = pipeline_run1.source.get_report()
    assert isinstance(report, BoomiSourceReport)
    # The trial account's two never-deployed processes, plus one deployed only
    # to a denied environment.
    assert sorted(report.processes_undeployed_skipped) == [
        "DH Test - SFTP Pickup",
        "DH Test - Salesforce Accounts",
        "DH Test - Shipment Notify",
    ]
    assert checkpoint1 and checkpoint1.state
    assert checkpoint2 and checkpoint2.state
    validate_all_providers_have_committed_successfully(pipeline_run1, 1)
    validate_all_providers_have_committed_successfully(pipeline_run2, 1)
    removed = set(
        checkpoint1.state.get_urns_not_in(
            type="dataFlow", other_checkpoint_state=checkpoint2.state
        )
    ) | set(
        checkpoint1.state.get_urns_not_in(
            type="dataJob", other_checkpoint_state=checkpoint2.state
        )
    )
    assert removed == {
        f"urn:li:dataFlow:(boomi,{CATALOG_SYNC},PROD)",
        f"urn:li:dataJob:(urn:li:dataFlow:(boomi,{CATALOG_SYNC},PROD),process)",
    }
    soft_deleted = {
        mcp["entityUrn"]
        for mcp in json.loads((tmp_path / "run2.json").read_text())
        if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
    }
    assert soft_deleted == removed
