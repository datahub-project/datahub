import datetime
import json
import logging
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeInternalError
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.ingestion.source.aws.glue_probe import GlueMetadataProbe
from tests.unit.glue.test_glue_source_stubs import (
    get_dataflow_graph_response_1,
    get_jobs_response,
    get_object_body_1,
    get_object_response_1,
)

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client

_REGION = "us-east-1"
_RECIPE: Dict[str, object] = {"aws_region": _REGION}
_OTHER_ACCOUNT = "222222222222"
_PRINCIPAL_ARN = (
    "arn:aws:sts::123456789012:assumed-role/ingest-role/someone@example.com"
)
_PRINCIPAL_MESSAGE = (
    f"User: {_PRINCIPAL_ARN} is not authorized to perform: glue:GetDatabases"
)


def _new_glue_client() -> "GlueClient":
    return boto3.client(
        "glue",
        region_name=_REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def glue(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stubber]:
    client = _new_glue_client()
    monkeypatch.setattr(GlueSourceConfig, "get_glue_client", lambda self: client)
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def test_probe_methods_advertises_the_glue_commands() -> None:
    commands = {spec.command: spec for spec in list_probe_methods("glue")}

    assert {"databases", "tables", "columns", "jobs", "job_nodes"} <= set(commands)
    assert commands["databases"].kind == "Database"


def test_databases_lists_every_database_ingestion_could_drop(glue: Stubber) -> None:
    glue.add_response(
        "get_databases",
        {
            "DatabaseList": [
                {"Name": "sales", "CatalogId": "123456789012"},
                {
                    "Name": "shared_link",
                    "CatalogId": "123456789012",
                    "TargetDatabase": {
                        "CatalogId": _OTHER_ACCOUNT,
                        "DatabaseName": "owner_db",
                    },
                },
                {"Name": "no_owner"},
            ]
        },
        {},
    )

    result = run_probe_method(
        "glue",
        {
            **_RECIPE,
            "database_pattern": {"deny": ["^sales$"]},
            "ignore_resource_links": True,
        },
        "databases",
        {},
    )

    assert result.kind == "Database"
    assert result.result == [
        {
            "name": "sales",
            "catalog_id": "123456789012",
            "resource_link": False,
            "target": None,
        },
        {
            "name": "shared_link",
            "catalog_id": "123456789012",
            "resource_link": True,
            "target": f"{_OTHER_ACCOUNT}/owner_db",
        },
        {"name": "no_owner", "catalog_id": "", "resource_link": False, "target": None},
    ]


def test_databases_pages_the_recipes_catalog(glue: Stubber) -> None:
    glue.add_response(
        "get_databases",
        {"DatabaseList": [{"Name": "sales", "CatalogId": _OTHER_ACCOUNT}]},
        {"CatalogId": _OTHER_ACCOUNT},
    )

    result = run_probe_method(
        "glue", {**_RECIPE, "catalog_id": _OTHER_ACCOUNT}, "databases", {}
    )

    assert isinstance(result.result, list)
    assert [r["name"] for r in result.result] == ["sales"]


def test_databases_stops_paging_at_the_limit(glue: Stubber) -> None:
    # limit=1 is fetched as 2 (the framework's one-past-the-limit), so exactly
    # two pages are requested; a third would be an unstubbed call and fail.
    glue.add_response(
        "get_databases", {"DatabaseList": [{"Name": "a"}], "NextToken": "t1"}, {}
    )
    glue.add_response(
        "get_databases",
        {"DatabaseList": [{"Name": "b"}], "NextToken": "t2"},
        {"NextToken": "t1"},
    )

    result = run_probe_method("glue", _RECIPE, "databases", {"limit": 1})

    assert isinstance(result.result, list)
    assert [r["name"] for r in result.result] == ["a"]
    assert result.truncated is True


def test_an_auth_failure_never_prints_the_principal(glue: Stubber) -> None:
    glue.add_client_error(
        "get_databases",
        service_error_code="UnrecognizedClientException",
        service_message=_PRINCIPAL_MESSAGE,
        http_status_code=400,
        response_meta={"RequestId": "req-123"},
    )

    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method("glue", _RECIPE, "databases", {})

    assert "UnrecognizedClientException" in str(info.value)
    assert "req-123" in str(info.value)
    assert "someone@example.com" not in str(info.value)


def test_building_the_provider_resolves_no_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _refuse(self: GlueSourceConfig) -> "GlueClient":
        raise AssertionError("the Glue client was built in for_config")

    monkeypatch.setattr(GlueSourceConfig, "get_glue_client", _refuse)

    with GlueMetadataProbe.for_config(GlueSourceConfig.model_validate(_RECIPE)):
        pass


def test_the_glue_client_is_closed_after_the_command(
    glue: Stubber, monkeypatch: pytest.MonkeyPatch
) -> None:
    closed: List[bool] = []
    monkeypatch.setattr(glue.client, "close", lambda: closed.append(True))
    glue.add_response("get_databases", {"DatabaseList": []}, {})

    run_probe_method("glue", _RECIPE, "databases", {})

    assert closed == [True]


_SALES: Dict[str, object] = {"Name": "sales", "CatalogId": "123456789012"}


def _databases(
    glue: Stubber,
    *databases: Dict[str, object],
    params: Optional[Dict[str, str]] = None,
) -> None:
    glue.add_response(
        "get_databases", {"DatabaseList": list(databases)}, dict(params or {})
    )


def test_tables_lists_tables_and_views_with_their_subtype(glue: Stubber) -> None:
    _databases(glue, _SALES)
    glue.add_response(
        "get_tables",
        {
            "TableList": [
                {
                    "Name": "orders",
                    "CatalogId": "123456789012",
                    "TableType": "EXTERNAL_TABLE",
                    "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "int"}]},
                    "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                    "Parameters": {"secret_hint": "never shown"},
                    "Owner": "someone@example.com",
                },
                {
                    "Name": "orders_view",
                    "TableType": "VIRTUAL_VIEW",
                    "ViewOriginalText": "SELECT 1",
                },
                {
                    "Name": "shared_orders",
                    "TargetTable": {
                        "CatalogId": _OTHER_ACCOUNT,
                        "DatabaseName": "owner_db",
                        "Name": "orders",
                    },
                },
            ]
        },
        {"DatabaseName": "sales"},
    )

    result = run_probe_method("glue", _RECIPE, "tables", {"database": "sales"})

    assert result.kind == "Table"
    assert result.parent_path == ["sales"]
    assert result.result == [
        {
            "name": "orders",
            "subtype": "Table",
            "table_type": "EXTERNAL_TABLE",
            "catalog_id": "123456789012",
            "resource_link": False,
            "column_count": 2,
            "database_resource_link": False,
            "database_catalog_id": "123456789012",
        },
        {
            "name": "orders_view",
            "subtype": "View",
            "table_type": "VIRTUAL_VIEW",
            "catalog_id": "",
            "resource_link": False,
            "column_count": 0,
            "database_resource_link": False,
            "database_catalog_id": "123456789012",
        },
        {
            "name": "shared_orders",
            "subtype": "Table",
            "table_type": None,
            "catalog_id": "",
            "resource_link": True,
            "column_count": 0,
            "database_resource_link": False,
            "database_catalog_id": "123456789012",
        },
    ]
    assert "never shown" not in str(result.to_dict())
    assert "someone@example.com" not in str(result.to_dict())
    assert "SELECT 1" not in str(result.to_dict())


def test_an_unknown_database_is_a_caller_error(glue: Stubber) -> None:
    _databases(glue, _SALES)

    with pytest.raises(ValueError, match="no database named 'nope'"):
        run_probe_method("glue", _RECIPE, "tables", {"database": "nope"})


def test_a_denied_table_listing_degrades_to_a_warning(glue: Stubber) -> None:
    _databases(glue, _SALES)
    glue.add_client_error(
        "get_tables",
        service_error_code="AccessDeniedException",
        service_message=_PRINCIPAL_MESSAGE,
        http_status_code=400,
        expected_params={"DatabaseName": "sales"},
    )

    result = run_probe_method("glue", _RECIPE, "tables", {"database": "sales"})

    assert result.result == []
    assert any("glue:GetTables on database 'sales'" in w for w in result.warnings)
    assert any("skips this database's tables" in w for w in result.warnings)
    assert "someone@example.com" not in str(result.to_dict())


def test_tables_warns_when_ingestion_never_lists_the_database(glue: Stubber) -> None:
    link: Dict[str, object] = {
        "Name": "shared_link",
        "CatalogId": "123456789012",
        "TargetDatabase": {"CatalogId": _OTHER_ACCOUNT, "DatabaseName": "owner_db"},
    }
    _databases(glue, link)
    glue.add_response(
        "get_tables", {"TableList": [{"Name": "t1"}]}, {"DatabaseName": "shared_link"}
    )

    result = run_probe_method(
        "glue",
        {**_RECIPE, "ignore_resource_links": True},
        "tables",
        {"database": "shared_link"},
    )

    assert isinstance(result.result, list)
    assert result.result[0]["database_resource_link"] is True
    assert any("ignore_resource_links is true" in w for w in result.warnings)


def test_tables_warns_when_the_database_belongs_to_another_catalog(
    glue: Stubber,
) -> None:
    _databases(
        glue,
        {"Name": "sales", "CatalogId": "333333333333"},
        params={"CatalogId": _OTHER_ACCOUNT},
    )
    glue.add_response(
        "get_tables",
        {"TableList": [{"Name": "orders"}]},
        {"DatabaseName": "sales", "CatalogId": _OTHER_ACCOUNT},
    )

    result = run_probe_method(
        "glue",
        {**_RECIPE, "catalog_id": _OTHER_ACCOUNT},
        "tables",
        {"database": "sales"},
    )

    assert isinstance(result.result, list)
    assert result.result[0]["database_catalog_id"] == "333333333333"
    assert any("not catalog_id" in w for w in result.warnings)


def test_columns_are_columns_then_partition_keys(glue: Stubber) -> None:
    glue.add_response(
        "get_tables",
        {
            "TableList": [
                {
                    "Name": "orders",
                    "StorageDescriptor": {
                        "Columns": [
                            {
                                "Name": "id",
                                "Type": "bigint",
                                "Comment": "order id",
                                "Parameters": {"x": "hidden"},
                            }
                        ]
                    },
                    "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                }
            ]
        },
        {"DatabaseName": "sales"},
    )

    result = run_probe_method(
        "glue", _RECIPE, "columns", {"database": "sales", "table": "orders"}
    )

    assert result.result == [
        {"name": "id", "type": "bigint", "comment": "order id", "partition_key": False},
        {"name": "dt", "type": "string", "comment": None, "partition_key": True},
    ]
    assert "hidden" not in str(result.to_dict())


def test_columns_of_a_resource_link_say_where_the_schema_lives(
    glue: Stubber,
) -> None:
    glue.add_response(
        "get_tables",
        {
            "TableList": [
                {
                    "Name": "shared_orders",
                    "TargetTable": {
                        "CatalogId": _OTHER_ACCOUNT,
                        "DatabaseName": "owner_db",
                        "Name": "orders",
                    },
                }
            ]
        },
        {"DatabaseName": "sales"},
    )

    result = run_probe_method(
        "glue", _RECIPE, "columns", {"database": "sales", "table": "shared_orders"}
    )

    assert result.result == []
    assert any("resource link" in w for w in result.warnings)


def test_columns_of_an_unknown_table_is_a_caller_error(glue: Stubber) -> None:
    glue.add_response(
        "get_tables", {"TableList": [{"Name": "orders"}]}, {"DatabaseName": "sales"}
    )

    with pytest.raises(ValueError, match="no table 'nope'"):
        run_probe_method(
            "glue", _RECIPE, "columns", {"database": "sales", "table": "nope"}
        )


_JOB: Dict[str, object] = {
    "Name": "nightly_load",
    "Role": "arn:aws:iam::123456789012:role/glue-job-role",
    "CreatedOn": datetime.datetime(2026, 1, 1, 0, 0, 0),
    "Command": {
        "Name": "glueetl",
        "ScriptLocation": "s3://scripts-bucket/nightly_load.py",
    },
    "DefaultArguments": {"--source-dsn": "job-arg-sentinel"},
    "GlueVersion": "4.0",
}


def test_jobs_are_listed_with_the_flow_urn_ingestion_emits(glue: Stubber) -> None:
    glue.add_response("get_jobs", {"Jobs": [_JOB]}, {})

    result = run_probe_method("glue", {**_RECIPE, "env": "DEV"}, "jobs", {})

    assert result.kind == "Job"
    assert result.result == [
        {
            "name": "nightly_load",
            "flow_urn": "urn:li:dataFlow:(glue,nightly_load,DEV)",
            "command": "glueetl",
            "script_location": "s3://scripts-bucket/nightly_load.py",
            "role": "arn:aws:iam::123456789012:role/glue-job-role",
            "glue_version": "4.0",
            "created_on": "2026-01-01 00:00:00",
            "last_modified_on": None,
        }
    ]
    assert "job-arg-sentinel" not in str(result.to_dict())


def test_jobs_say_when_ingestion_emits_none_of_them(glue: Stubber) -> None:
    glue.add_response("get_jobs", {"Jobs": [_JOB]}, {})

    result = run_probe_method(
        "glue", {**_RECIPE, "extract_transforms": False}, "jobs", {}
    )

    assert any("extract_transforms" in w for w in result.warnings)


def test_jobs_ignore_catalog_id_as_ingestion_does(glue: Stubber) -> None:
    glue.add_response("get_jobs", {"Jobs": [_JOB]}, {})

    result = run_probe_method(
        "glue", {**_RECIPE, "catalog_id": _OTHER_ACCOUNT}, "jobs", {}
    )

    assert isinstance(result.result, list)
    assert [r["name"] for r in result.result] == ["nightly_load"]
    assert any("not cross-account" in w for w in result.warnings)


def test_tables_round_trip_through_from_run(glue: Stubber) -> None:
    recipe: Dict[str, object] = {**_RECIPE, "ignore_resource_links": True}
    _databases(glue, _SALES)
    glue.add_response(
        "get_tables",
        {
            "TableList": [
                {"Name": "orders"},
                {
                    "Name": "shared_orders",
                    "TargetTable": {
                        "CatalogId": _OTHER_ACCOUNT,
                        "DatabaseName": "owner_db",
                        "Name": "orders",
                    },
                },
            ]
        },
        {"DatabaseName": "sales"},
    )

    run = run_probe_method("glue", recipe, "tables", {"database": "sales"})
    listing = listing_from_run(run.to_dict())
    verdicts = check_filters(
        source_type="glue",
        config_dict=recipe,
        kind=listing.kind or "",
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )

    assert {r.name: r.excluded_by for r in verdicts.results} == {
        "orders": None,
        "shared_orders": "ignore_resource_links",
    }


_JOB_ONE: Dict[str, object] = {"Jobs": [get_jobs_response["Jobs"][0]]}
_JOB_ONE_NAME = str(get_jobs_response["Jobs"][0]["Name"])
_SCRIPT = {
    "Bucket": "aws-glue-assets-123412341234-us-west-2",
    "Key": "scripts/job-1.py",
}


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stubber]:
    client: "S3Client" = boto3.client(
        "s3",
        region_name=_REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    monkeypatch.setattr(
        GlueSourceConfig, "get_s3_client", lambda self, verify_ssl=None: client
    )
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def _stub_one_job(glue: Stubber, s3: Stubber) -> None:
    glue.add_response("get_jobs", _JOB_ONE, {})
    s3.add_response("get_object", get_object_response_1(), _SCRIPT)
    glue.add_response(
        "get_dataflow_graph",
        get_dataflow_graph_response_1,
        {"PythonScript": get_object_body_1},
    )


def test_job_nodes_match_the_datajobs_ingestion_emits(
    glue: Stubber, s3: Stubber
) -> None:
    # Ingestion's own path, on the same stubs: _transform_extraction over a
    # source whose clients are the stubbed ones.
    _stub_one_job(glue, s3)
    config = GlueSourceConfig.model_validate(_RECIPE)
    # Both getters are patched to return the stubbed clients.
    source = GlueSource.for_probe(
        config, glue_client=config.get_glue_client(), s3_client=config.get_s3_client()
    )
    emitted = {
        wu.get_urn()
        for wu in source._transform_extraction()
        if wu.get_urn().startswith("urn:li:dataJob:")
    }

    _stub_one_job(glue, s3)
    result = run_probe_method("glue", _RECIPE, "job_nodes", {"job": _JOB_ONE_NAME})

    assert isinstance(result.result, list)
    probed = {r["urn"] for r in result.result if r["emitted_as_datajob"]}
    assert emitted and probed == emitted
    assert all("Args" not in r and "args" not in r for r in result.result)


def test_a_job_without_a_readable_script_is_one_datajob(
    glue: Stubber, s3: Stubber
) -> None:
    glue.add_response("get_jobs", _JOB_ONE, {})
    s3.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        http_status_code=404,
        expected_params=_SCRIPT,
    )

    result = run_probe_method("glue", _RECIPE, "job_nodes", {"job": _JOB_ONE_NAME})

    assert isinstance(result.result, list)
    assert len(result.result) == 1
    assert result.result[0]["datajob_name"] == _JOB_ONE_NAME
    assert result.result[0]["emitted_as_datajob"] is True
    assert result.warnings  # ingestion's "Unable to download DAG" warning, folded in


def test_an_unknown_job_is_a_caller_error(glue: Stubber) -> None:
    glue.add_response("get_jobs", _JOB_ONE, {})

    with pytest.raises(ValueError, match="no Glue job named 'nope'"):
        run_probe_method("glue", _RECIPE, "job_nodes", {"job": "nope"})


# Built at runtime so the secret scanner does not mistake the fixture for a
# committed credential; what matters is that a node arg carries one.
_NODE_SECRET = "-".join(["node", "arg", "sentinel"])
_SECRET_DAG: Dict[str, object] = {
    "DagNodes": [
        {
            "Id": "source0",
            "NodeType": "DataSource",
            "Args": [
                {"Name": "connection_type", "Value": '"custom"'},
                {
                    "Name": "connection_options",
                    "Value": json.dumps({"password": _NODE_SECRET}),
                },
            ],
            "LineNumber": 1,
        }
    ],
    "DagEdges": [],
}


@pytest.mark.parametrize("ignore_unsupported", [True, False])
def test_job_node_args_never_reach_the_error_text(
    glue: Stubber, s3: Stubber, ignore_unsupported: bool
) -> None:
    glue.add_response("get_jobs", _JOB_ONE, {})
    s3.add_response("get_object", get_object_response_1(), _SCRIPT)
    glue.add_response(
        "get_dataflow_graph", _SECRET_DAG, {"PythonScript": get_object_body_1}
    )
    recipe = {**_RECIPE, "ignore_unsupported_connectors": ignore_unsupported}

    if ignore_unsupported:
        result = run_probe_method("glue", recipe, "job_nodes", {"job": _JOB_ONE_NAME})
        assert _NODE_SECRET not in str(result.to_dict())
    else:
        with pytest.raises(ValueError) as info:
            run_probe_method("glue", recipe, "job_nodes", {"job": _JOB_ONE_NAME})
        assert _NODE_SECRET not in str(info.value)
        assert info.value.__cause__ is None
        assert "ignore_unsupported_connectors" in str(info.value)


def _stub_job_with_dag(glue: Stubber, s3: Stubber, dag: Dict[str, object]) -> None:
    glue.add_response("get_jobs", _JOB_ONE, {})
    s3.add_response("get_object", get_object_response_1(), _SCRIPT)
    glue.add_response("get_dataflow_graph", dag, {"PythonScript": get_object_body_1})


def test_an_unparseable_node_arg_is_never_quoted(
    glue: Stubber, s3: Stubber, caplog: pytest.LogCaptureFixture
) -> None:
    # Not valid YAML (a bare subscript), the shape a hand-edited script leaves
    # behind; PyYAML's error quotes a window of the input it choked on.
    malformed = (
        '{"url": "jdbc:postgresql://db.example:5432/db", "password": "'
        + _NODE_SECRET
        + '", "dbtable": args["T"]}'
    )
    dag: Dict[str, object] = {
        "DagNodes": [
            {
                "Id": "source0",
                "NodeType": "DataSource",
                "Args": [{"Name": "connection_options", "Value": malformed}],
                "LineNumber": 1,
            }
        ],
        "DagEdges": [],
    }
    _stub_job_with_dag(glue, s3, dag)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(ValueError) as info:
        run_probe_method("glue", _RECIPE, "job_nodes", {"job": _JOB_ONE_NAME})

    assert "DataSource-source0" in str(info.value)
    assert _NODE_SECRET not in str(info.value)
    assert _NODE_SECRET not in caplog.text
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__


def test_another_value_error_is_not_blamed_on_unsupported_connectors(
    glue: Stubber, s3: Stubber, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(self: GlueSource, dag: object, flow_urn: str) -> None:
        raise ValueError(f"bad path {_NODE_SECRET}")

    monkeypatch.setattr(GlueSource, "process_dataflow_graph", _raise)
    _stub_one_job(glue, s3)
    recipe = {**_RECIPE, "ignore_unsupported_connectors": False}

    with pytest.raises(ValueError) as info:
        run_probe_method("glue", recipe, "job_nodes", {"job": _JOB_ONE_NAME})

    assert "ignore_unsupported_connectors" not in str(info.value)
    assert _NODE_SECRET not in str(info.value)


def test_a_defect_in_dag_processing_names_only_the_exception_class(
    glue: Stubber, s3: Stubber, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(self: GlueSource, dag: object, flow_urn: str) -> None:
        raise KeyError(_NODE_SECRET)

    monkeypatch.setattr(GlueSource, "process_dataflow_graph", _raise)
    _stub_one_job(glue, s3)

    with pytest.raises(ProbeInternalError) as info:
        run_probe_method("glue", _RECIPE, "job_nodes", {"job": _JOB_ONE_NAME})

    assert "KeyError" in str(info.value)
    assert _NODE_SECRET not in str(info.value)


def test_a_denied_role_assumption_names_sts_not_the_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _deny(self: GlueSourceConfig) -> "GlueClient":
        raise ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": f"User: {_PRINCIPAL_ARN} is not authorized to perform: "
                    f"sts:AssumeRole",
                },
                "ResponseMetadata": {
                    "RequestId": "req-sts",
                    "HostId": "",
                    "HTTPStatusCode": 403,
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
            },
            "AssumeRole",
        )

    monkeypatch.setattr(GlueSourceConfig, "get_glue_client", _deny)

    with pytest.raises(ProbeConnectionError) as info:
        run_probe_method(
            "glue",
            {**_RECIPE, "aws_role": "arn:aws:iam::123456789012:role/ingest-role"},
            "databases",
            {},
        )

    text = str(info.value)
    assert "sts:AssumeRole" in text and "aws_role" in text
    assert "Lake Formation" not in text
    assert "someone@example.com" not in text
    assert "arn:aws" not in text


def test_a_denied_column_listing_explains_what_ingestion_does(glue: Stubber) -> None:
    glue.add_client_error(
        "get_tables",
        service_error_code="AccessDeniedException",
        service_message=_PRINCIPAL_MESSAGE,
        http_status_code=400,
        expected_params={"DatabaseName": "sales"},
    )

    result = run_probe_method(
        "glue", _RECIPE, "columns", {"database": "sales", "table": "orders"}
    )

    assert result.result == []
    assert any("skips this database's tables" in w for w in result.warnings)
