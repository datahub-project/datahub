from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

import boto3
import pytest
from botocore.stub import Stubber

from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.aws.glue import GlueSourceConfig
from datahub.ingestion.source.aws.glue_probe import GlueMetadataProbe

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

_REGION = "us-east-1"
_RECIPE: Dict[str, object] = {"aws_region": _REGION}
_OTHER_ACCOUNT = "222222222222"
_PRINCIPAL_MESSAGE = (
    "User: arn:aws:sts::123456789012:assumed-role/ingest-role/someone@example.com "
    "is not authorized to perform: glue:GetDatabases"
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

    assert {"databases", "tables", "columns"} <= set(commands)
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
