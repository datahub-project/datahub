from typing import TYPE_CHECKING, Dict, Iterator, List

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

    assert {"databases"} <= set(commands)
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
