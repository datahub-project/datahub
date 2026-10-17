"""DynamoDB's probe, against moto: names as table_pattern sees them, a
failed read that is not an empty answer, and verdicts that are ingestion's."""

import json
import pathlib
from typing import Dict, Iterator, List, Set

import boto3
import pytest
import yaml
from botocore.stub import Stubber
from click.testing import CliRunner, Result
from moto import mock_aws

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.dynamodb.dynamodb import DynamoDBConfig
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness and the CLI mask output against the process-global
# secret registry; isolate it so the fixture's secret never masks a name.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_REGION = "us-east-1"
_INSTANCE = "my_instance"
_RECIPE: Dict[str, object] = {
    "aws_access_key_id": "my-access-key-id",
    "aws_secret_access_key": "my-secret-access-key",
    "aws_region": _REGION,
    "platform_instance": _INSTANCE,
}
_TABLES = ("Orders", "Products", "orders_archive")


@pytest.fixture(autouse=True)
def _aws() -> Iterator[None]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name=_REGION)
        for name in _TABLES:
            client.create_table(
                TableName=name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            client.put_item(TableName=name, Item={"pk": {"S": "1"}})
        # A table in another region: ingestion reads one region per run, so
        # the probe must not list it either.
        boto3.client("dynamodb", region_name="eu-west-1").create_table(
            TableName="Elsewhere",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield


@pytest.fixture(autouse=True)
def _no_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)


def _recipe(**overrides: object) -> Dict[str, object]:
    return {**_RECIPE, **overrides}


def _names(result_rows: object) -> List[str]:
    assert isinstance(result_rows, list)
    return [row["name"] for row in result_rows]


def test_tables_are_named_as_table_pattern_sees_them() -> None:
    result = run_probe_method(
        "dynamodb",
        _recipe(table_pattern={"deny": ["^us-east-1\\.Orders$"]}),
        "tables",
        {},
    )
    # The denied table is still listed: probe filter explains it.
    assert sorted(_names(result.result)) == [f"{_REGION}.{t}" for t in sorted(_TABLES)]
    assert isinstance(result.result, list)
    assert {row["region"] for row in result.result} == {_REGION}
    assert not result.truncated


def test_the_limit_is_reported_as_truncation() -> None:
    result = run_probe_method("dynamodb", _recipe(), "tables", {"limit": 2})
    assert len(_names(result.result)) == 2
    assert result.truncated


def _emitted_tables(index: EmittedIndex) -> Set[str]:
    # The URN name carries the platform instance as a prefix; the listing's
    # name is what table_pattern sees, without it.
    prefix = f"{_INSTANCE}."
    names = {DatasetUrn.from_string(urn).name for urn in index.urns("dataset")}
    assert all(name.startswith(prefix) for name in names)
    return {name[len(prefix) :] for name in names}


@pytest.mark.parametrize(
    "table_pattern",
    [
        pytest.param({}, id="allow-all"),
        pytest.param({"allow": ["us-east-1\\.Products"]}, id="allow-one"),
        # Matched on region.table, so a bare-name pattern keeps nothing.
        pytest.param({"allow": ["^Orders$", "us-east-1\\.O"]}, id="qualified-only"),
        pytest.param(
            {"allow": [".*"], "deny": [".*archive"], "ignoreCase": False},
            id="deny",
        ),
    ],
)
def test_verdicts_match_ingestion(
    tmp_path: pathlib.Path, table_pattern: Dict[str, object]
) -> None:
    recipe = _recipe(**({"table_pattern": table_pattern} if table_pattern else {}))
    assert_probe_parity(
        "dynamodb",
        recipe,
        pipeline_ingestion("dynamodb", tmp_path),
        [
            ParityListing(
                label="tables",
                command="tables",
                emitted=_emitted_tables,
            )
        ],
    )


def test_the_deny_reason_is_table_pattern(tmp_path: pathlib.Path) -> None:
    report = assert_probe_parity(
        "dynamodb",
        _recipe(table_pattern={"deny": [".*archive"]}),
        pipeline_ingestion("dynamodb", tmp_path),
        [ParityListing(label="tables", command="tables", emitted=_emitted_tables)],
    )
    assert report.excluded_by("tables") == {
        f"{_REGION}.orders_archive": "table_pattern"
    }


def _cli(tmp_path: pathlib.Path, *args: str, **config: object) -> Result:
    path = tmp_path / "r.yml"
    path.write_text(
        yaml.safe_dump({"source": {"type": "dynamodb", "config": _recipe(**config)}})
    )
    return CliRunner().invoke(recipe_cli, [*args, "--recipe", str(path)])


def test_probe_run_succeeds_through_the_cli(tmp_path: pathlib.Path) -> None:
    result = _cli(tmp_path, "probe", "run", "tables")
    assert result.exit_code == 0, result.output
    assert f"{_REGION}.Products" in result.output


def test_probe_filter_judges_the_qualified_name(tmp_path: pathlib.Path) -> None:
    result = _cli(
        tmp_path,
        "probe",
        "filter",
        "--kind",
        "Table",
        "--name",
        f"{_REGION}.Orders",
        "--name",
        f"{_REGION}.Products",
        table_pattern={"allow": ["us-east-1\\.Orders"]},
    )
    assert result.exit_code == 0, result.output
    verdicts = {r["name"]: r["included"] for r in json.loads(result.output)["results"]}
    assert verdicts == {f"{_REGION}.Orders": True, f"{_REGION}.Products": False}


def test_no_region_is_the_callers_to_fix(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for var in ("AWS_DEFAULT_REGION", "AWS_REGION"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-such-config"))
    config = dict(_RECIPE)
    del config["aws_region"]
    path = tmp_path / "r.yml"
    path.write_text(yaml.safe_dump({"source": {"type": "dynamodb", "config": config}}))
    result = CliRunner().invoke(
        recipe_cli, ["probe", "run", "tables", "--recipe", str(path)]
    )
    assert result.exit_code == rc.EXIT_USER, result.output
    assert "aws_region" in result.output


def test_a_denied_listing_fails_with_its_code_not_an_empty_answer(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ingestion's _list_tables logs and swallows this, which reads as "no
    # tables"; the probe must say it could not look.
    client = boto3.client("dynamodb", region_name=_REGION)
    stubber = Stubber(client)
    stubber.add_client_error(
        "list_tables",
        service_error_code="AccessDeniedException",
        service_message="User arn:aws:iam::000000000000:user/someone is not authorized",
        http_status_code=400,
    )
    stubber.activate()
    monkeypatch.setattr(DynamoDBConfig, "get_dynamodb_client", lambda self: client)
    result = _cli(tmp_path, "probe", "run", "tables")
    assert result.exit_code == rc.EXIT_CONNECTION, result.output
    assert "AccessDeniedException" in result.output
    # The code, never the message, which names the calling principal.
    assert "arn:aws" not in result.output
