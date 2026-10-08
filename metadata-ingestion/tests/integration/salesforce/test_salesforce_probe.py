"""Salesforce's probe over the ingestion suite's mocked REST responses: the
listing ingestion enumerates, split by kind, and verdicts that are
ingestion's."""

import json
import pathlib
from typing import Callable, Dict, Iterator, List, Set
from unittest import mock

import pytest
import requests
import yaml
from click.testing import CliRunner, Result
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from simple_salesforce.util import exception_handler

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn

# The suite's directory is not a package, so mypy cannot follow the import.
from tests.integration.salesforce.test_salesforce import (  # type: ignore[import-untyped]
    MockResponse,
    side_effect_call_salesforce,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness and the CLI mask output against the process-global
# secret registry; isolate it so the fixture's secret never masks a name.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_SDK = "datahub.ingestion.source.salesforce.Salesforce"
_RECIPE: Dict[str, object] = {
    "auth": "DIRECT_ACCESS_TOKEN",
    "instance_url": "https://mydomain.my.salesforce.com/",
    "access_token": "my-access-token",
}


@pytest.fixture
def sdk() -> Iterator[mock.Mock]:
    with mock.patch(_SDK) as sdk:
        client = mock.Mock()
        client._call_salesforce = mock.Mock(side_effect=side_effect_call_salesforce)
        sdk.return_value = client
        yield sdk


@pytest.fixture(autouse=True)
def _no_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)


def _recipe(**overrides: object) -> Dict[str, object]:
    return {**_RECIPE, **overrides}


def _names(rows: object) -> List[str]:
    assert isinstance(rows, list)
    return [row["name"] for row in rows]


def test_objects_and_custom_objects_split_the_one_listing(sdk: mock.Mock) -> None:
    recipe = _recipe(object_pattern={"allow": ["^Account$"]})
    standard = _names(run_probe_method("salesforce", recipe, "objects", {}).result)
    custom = _names(run_probe_method("salesforce", recipe, "custom_objects", {}).result)
    assert custom == ["Property__c"]
    # Denied objects are listed too: probe filter explains them.
    assert "Account" in standard and "Contract" in standard
    assert "Property__c" not in standard
    # 153 customizable objects in the fixture, one of them custom.
    assert len(standard) == 152


def test_records_carry_the_label_the_pattern_is_not_matched_on(
    sdk: mock.Mock,
) -> None:
    rows = run_probe_method("salesforce", _recipe(), "custom_objects", {}).result
    assert isinstance(rows, list)
    assert set(rows[0]) == {"name", "label"}


def test_the_limit_is_reported_as_truncation(sdk: mock.Mock) -> None:
    result = run_probe_method("salesforce", _recipe(), "objects", {"limit": 3})
    assert len(_names(result.result)) == 3
    assert result.truncated


def _emitted(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    """The API names of the datasets ingestion emitted with this subtype."""

    def names(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn, aspects in index.aspects.items()
            if any(
                isinstance(a, SubTypesClass) and sub_type in a.typeNames
                for a in aspects
            )
        }

    return names


def _listings(custom_expected: bool) -> List[ParityListing]:
    return [
        ParityListing(
            label="objects",
            command="objects",
            emitted=_emitted(DatasetSubTypes.SALESFORCE_STANDARD_OBJECT),
        ),
        ParityListing(
            label="custom_objects",
            command="custom_objects",
            emitted=_emitted(DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT),
            expect_empty=not custom_expected,
        ),
    ]


# Each recipe keeps only objects the fixture mocks in full: an object without
# mocked field responses fails mid-way in ingestion and emits no subtype.
@pytest.mark.parametrize(
    "object_pattern, custom_expected",
    [
        pytest.param({"allow": ["^Account$", "^Property__c$"]}, True, id="golden"),
        pytest.param(
            {"allow": ["^Account$", "^Property__c$"], "deny": ["^Property"]},
            False,
            id="deny-custom",
        ),
        # Prefix match, case-insensitive by default: "Account$" keeps Account
        # but not AccountCleanInfo, and "property__c" keeps Property__c.
        pytest.param({"allow": ["Account$", "property__c"]}, True, id="match-rules"),
    ],
)
def test_verdicts_match_ingestion(
    sdk: mock.Mock,
    tmp_path: pathlib.Path,
    object_pattern: Dict[str, object],
    custom_expected: bool,
) -> None:
    assert_probe_parity(
        "salesforce",
        _recipe(object_pattern=object_pattern),
        pipeline_ingestion("salesforce", tmp_path),
        _listings(custom_expected),
    )


def test_the_deny_reason_is_object_pattern(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    report = assert_probe_parity(
        "salesforce",
        _recipe(
            object_pattern={
                "allow": ["^Account$", "^Property__c$"],
                "deny": ["^Property"],
            }
        ),
        pipeline_ingestion("salesforce", tmp_path),
        _listings(custom_expected=False),
    )
    assert report.excluded_by("custom_objects") == {"Property__c": "object_pattern"}


def _cli(tmp_path: pathlib.Path, *args: str, **config: object) -> Result:
    path = tmp_path / "r.yml"
    path.write_text(
        yaml.safe_dump({"source": {"type": "salesforce", "config": _recipe(**config)}})
    )
    return CliRunner().invoke(recipe_cli, [*args, "--recipe", str(path)])


def test_probe_run_succeeds_through_the_cli(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    result = _cli(tmp_path, "probe", "run", "custom_objects")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["kind"] == "Custom Object"


def test_probe_filter_judges_the_api_name(tmp_path: pathlib.Path) -> None:
    result = _cli(
        tmp_path,
        "probe",
        "filter",
        "--kind",
        "Custom Object",
        "--name",
        "Property__c",
        "--name",
        "Other__c",
        object_pattern={"allow": ["^Property__c$"]},
    )
    assert result.exit_code == 0, result.output
    verdicts = {r["name"]: r["included"] for r in json.loads(result.output)["results"]}
    assert verdicts == {"Property__c": True, "Other__c": False}


def test_a_missing_credential_is_the_callers_to_fix(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    result = _cli(
        tmp_path,
        "probe",
        "run",
        "objects",
        auth="USERNAME_PASSWORD",
        username="someone@example.com",
        password=_RECIPE["access_token"],
    )
    assert result.exit_code == rc.EXIT_USER, result.output
    assert "security_token" in result.output
    sdk.assert_not_called()


def test_a_failed_login_is_the_sources_with_its_code(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    sdk.side_effect = SalesforceAuthenticationFailed(
        "INVALID_LOGIN",
        "Invalid username, password, security token; or user locked out.",
    )
    result = _cli(tmp_path, "probe", "run", "objects")
    assert result.exit_code == rc.EXIT_CONNECTION, result.output
    assert "INVALID_LOGIN" in result.output
    assert "locked out" not in result.output


def _raise_api_error(url: str, status: int, code: str, message: str) -> None:
    """Raise what simple_salesforce raises for an error response, through its
    own handler, so the exception carries the body as the real one does."""
    response = requests.Response()
    response.status_code = status
    response.url = url
    response._content = json.dumps([{"errorCode": code, "message": message}]).encode()
    exception_handler(response)


def test_a_missing_setup_permission_is_named(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    def refuse_entity_definition(method: str, url: str) -> MockResponse:
        if "FROM EntityDefinition" in url:
            _raise_api_error(
                url,
                400,
                "INVALID_TYPE",
                "sObject type 'EntityDefinition' is not supported.",
            )
        return side_effect_call_salesforce(method, url)

    sdk.return_value._call_salesforce.side_effect = refuse_entity_definition
    result = _cli(tmp_path, "probe", "run", "objects")
    assert result.exit_code == rc.EXIT_CONNECTION, result.output
    assert "View Setup and Configuration" in result.output


def test_an_expired_session_is_labelled_by_its_code(
    sdk: mock.Mock, tmp_path: pathlib.Path
) -> None:
    def expired(method: str, url: str) -> MockResponse:
        if "FROM EntityDefinition" in url:
            _raise_api_error(url, 401, "INVALID_SESSION_ID", "Session expired")
        return side_effect_call_salesforce(method, url)

    sdk.return_value._call_salesforce.side_effect = expired
    result = _cli(tmp_path, "probe", "run", "objects")
    assert result.exit_code == rc.EXIT_CONNECTION, result.output
    assert "INVALID_SESSION_ID" in result.output
    assert "Session expired" not in result.output
