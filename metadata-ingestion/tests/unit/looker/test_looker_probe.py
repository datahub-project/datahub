import json
from typing import Any, Dict
from unittest import mock

import looker_sdk.rtl.requests_transport as looker_requests_transport
import pytest
from looker_sdk.error import SDKError

from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError, ProbeReadFailed
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_probe import (
    LookerMetadataProbe,
    sdk_error_status,
)
from datahub.ingestion.source.looker.looker_source import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
)
from tests.unit.looker.looker_probe_fixtures import (
    API_USER_EMAIL,
    CLIENT_ID,
    CLIENT_SECRET,
    fake_looker,
    install,
    recipe,
    sdk_error,
)


def _run(command: str, params: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    with fake_looker():
        return run_probe_method("looker", recipe(**overrides), command, params).to_dict()


def test_building_the_provider_opens_no_connection() -> None:
    with mock.patch("looker_sdk.init40") as init40:
        probe = LookerMetadataProbe.for_config(
            LookerDashboardSourceConfig.model_validate(recipe())
        )
        with probe:
            pass
    init40.assert_not_called()


def test_exit_closes_the_sdk_session_once_a_command_opened_it() -> None:
    client = install(mock.MagicMock())
    transport = mock.create_autospec(
        looker_requests_transport.RequestsTransport, instance=True
    )
    transport.session = mock.MagicMock()
    client.transport = transport
    with mock.patch("looker_sdk.init40", return_value=client):
        run_probe_method("looker", recipe(), "permissions", {})
    transport.session.close.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [sdk_error(401, f"bad {CLIENT_SECRET}"), SDKError(f"raw body {CLIENT_SECRET}")],
)
def test_refused_credentials_name_the_host_and_nothing_the_sdk_said(
    error: SDKError,
) -> None:
    client = install(mock.MagicMock())
    client.me.side_effect = error
    with (
        mock.patch("looker_sdk.init40", return_value=client),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        run_probe_method("looker", recipe(), "permissions", {})
    text = str(raised.value)
    assert "looker.example.com" in text
    assert CLIENT_SECRET not in text and "raw body" not in text
    assert CLIENT_ID not in text


def test_a_base_url_carrying_userinfo_is_refused_without_echoing_it() -> None:
    with fake_looker(), pytest.raises(ValueError) as raised:
        run_probe_method(
            "looker",
            recipe(base_url="https://someone:hunter22@looker.example.com"),
            "permissions",
            {},
        )
    assert "hunter22" not in str(raised.value)


def test_sdk_error_status_reads_the_documentation_url_then_the_message() -> None:
    assert sdk_error_status(sdk_error(404)) == 404
    assert sdk_error_status(SDKError("Looker Not Found (404)")) == 404
    assert sdk_error_status(SDKError("no status here")) is None


def test_a_read_failure_reports_the_status_and_not_the_sdk_text() -> None:
    client = install(mock.MagicMock())
    client.user_roles.side_effect = sdk_error(500, f"boom {CLIENT_SECRET}")
    with (
        mock.patch("looker_sdk.init40", return_value=client),
        pytest.raises(ProbeReadFailed) as raised,
    ):
        run_probe_method("looker", recipe(), "permissions", {})
    assert "HTTP 500" in str(raised.value)
    assert CLIENT_SECRET not in str(raised.value) and "boom" not in str(raised.value)


def test_permissions_report_what_ingestion_needs_and_lacks() -> None:
    result = _run("permissions", {})["result"]
    assert result["granted"] == ["access_data", "explore", "see_lookml", "see_looks"]
    assert "see_users" in result["missing_for_metadata"]
    assert set(result["missing_for_metadata"]) <= BASIC_INGEST_REQUIRED_PERMISSIONS
    assert result["missing_for_usage"] == ["see_system_activity"]


def test_unreadable_roles_degrade_to_a_warning() -> None:
    client = install(mock.MagicMock())
    client.user_roles.side_effect = sdk_error(403)
    with mock.patch("looker_sdk.init40", return_value=client):
        run = run_probe_method("looker", recipe(), "permissions", {})
    assert run.result == {
        "granted": None,
        "missing_for_metadata": None,
        "missing_for_usage": None,
    }
    assert any("HTTP 403" in w for w in run.warnings)


def test_no_command_output_carries_credentials_or_the_api_user() -> None:
    dumped = json.dumps(_run("permissions", {}))
    for secret in (CLIENT_ID, CLIENT_SECRET, API_USER_EMAIL, '"7"'):
        assert secret not in dumped


def test_methods_are_advertised_with_their_kinds() -> None:
    kinds = {spec.command: spec.kind for spec in list_probe_methods("looker")}
    assert kinds["permissions"] is None
