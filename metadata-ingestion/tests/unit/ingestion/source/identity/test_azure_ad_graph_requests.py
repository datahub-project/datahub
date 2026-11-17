from typing import Any, Callable, Dict, Iterable, List
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.identity.azure_ad import AzureADConfig, AzureADSource

Fetch = Callable[[AzureADSource], Iterable[List]]

_FETCHES = [
    pytest.param(lambda source: source._get_azure_ad_users(), id="users"),
    pytest.param(
        lambda source: source._get_azure_ad_group_members({"id": "group-id"}),
        id="group_members",
    ),
]


def _make_source(**overrides: object) -> AzureADSource:
    config = AzureADConfig.model_validate(
        {
            "client_id": "client",
            "tenant_id": "tenant",
            "client_secret": "secret",
            "authority": "https://login.microsoftonline.com/tenant",
            "token_url": "https://login.microsoftonline.com/tenant/oauth2/token",
            **overrides,
        }
    )
    with patch.object(AzureADSource, "get_token", return_value="token"):
        return AzureADSource(config, PipelineContext(run_id="azure-ad-test"))


def _respond_with(source: AzureADSource, status_code: int, text: str) -> MagicMock:
    session = MagicMock()
    session.get.return_value = MagicMock(status_code=status_code, text=text)
    source.session = session
    return session


def _requested_select(source: AzureADSource, fetch: Fetch) -> List[str]:
    session = _respond_with(source, 200, '{"value": []}')
    list(fetch(source))
    url = session.get.call_args.args[0]
    return parse_qs(urlparse(url).query)["$select"][0].split(",")


@pytest.mark.parametrize("fetch", _FETCHES)
def test_user_requests_select_properties_graph_omits_by_default(fetch: Fetch) -> None:
    selected = _requested_select(_make_source(), fetch)

    # Graph only returns these when they are requested with $select.
    assert "accountEnabled" in selected
    assert "usageLocation" in selected
    # $select replaces Graph's default set, so mapped properties must stay in it.
    assert {"id", "displayName", "givenName", "surname", "mail", "jobTitle"} <= set(
        selected
    )
    assert "userPrincipalName" in selected


@pytest.mark.parametrize("fetch", _FETCHES)
def test_select_includes_custom_username_attribute(fetch: Fetch) -> None:
    source = _make_source(azure_ad_response_to_username_attr="employeeId")

    assert "employeeId" in _requested_select(source, fetch)


@pytest.mark.parametrize("fetch", _FETCHES)
def test_rejected_select_names_the_username_setting(fetch: Fetch) -> None:
    source = _make_source(azure_ad_response_to_username_attr="employeeNumber")
    graph_error = "Could not find a property named 'employeeNumber' on type 'microsoft.graph.user'."
    _respond_with(source, 400, f'{{"error": {{"message": "{graph_error}"}}}}')

    with pytest.raises(Exception) as raised:
        list(fetch(source))

    assert "azure_ad_response_to_username_attr" in str(raised.value)
    assert graph_error in str(raised.value)


@pytest.mark.parametrize("status_code", [401, 403])
def test_permission_errors_do_not_blame_the_username_setting(status_code: int) -> None:
    source = _make_source()
    _respond_with(
        source, status_code, '{"error": {"code": "Authorization_RequestDenied"}}'
    )

    with pytest.raises(Exception) as raised:
        list(source._get_azure_ad_users())

    assert "azure_ad_response_to_username_attr" not in str(raised.value)
    assert "Authorization_RequestDenied" in str(raised.value)


def test_user_country_comes_from_usage_location_not_mobile_phone() -> None:
    info = _make_source()._map_azure_ad_user_to_corp_user(
        {"displayName": "Jane Doe", "mobilePhone": "+1 555 0100", "usageLocation": "GB"}
    )

    assert info.countryCode == "GB"


def test_warns_when_graph_omits_account_enabled() -> None:
    source = _make_source(ingest_groups=False, ingest_group_membership=False)
    users: List[Dict[str, Any]] = [
        {"userPrincipalName": "a@example.com", "accountEnabled": True},
        {"userPrincipalName": "b@example.com"},
        {"userPrincipalName": "c@example.com", "accountEnabled": None},
        # The same user listed twice, as happens across group memberships.
        {"userPrincipalName": "c@example.com", "accountEnabled": None},
    ]
    with patch.object(AzureADSource, "_get_azure_ad_users", return_value=[users]):
        list(source.get_workunits_internal())

    warnings = list(source.report.warnings)
    assert len(warnings) == 1
    assert "User.Read.All" in str(warnings[0].context)
    assert "2 user(s)" in str(warnings[0].context)


def test_no_account_enabled_warning_when_graph_returns_it() -> None:
    source = _make_source(ingest_groups=False, ingest_group_membership=False)
    users = [{"userPrincipalName": "a@example.com", "accountEnabled": False}]
    with patch.object(AzureADSource, "_get_azure_ad_users", return_value=[users]):
        list(source.get_workunits_internal())

    assert not list(source.report.warnings)
