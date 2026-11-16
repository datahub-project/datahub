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


def test_user_country_comes_from_usage_location_not_mobile_phone() -> None:
    info = _make_source()._map_azure_ad_user_to_corp_user(
        {"displayName": "Jane Doe", "mobilePhone": "+1 555 0100", "usageLocation": "GB"}
    )

    assert info.countryCode == "GB"


def _requested_query(source: AzureADSource) -> Dict[str, List[str]]:
    session = _respond_with(source, 200, '{"value": []}')
    list(source._get_azure_ad_users())
    return parse_qs(urlparse(session.get.call_args.args[0]).query)


def test_select_includes_department_company_and_extra_attributes() -> None:
    source = _make_source(user_select_attributes=["officeLocation", "employeeId"])

    selected = _requested_query(source)["$select"][0].split(",")

    assert {"department", "companyName", "officeLocation", "employeeId"} <= set(
        selected
    )


def test_manager_is_expanded_only_when_enabled() -> None:
    assert "$expand" not in _requested_query(_make_source())

    expand = _requested_query(_make_source(ingest_user_manager=True))["$expand"][0]
    assert expand == "manager($select=id,userPrincipalName)"


def test_user_mapping_department_manager_and_custom_properties() -> None:
    source = _make_source(user_select_attributes=["officeLocation", "employeeId"])
    azure_ad_user: Dict[str, object] = {
        "displayName": "Jane Doe",
        "userPrincipalName": "jane@example.com",
        "department": "Data & AI",
        "companyName": "Example Corp",
        "officeLocation": "Madrid",
        "employeeId": None,
        "manager": {
            "@odata.type": "#microsoft.graph.user",
            "id": "m-1",
            "userPrincipalName": "boss@example.com",
        },
    }

    info = source._map_azure_ad_user_to_corp_user(azure_ad_user)

    assert info.departmentName == "Data & AI"
    assert info.managerUrn == "urn:li:corpuser:boss@example.com"
    # Unset attributes are left out rather than stored as "None".
    assert info.customProperties == {
        "companyName": "Example Corp",
        "officeLocation": "Madrid",
    }


def test_manager_that_is_not_a_user_is_ignored() -> None:
    source = _make_source()
    azure_ad_user: Dict[str, object] = {
        "displayName": "Jane Doe",
        "manager": {"@odata.type": "#microsoft.graph.orgContact", "id": "c-1"},
    }

    assert source._map_azure_ad_user_to_corp_user(azure_ad_user).managerUrn is None


def test_warns_when_graph_omits_account_enabled() -> None:
    source = _make_source(ingest_groups=False, ingest_group_membership=False)
    users: List[Dict[str, Any]] = [
        {"userPrincipalName": "a@example.com", "accountEnabled": True},
        {"userPrincipalName": "b@example.com"},
        {"userPrincipalName": "c@example.com", "accountEnabled": None},
    ]
    with patch.object(AzureADSource, "_get_azure_ad_users", return_value=[users]):
        list(source.get_workunits_internal())

    warnings = list(source.report.warnings)
    assert len(warnings) == 1
    assert "User.Read.All" in str(warnings[0].context)
    assert "2" in str(warnings[0].context)


def test_no_account_enabled_warning_when_graph_returns_it() -> None:
    source = _make_source(ingest_groups=False, ingest_group_membership=False)
    users = [{"userPrincipalName": "a@example.com", "accountEnabled": False}]
    with patch.object(AzureADSource, "_get_azure_ad_users", return_value=[users]):
        list(source.get_workunits_internal())

    assert not list(source.report.warnings)
