from typing import Dict, List
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.identity.azure_ad import AzureADConfig, AzureADSource


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


def _requested_select(source: AzureADSource, fetch: str) -> List[str]:
    response = MagicMock(status_code=200, text='{"value": []}')
    source.session = MagicMock()
    source.session.get.return_value = response
    if fetch == "users":
        list(source._get_azure_ad_users())
    else:
        list(source._get_azure_ad_group_members({"id": "group-id"}))
    url = source.session.get.call_args.args[0]
    return parse_qs(urlparse(url).query)["$select"][0].split(",")


@pytest.mark.parametrize("fetch", ["users", "group_members"])
def test_user_requests_select_properties_graph_omits_by_default(fetch: str) -> None:
    selected = _requested_select(_make_source(), fetch)

    # Graph only returns these when they are requested with $select.
    assert "accountEnabled" in selected
    assert "usageLocation" in selected
    # $select replaces Graph's default set, so mapped properties must stay in it.
    assert {"id", "displayName", "givenName", "surname", "mail", "jobTitle"} <= set(
        selected
    )
    assert "userPrincipalName" in selected


@pytest.mark.parametrize("fetch", ["users", "group_members"])
def test_select_includes_custom_username_attribute(fetch: str) -> None:
    source = _make_source(azure_ad_response_to_username_attr="employeeId")

    assert "employeeId" in _requested_select(source, fetch)


def test_user_mapping_uses_usage_location_as_country_and_reads_status() -> None:
    source = _make_source()
    azure_ad_user: Dict[str, object] = {
        "displayName": "Jane Doe",
        "givenName": "Jane",
        "surname": "Doe",
        "mail": "jane@example.com",
        "mobilePhone": "+1 555 0100",
        "usageLocation": "GB",
        "accountEnabled": False,
    }

    info = source._map_azure_ad_user_to_corp_user(azure_ad_user)

    assert info.countryCode == "GB"
    assert info.active is False


def _requested_query(source: AzureADSource) -> Dict[str, List[str]]:
    source.session = MagicMock()
    source.session.get.return_value = MagicMock(status_code=200, text='{"value": []}')
    list(source._get_azure_ad_users())
    return parse_qs(urlparse(source.session.get.call_args.args[0]).query)


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
