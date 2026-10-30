import base64
from typing import Any, Dict, List

import pytest
import requests
import requests_mock

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    FABRIC_BASE_URL,
    FABRIC_SCOPE,
    LRO_MIN_SLEEP_SECONDS,
    FabricDefinitionError,
    FabricDefinitionResolver,
    FabricLroTimeoutError,
)

WORKSPACE = "64ED5CAD-7C10-4684-8180-826122881108"
MODEL = "05169CD2-E713-41E6-9600-1D8066D95445"
DEFINITION_URL = (
    f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE}/semanticModels/{MODEL}/getDefinition"
)
LOCATION = f"{FABRIC_BASE_URL}/operations/0D1E2F3A-4B5C-4D6E-8F70-8192A3B4C5D6"


class FakeMsal:
    def __init__(self, token: Dict[str, Any]) -> None:
        self.token = token
        self.scopes: List[List[str]] = []

    def acquire_token_for_client(self, scopes: List[str]) -> Dict[str, Any]:
        self.scopes.append(scopes)
        return self.token


def _definition_body(text: str = "table Policies\n") -> Dict[str, Any]:
    return {
        "definition": {
            "format": "TMDL",
            "parts": [
                {
                    "path": "definition/tables/Policies.tmdl",
                    "payload": base64.b64encode(text.encode()).decode(),
                    "payloadType": "InlineBase64",
                },
                {"path": "broken", "payload": "%%%not-base64%%%"},
            ],
        }
    }


@pytest.fixture
def sleeps() -> List[float]:
    return []


@pytest.fixture
def resolver(sleeps: List[float]) -> FabricDefinitionResolver:
    return FabricDefinitionResolver(
        msal_client=FakeMsal({"access_token": "fabric-token", "expires_in": 3600}),  # type: ignore[arg-type]
        request_session=requests.Session(),
        lro_timeout_seconds=10,
        sleep_fn=sleeps.append,
    )


def test_token_uses_fabric_scope_and_bearer_header(
    resolver: FabricDefinitionResolver,
) -> None:
    assert resolver.get_access_token() == "Bearer fabric-token"
    assert resolver._msal_client.scopes == [[FABRIC_SCOPE]]  # type: ignore[attr-defined]
    # Cached: a second call does not go back to MSAL.
    resolver.get_access_token()
    assert len(resolver._msal_client.scopes) == 1  # type: ignore[attr-defined]


def test_missing_token_is_a_configuration_error() -> None:
    resolver = FabricDefinitionResolver(
        msal_client=FakeMsal({"error": "unauthorized_client"}),  # type: ignore[arg-type]
        request_session=requests.Session(),
        lro_timeout_seconds=10,
    )
    with pytest.raises(ConfigurationError):
        resolver.get_access_token()


def test_get_definition_direct_response_decodes_parts(
    resolver: FabricDefinitionResolver,
) -> None:
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, json=_definition_body())
        definition = resolver.get_item_definition(
            WORKSPACE, MODEL, "semanticModels", "TMDL"
        )
        assert m.last_request.qs == {"format": ["tmdl"]}
        assert m.last_request.headers["Authorization"] == "Bearer fabric-token"

    assert definition.format == "TMDL"
    # The undecodable part is reported, the valid one is text.
    assert [p.path for p in definition.parts] == ["definition/tables/Policies.tmdl"]
    assert definition.parts[0].text == "table Policies\n"
    assert definition.failed_parts == ["broken"]


def test_get_definition_polls_lro_until_succeeded(
    resolver: FabricDefinitionResolver, sleeps: List[float]
) -> None:
    with requests_mock.Mocker() as m:
        m.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": LOCATION, "Retry-After": "3"},
            text="",
        )
        m.get(
            LOCATION,
            [
                {"json": {"status": "Running"}, "headers": {"Retry-After": "1"}},
                {"json": {"status": "Succeeded"}},
            ],
        )
        m.get(f"{LOCATION}/result", json=_definition_body())

        definition = resolver.get_item_definition(
            WORKSPACE, MODEL, "semanticModels", "TMDL"
        )

    assert [p.path for p in definition.parts] == ["definition/tables/Policies.tmdl"]
    # First wait honours the 202's Retry-After, the second the poll's.
    assert sleeps == [3.0, 1.0]


def test_get_definition_lro_failure_raises(
    resolver: FabricDefinitionResolver,
) -> None:
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, status_code=202, headers={"Location": LOCATION}, text="")
        m.get(
            LOCATION,
            json={"status": "Failed", "error": {"message": "Item is protected"}},
        )
        with pytest.raises(FabricDefinitionError, match="Item is protected"):
            resolver.get_item_definition(WORKSPACE, MODEL, "semanticModels", "TMDL")


def test_get_definition_lro_timeout_is_bounded(
    resolver: FabricDefinitionResolver, sleeps: List[float]
) -> None:
    with requests_mock.Mocker() as m:
        m.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": LOCATION, "Retry-After": "4"},
            text="",
        )
        m.get(LOCATION, json={"status": "Running"}, headers={"Retry-After": "4"})
        with pytest.raises(FabricLroTimeoutError):
            resolver.get_item_definition(WORKSPACE, MODEL, "semanticModels", "TMDL")
    # 4 + 4 = 8 fits the 10s budget; the third 4s wait would exceed it.
    assert sleeps == [4.0, 4.0]


def test_get_definition_lro_zero_retry_after_still_times_out(
    resolver: FabricDefinitionResolver, sleeps: List[float]
) -> None:
    # A server answering Retry-After: 0 must not turn the budgeted poll loop
    # into a busy loop; the floor makes each wait count against the budget.
    with requests_mock.Mocker() as m:
        m.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": LOCATION, "Retry-After": "0"},
            text="",
        )
        m.get(LOCATION, json={"status": "Running"}, headers={"Retry-After": "0"})
        with pytest.raises(FabricLroTimeoutError):
            resolver.get_item_definition(WORKSPACE, MODEL, "semanticModels", "TMDL")
    assert sleeps
    assert all(s == LRO_MIN_SLEEP_SECONDS for s in sleeps)
    assert sum(sleeps) <= 10


def test_get_definition_202_without_location_is_an_error(
    resolver: FabricDefinitionResolver,
) -> None:
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, status_code=202, text="")
        with pytest.raises(FabricDefinitionError, match="Location"):
            resolver.get_item_definition(WORKSPACE, MODEL, "semanticModels", "TMDL")


def test_get_definition_http_error_propagates(
    resolver: FabricDefinitionResolver,
) -> None:
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, status_code=403, json={"errorCode": "Unauthorized"})
        with pytest.raises(requests.exceptions.HTTPError):
            resolver.get_item_definition(WORKSPACE, MODEL, "semanticModels", "TMDL")


def test_list_items_follows_continuation_token(
    resolver: FabricDefinitionResolver,
) -> None:
    url = f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE}/items"
    with requests_mock.Mocker() as m:
        m.get(
            url,
            [
                {
                    "json": {
                        "value": [
                            {"id": "a", "type": "Lakehouse", "displayName": "LH"}
                        ],
                        "continuationToken": "next",
                    }
                },
                {
                    "json": {
                        "value": [
                            {"id": "b", "type": "SQLEndpoint", "displayName": "LH"},
                            {"type": "Report", "displayName": "no id"},
                        ]
                    }
                },
            ],
        )
        items = resolver.list_items(WORKSPACE)
        assert m.request_history[1].qs == {"continuationtoken": ["next"]}

    assert [(i.id, i.type, i.workspace_id) for i in items] == [
        ("a", "Lakehouse", WORKSPACE),
        ("b", "SQLEndpoint", WORKSPACE),
    ]


def test_lakehouse_sql_endpoint_id(resolver: FabricDefinitionResolver) -> None:
    url = f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE}/lakehouses/lh"
    with requests_mock.Mocker() as m:
        m.get(url, json={"properties": {"sqlEndpointProperties": {"id": "ep"}}})
        assert resolver.get_lakehouse_sql_endpoint_id(WORKSPACE, "lh") == "ep"
        m.get(url, json={"properties": {}})
        assert resolver.get_lakehouse_sql_endpoint_id(WORKSPACE, "lh") is None


def test_binary_parts_are_skipped_without_being_reported(
    resolver: FabricDefinitionResolver,
) -> None:
    # Report definitions ship background images and custom-visual bundles as
    # base64 binary parts; they carry no metadata and must not look like
    # failures to the operator.
    body = _definition_body()
    body["definition"]["parts"].append(
        {
            "path": "StaticResources/RegisteredResources/background.png",
            "payload": base64.b64encode(b"\x89PNG\r\n\x1a\n\x00\x00").decode(),
            "payloadType": "InlineBase64",
        }
    )
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, json=body)
        definition = resolver.get_item_definition(
            WORKSPACE, MODEL, "semanticModels", "TMDL"
        )
    assert [p.path for p in definition.parts] == ["definition/tables/Policies.tmdl"]
    assert definition.failed_parts == ["broken"]


def test_unreadable_metadata_part_is_reported_and_bom_is_tolerated(
    resolver: FabricDefinitionResolver,
) -> None:
    # The binary skip must not swallow a .tmdl we cannot decode (a table would
    # silently vanish), and a UTF-8 BOM must not reach the TMDL parser.
    body = _definition_body()
    body["definition"]["parts"].append(
        {
            "path": "definition/tables/Legacy.tmdl",
            "payload": base64.b64encode("table Legacy\n".encode("utf-16")).decode(),
            "payloadType": "InlineBase64",
        }
    )
    body["definition"]["parts"].append(
        {
            "path": "definition/relationships.tmdl",
            "payload": base64.b64encode("\ufeffrelationship r1\n".encode()).decode(),
            "payloadType": "InlineBase64",
        }
    )
    with requests_mock.Mocker() as m:
        m.post(DEFINITION_URL, json=body)
        definition = resolver.get_item_definition(
            WORKSPACE, MODEL, "semanticModels", "TMDL"
        )
    assert definition.failed_parts == ["broken", "definition/tables/Legacy.tmdl"]
    assert definition.parts[-1].text == "relationship r1\n"
