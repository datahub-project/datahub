from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pydantic
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.omni.omni import OmniSource, _scim_user_email
from datahub.ingestion.source.omni.omni_api import OmniClient, ScimUser
from datahub.ingestion.source.omni.omni_config import OmniSourceConfig


def _client() -> OmniClient:
    return OmniClient(
        base_url="https://acme.omniapp.co/api", api_key=pydantic.SecretStr("k")
    )


def _users(ids: List[str]) -> List[Dict[str, Any]]:
    return [{"id": i, "userName": f"{i}@example.com"} for i in ids]


def _list_user_ids(
    client: OmniClient, pages: List[Dict[str, Any]], page_size: int
) -> List[str]:
    with patch.object(client, "_request", side_effect=pages) as request:
        ids = [u.get("id", "") for u in client.list_users(page_size=page_size)]
    assert request.call_count == len(pages), "unexpected number of SCIM requests"
    return ids


def test_list_users_follows_offset_paging_until_total() -> None:
    client = _client()
    pages = [
        {"Resources": _users(["u1", "u2"]), "totalResults": 3},
        {"Resources": _users(["u3"]), "totalResults": 3},
    ]
    with patch.object(client, "_request", side_effect=pages) as request:
        ids = [u.get("id") for u in client.list_users(page_size=2)]
    assert ids == ["u1", "u2", "u3"]
    assert [c.kwargs["params"]["startIndex"] for c in request.call_args_list] == [
        1,
        3,
    ]


def test_list_users_short_page_with_total_keeps_paging() -> None:
    # SCIM servers may cap a page below the requested count; with a total
    # present that must not end paging early.
    pages = [
        {"Resources": _users(["u1"]), "totalResults": 2},
        {"Resources": _users(["u2"]), "totalResults": 2},
    ]
    assert _list_user_ids(_client(), pages, page_size=100) == ["u1", "u2"]


def test_list_users_stops_on_short_page_without_total() -> None:
    # A server that ignores startIndex and omits totalResults would otherwise
    # be polled until the pagination safety cap.
    pages = [{"Resources": _users(["u1"])}]
    assert _list_user_ids(_client(), pages, page_size=100) == ["u1"]


@pytest.mark.parametrize(
    "user, expected",
    [
        ({"userName": " Jane.Doe@Example.com "}, "Jane.Doe@Example.com"),
        (
            {
                "userName": "jdoe",
                "emails": [
                    {"primary": False, "value": "alt@example.com"},
                    {"primary": True, "value": "main@example.com"},
                ],
            },
            "main@example.com",
        ),
        (
            {"userName": "jdoe", "emails": [{"value": "jdoe"}, {"value": "j@x.io"}]},
            "j@x.io",
        ),
        ({"userName": "jdoe", "emails": [{"value": "jdoe"}]}, None),
        ({}, None),
    ],
)
def test_scim_user_email(user: ScimUser, expected: Optional[str]) -> None:
    assert _scim_user_email(user) == expected


def test_owners_resolved_counts_distinct_owners() -> None:
    config = OmniSourceConfig.model_validate(
        {"base_url": "https://acme.omniapp.co/api", "api_key": "k"}
    )
    source = OmniSource(config, PipelineContext(run_id="test"))
    source._owner_email_by_id = {"u1": "a@example.com"}
    source._owner_lookup_succeeded = True

    for _ in range(3):
        source._resolve_owner_urn(owner_id="u1", owner_name="A")
    source._resolve_owner_urn(owner_id="u2", owner_name="B")
    source._resolve_owner_urn(owner_id="u2", owner_name="B")

    assert source.report.owners_resolved_to_email == 1
    assert list(source.report.owners_unresolved) == ["u2"]
