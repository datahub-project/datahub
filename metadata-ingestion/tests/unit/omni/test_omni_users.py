from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import patch

import pydantic
import pytest
import requests

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


def test_list_users_stops_when_server_replays_full_page_without_total() -> None:
    # Full pages and no totalResults defeat the short-page check; a replayed
    # page must still end paging instead of running to the safety cap.
    page = {"Resources": _users(["u1", "u2"])}
    assert _list_user_ids(_client(), [page, page], page_size=2) == ["u1", "u2"]


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


class _UsersClient:
    """Stands in for OmniClient.list_users; other methods are never reached."""

    def __init__(self, users: List[ScimUser], error: Optional[Exception] = None):
        self._users = users
        self._error = error

    def list_users(self, page_size: int = 100) -> Iterator[ScimUser]:
        yield from self._users
        if self._error is not None:
            raise self._error


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status}", response=response)


def _load(
    users: List[ScimUser],
    error: Optional[Exception] = None,
    extra_config: Optional[Dict[str, Any]] = None,
) -> OmniSource:
    config = OmniSourceConfig.model_validate(
        {
            "base_url": "https://acme.omniapp.co/api",
            "api_key": "k",
            **(extra_config or {}),
        }
    )
    source = OmniSource(config, PipelineContext(run_id="test"))
    source.client = _UsersClient(users, error)  # type: ignore[assignment]
    source._load_owner_emails()
    return source


_ADMIN: ScimUser = {"id": "u1", "userName": "Admin.User@example.com"}


@pytest.mark.parametrize(
    "lowercase, expected",
    [(True, "admin.user@example.com"), (False, "Admin.User@example.com")],
)
def test_load_owner_emails(lowercase: bool, expected: str) -> None:
    source = _load(
        [_ADMIN, {"id": "u2", "userName": "jdoe"}],
        extra_config={"lowercase_owner_emails": lowercase},
    )
    assert source._owner_email_by_id == {"u1": expected}
    assert source.report.users_scanned == 2


def test_partial_user_listing_resolves_no_owner() -> None:
    # Users from pages that loaded before the failure must not be used either,
    # otherwise an arbitrary subset of owners resolves.
    source = _load([_ADMIN], error=_http_error(503))
    assert source._owner_email_by_id == {}
    assert source._resolve_owner_urn(owner_id="u1", owner_name="A") == (
        "urn:li:corpuser:u1"
    )
    assert not source.report.owners_unresolved
    assert [w.title for w in source.report.warnings] == ["Owner email lookup failed"]


@pytest.mark.parametrize("status", [401, 403])
def test_unauthorized_user_listing_is_info(status: int) -> None:
    source = _load([], error=_http_error(status))
    assert not source.report.warnings
    assert [i.title for i in source.report.infos] == ["Owner emails not resolved"]


def test_non_http_user_listing_failure_warns() -> None:
    source = _load([], error=ConnectionError("reset"))
    assert [w.title for w in source.report.warnings] == ["Owner email lookup failed"]


def test_owner_falls_back_to_name_then_none() -> None:
    source = _load([])
    assert source._resolve_owner_urn(owner_id="", owner_name="Jane") == (
        "urn:li:corpuser:Jane"
    )
    assert source._resolve_owner_urn(owner_id="", owner_name="") is None


def test_list_users_stops_on_empty_page() -> None:
    assert _list_user_ids(_client(), [{"Resources": []}], page_size=100) == []


def test_list_users_stops_at_safety_cap() -> None:
    pages = [
        {"Resources": _users(["u1"]), "totalResults": 99},
        {"Resources": _users(["u2"]), "totalResults": 99},
    ]
    with patch("datahub.ingestion.source.omni.omni_api._MAX_PAGINATION_PAGES", 2):
        assert _list_user_ids(_client(), pages, page_size=1) == ["u1", "u2"]
