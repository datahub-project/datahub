from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pydantic
import pytest

from datahub.ingestion.source.omni.omni import _scim_user_email
from datahub.ingestion.source.omni.omni_api import OmniClient


def _scim_page(ids: List[str], total: int, start_index: int) -> Dict[str, Any]:
    return {
        "Resources": [{"id": i, "userName": f"{i}@example.com"} for i in ids],
        "totalResults": total,
        "startIndex": start_index,
        "itemsPerPage": len(ids),
    }


def test_list_users_follows_scim_offset_paging() -> None:
    client = OmniClient(
        base_url="https://acme.omniapp.co/api",
        api_key=pydantic.SecretStr("k"),
    )
    pages = {
        1: _scim_page(["u1", "u2"], total=3, start_index=1),
        3: _scim_page(["u3"], total=3, start_index=3),
    }
    calls: List[Optional[Dict[str, Any]]] = []

    def fake_request(
        method: str, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        assert path == "/scim/v2/users"
        calls.append(params)
        assert params is not None
        return pages[params["startIndex"]]

    with patch.object(client, "_request", side_effect=fake_request):
        users = list(client.list_users(page_size=2))

    assert [u["id"] for u in users] == ["u1", "u2", "u3"]
    assert [c["startIndex"] for c in calls if c] == [1, 3]


@pytest.mark.parametrize(
    "user, expected",
    [
        ({"userName": " Jane.Doe@Example.com "}, "jane.doe@example.com"),
        (
            {
                "emails": [
                    {"primary": False, "value": "alt@example.com"},
                    {"primary": True, "value": "Main@Example.com"},
                ]
            },
            "main@example.com",
        ),
        ({"userName": "not-an-email"}, None),
        ({}, None),
    ],
)
def test_scim_user_email(user: Dict[str, Any], expected: Optional[str]) -> None:
    assert _scim_user_email(user) == expected
