from unittest.mock import MagicMock

import pytest
import requests

from tests.e2e.privileges import utils as privileges_utils
from tests.e2e.utils import TestSessionWrapper
from utilities.domains import Domain

pytestmark = [pytest.mark.domain(Domain.PLATFORM)]


@pytest.fixture
def waits(monkeypatch):
    wait = MagicMock()
    monkeypatch.setattr(privileges_utils, "wait_for_writes_to_sync", wait)
    monkeypatch.setattr(privileges_utils, "get_frontend_url", lambda: "http://fe")
    return wait


@pytest.fixture
def wrapper(monkeypatch):
    session = object.__new__(TestSessionWrapper)
    session._upstream = MagicMock()
    session._upstream.cookies = requests.cookies.RequestsCookieJar()
    session._gms_token = "token"
    wrapper_wait = MagicMock()
    monkeypatch.setattr(session, "_wait", wrapper_wait, raising=False)
    return session, wrapper_wait


def test_write_helper_waits_once_on_a_wrapped_session(waits, wrapper):
    session, wrapper_wait = wrapper

    privileges_utils.remove_user(session, "urn:li:corpuser:someone")

    session._upstream.post.assert_called_once()
    wrapper_wait.assert_not_called()
    waits.assert_called_once()


def test_create_user_skips_the_pre_delete_for_a_new_email(waits, wrapper):
    session, _ = wrapper
    session._upstream.post.return_value.json.return_value = {
        "data": {"getInviteToken": {"inviteToken": "invite"}}
    }

    privileges_utils.create_user(
        session, "new@smoke.datahub.test", "pw", remove_existing=False
    )

    posted_bodies = [
        call.kwargs.get("json", {}) for call in session._upstream.post.call_args_list
    ]
    assert not any(
        "removeUser" in body.get("query", "") for body in posted_bodies if body
    )


def _graphql_response(body, status=200):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body
    return response


_DENIED = {"message": "Unauthorized", "extensions": {"code": 403}}


@pytest.mark.parametrize(
    ("response", "waits_expected"),
    [
        (_graphql_response({"data": {"setTag": None}, "errors": [_DENIED]}), False),
        (_graphql_response({"data": None, "errors": [_DENIED]}), False),
        (_graphql_response({}, status=403), False),
        (_graphql_response({"data": {"setTag": True}}), True),
        # One field written, another denied: the write still has to sync.
        (
            _graphql_response({"data": {"a": True, "b": None}, "errors": [_DENIED]}),
            True,
        ),
        (
            _graphql_response(
                {"data": {"setTag": None}, "errors": [{"extensions": {"code": 500}}]}
            ),
            True,
        ),
        # Denied while reading the result, after the resolver ran: the write may exist.
        (
            _graphql_response(
                {
                    "data": {"createThing": None},
                    "errors": [{**_DENIED, "path": ["createThing", "owner"]}],
                }
            ),
            True,
        ),
    ],
)
def test_wrapper_skips_the_sync_wait_only_for_denied_writes(
    wrapper, response, waits_expected
):
    session, wrapper_wait = wrapper
    session._upstream.post.return_value = response

    session.post("http://gms/api/graphql", json={"query": "mutation { setTag }"})

    assert wrapper_wait.called is waits_expected
