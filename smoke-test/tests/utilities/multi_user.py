"""Helpers for provisioning ephemeral native users in smoke tests."""

from __future__ import annotations

import logging
import uuid

from tests.tokens.token_utils import removeUser, wait_for_user_in_list
from tests.utils import (
    TestSessionWrapper,
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

_DEFAULT_PASSWORD = "smokeTestUser1!"


def _user_email(prefix: str) -> str:
    # Valid email for auth.native.signUp.enforceValidEmail (Play EmailValidator).
    slug = prefix.replace("_", "-").lower()
    return f"{slug}.{uuid.uuid4().hex[:8]}@smoke.datahub.test"


def _fetch_invite_token(admin_session) -> str:
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {
            getInviteToken(input: $input){
              inviteToken
            }
        }""",
        "variables": {"input": {}},
    }
    response = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data.get("data"), f"getInviteToken failed: {res_data}"
    invite_token = res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    return invite_token


def _restore_admin_frontend_session(admin_session) -> None:
    """Re-establish admin frontend cookies after /signUp overwrites the session."""
    admin_user, admin_pass = get_admin_credentials()
    fresh_login = login_as(admin_user, admin_pass)
    if isinstance(admin_session, TestSessionWrapper):
        admin_session._upstream.cookies.clear()
        admin_session._upstream.cookies.update(fresh_login.cookies)
    else:
        admin_session.cookies.clear()
        admin_session.cookies.update(fresh_login.cookies)


def make_step_actor_user(admin_session, prefix: str) -> tuple[str, TestSessionWrapper]:
    """Create a native user and return ``(urn, session)`` logged in as that user.

    ``admin_session`` must have ``manageIdentities`` (typically the datahub admin).
    """
    email = _user_email(prefix)
    user_urn = f"urn:li:corpuser:{email}"

    res_data = removeUser(admin_session, user_urn)
    assert res_data
    assert "error" not in res_data
    wait_for_writes_to_sync()

    sign_up_json = {
        "fullName": "Smoke Test User",
        "email": email,
        "password": _DEFAULT_PASSWORD,
        "title": "Data Engineer",
        "inviteToken": _fetch_invite_token(admin_session),
    }
    sign_up_response = admin_session.post(
        f"{get_frontend_url()}/signUp", json=sign_up_json
    )
    sign_up_response.raise_for_status()

    # signUp assigns the new user's frontend cookie on the underlying requests session.
    _restore_admin_frontend_session(admin_session)

    wait_for_writes_to_sync()
    wait_for_user_in_list(admin_session, email, present=True)

    regular_session = TestSessionWrapper(login_as(email, _DEFAULT_PASSWORD))
    logger.info("Provisioned step actor user %s", user_urn)
    return user_urn, regular_session


def cleanup_step_actor_user(admin_session, user_urn: str) -> None:
    """Remove a user created by :func:`make_step_actor_user`."""
    email = user_urn.removeprefix("urn:li:corpuser:")
    try:
        res_data = removeUser(admin_session, user_urn)
        if not res_data.get("data", {}).get("removeUser"):
            logger.warning("removeUser returned false for %s: %s", user_urn, res_data)
            return
        wait_for_writes_to_sync()
        wait_for_user_in_list(admin_session, email, present=False)
        logger.info("Removed step actor user %s", user_urn)
    except Exception:
        logger.warning("Failed to remove step actor user %s", user_urn, exc_info=True)
