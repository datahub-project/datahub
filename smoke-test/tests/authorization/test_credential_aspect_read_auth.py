"""
E2E authorization smoke tests for reading the corpUserCredentials aspect.

The aspect holds native users' password hashes, encrypted salts and reset tokens.
Any authenticated account passes the entity-level READ check on ``corpuser``, so
the aspect itself must additionally require Manage User Credentials on every
surface that returns raw aspects: GraphQL ``aspects``, OpenAPI v3 and Rest.li.
"""

import logging
import urllib.parse
import uuid

import pytest
import requests

from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import create_user, remove_user
from tests.utilities.domains import Domain
from tests.utils import get_frontend_session, get_frontend_url, get_gms_url, login_as

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.domain(Domain.PLATFORM),
    pytest.mark.p0,
]

_UNIQUE = uuid.uuid4().hex[:8]
USER_PASSWORD = "user"
ATTACKER_EMAIL = f"cred.read.attacker.{_UNIQUE}@smoke.datahub.test"
ATTACKER_URN = f"urn:li:corpuser:{ATTACKER_EMAIL}"
VICTIM_EMAIL = f"cred.read.victim.{_UNIQUE}@smoke.datahub.test"
VICTIM_URN = f"urn:li:corpuser:{VICTIM_EMAIL}"
CREDENTIALS_ASPECT = "corpUserCredentials"
INFO_ASPECT = "corpUserInfo"

_state: dict = {}


def _graphql(session, payload: dict) -> dict:
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


def _mint_token(admin_session, actor_urn: str) -> tuple[str, str]:
    data = _graphql(
        admin_session,
        {
            "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
                createAccessToken(input: $input) {
                  accessToken
                  metadata { id }
                }
              }""",
            "variables": {
                "input": {
                    "type": "PERSONAL",
                    "actorUrn": actor_urn,
                    "duration": "ONE_HOUR",
                    "name": f"cred-read-token-{_UNIQUE}",
                }
            },
        },
    )
    result = data["data"]["createAccessToken"]
    return result["accessToken"], result["metadata"]["id"]


def _revoke_token(admin_session, token_id: str) -> None:
    _graphql(
        admin_session,
        {
            "query": """mutation revokeAccessToken($tokenId: String!) {
                revokeAccessToken(tokenId: $tokenId)
            }""",
            "variables": {"tokenId": token_id},
        },
    )


@pytest.fixture(scope="module", autouse=True)
def credential_users():
    admin_session = get_frontend_session()
    create_user(admin_session, ATTACKER_EMAIL, USER_PASSWORD)
    create_user(admin_session, VICTIM_EMAIL, USER_PASSWORD)
    token, token_id = _mint_token(admin_session, ATTACKER_URN)
    wait_for_writes_to_sync()
    _state["attacker_headers"] = {
        "Authorization": f"Bearer {token}",
        "X-RestLi-Protocol-Version": "2.0.0",
    }
    yield
    try:
        _revoke_token(admin_session, token_id)
    except Exception:
        logger.warning("Failed to revoke attacker token during cleanup")
    for urn in (ATTACKER_URN, VICTIM_URN):
        try:
            remove_user(admin_session, urn)
        except Exception:
            logger.warning("Failed to remove %s during cleanup", urn)


def _raw_aspects_payload(aspect_name: str) -> dict:
    return {
        "query": """query getAspects($urn: String!, $names: [String!]) {
            corpUser(urn: $urn) {
              urn
              aspects(input: { aspectNames: $names }) { aspectName }
            }
          }""",
        "variables": {"urn": VICTIM_URN, "names": [aspect_name]},
    }


def test_graphql_raw_aspects_hide_credentials_from_regular_user():
    user_session = login_as(ATTACKER_EMAIL, USER_PASSWORD)

    info = _graphql(user_session, _raw_aspects_payload(INFO_ASPECT))
    assert not info.get("errors"), info
    assert [a["aspectName"] for a in info["data"]["corpUser"]["aspects"]] == [
        INFO_ASPECT
    ]

    creds = _graphql(user_session, _raw_aspects_payload(CREDENTIALS_ASPECT))
    assert not creds.get("errors"), creds
    assert creds["data"]["corpUser"]["aspects"] == []


def test_openapi_credentials_aspect_forbidden_for_regular_user():
    encoded = urllib.parse.quote(VICTIM_URN, safe="")
    headers = _state["attacker_headers"]

    aspect_resp = requests.get(
        f"{get_gms_url()}/openapi/v3/entity/corpuser/{encoded}/{CREDENTIALS_ASPECT}",
        headers=headers,
    )
    assert aspect_resp.status_code == 403, aspect_resp.text

    entity_resp = requests.get(
        f"{get_gms_url()}/openapi/v3/entity/corpuser/{encoded}", headers=headers
    )
    assert entity_resp.status_code == 200, entity_resp.text
    body = entity_resp.json()
    assert INFO_ASPECT in body, body
    assert CREDENTIALS_ASPECT not in body, body


def test_restli_credentials_aspect_forbidden_for_regular_user():
    encoded = urllib.parse.quote(VICTIM_URN, safe="")
    resp = requests.get(
        f"{get_gms_url()}/aspects/{encoded}",
        params={"aspect": CREDENTIALS_ASPECT, "version": "0"},
        headers=_state["attacker_headers"],
    )
    assert resp.status_code == 403, resp.text


def test_openapi_credentials_aspect_visible_to_admin(auth_session):
    encoded = urllib.parse.quote(VICTIM_URN, safe="")
    resp = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v3/entity/corpuser/{encoded}/{CREDENTIALS_ASPECT}"
    )
    assert resp.status_code == 200, resp.text
    assert "hashedPassword" in resp.json().get("value", {}), resp.text
