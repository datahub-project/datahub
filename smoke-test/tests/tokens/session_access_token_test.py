import os
import time

import pytest
from requests.exceptions import HTTPError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AuditStampClass, CorpUserStatusClass
from tests.utils import (
    get_admin_credentials,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

from .token_utils import getUserId, listUsers, removeUser

pytestmark = pytest.mark.no_cypress_suite1

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

(admin_user, admin_pass) = get_admin_credentials()
user_urn = "urn:li:corpuser:sessionUser"


@pytest.fixture(scope="class")
def custom_user_session():
    """Fixture to execute setup before and tear down after all tests are run"""
    admin_session = login_as(admin_user, admin_pass)

    res_data = removeUser(admin_session, user_urn)
    assert res_data
    assert "error" not in res_data

    # Test getting the invite token
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {
            getInviteToken(input: $input){
              inviteToken
            }
        }""",
        "variables": {"input": {}},
    }

    get_invite_token_response = admin_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    get_invite_token_response.raise_for_status()
    get_invite_token_res_data = get_invite_token_response.json()

    assert get_invite_token_res_data
    assert get_invite_token_res_data["data"]
    invite_token = get_invite_token_res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    assert "error" not in invite_token

    # Pass the invite token when creating the user
    sign_up_json = {
        "fullName": "Test Session User",
        "email": "sessionUser",
        "password": "sessionUser",
        "title": "Date Engineer",
        "inviteToken": invite_token,
    }

    sign_up_response = admin_session.post(
        f"{get_frontend_url()}/signUp", json=sign_up_json
    )
    sign_up_response.raise_for_status()
    assert sign_up_response
    assert "error" not in sign_up_response
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # signUp will override the session cookie to the new user to be signed up.
    admin_session.cookies.clear()
    admin_session = login_as(admin_user, admin_pass)

    # Make user created user is there.
    res_data = listUsers(admin_session)
    assert res_data["data"]
    assert res_data["data"]["listUsers"]
    assert {"username": "sessionUser"} in res_data["data"]["listUsers"]["users"]

    yield login_as(sign_up_json["email"], sign_up_json["password"])

    # Delete created user
    res_data = removeUser(admin_session, user_urn)
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeUser"] is True
    # Sleep for eventual consistency
    wait_for_writes_to_sync()

    # Make user created user is not there.
    res_data = listUsers(admin_session)
    assert res_data["data"]
    assert res_data["data"]["listUsers"]
    assert {"username": "sessionUser"} not in res_data["data"]["listUsers"]["users"]


@pytest.mark.dependency()
def test_soft_delete(graph_client, custom_user_session):
    # assert initial access
    assert getUserId(custom_user_session) == {"urn": user_urn}

    graph_client.soft_delete_entity(urn=user_urn)
    wait_for_writes_to_sync()

    with pytest.raises(HTTPError) as req_info:
        getUserId(custom_user_session)
    assert "403 Client Error: Forbidden" in str(req_info.value)

    # undo soft delete
    graph_client.set_soft_delete_status(urn=user_urn, delete=False)
    wait_for_writes_to_sync()


@pytest.mark.dependency(depends=["test_soft_delete"])
def test_suspend(graph_client, custom_user_session):
    # assert initial access
    assert getUserId(custom_user_session) == {"urn": user_urn}

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityType="corpuser",
            entityUrn=user_urn,
            changeType="UPSERT",
            aspectName="corpUserStatus",
            aspect=CorpUserStatusClass(
                status="SUSPENDED",
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000.0), actor="urn:li:corpuser:unknown"
                ),
            ),
        )
    )
    wait_for_writes_to_sync()

    with pytest.raises(HTTPError) as req_info:
        getUserId(custom_user_session)
    assert "403 Client Error: Forbidden" in str(req_info.value)

    # undo suspend
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityType="corpuser",
            entityUrn=user_urn,
            changeType="UPSERT",
            aspectName="corpUserStatus",
            aspect=CorpUserStatusClass(
                status="ACTIVE",
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000.0), actor="urn:li:corpuser:unknown"
                ),
            ),
        )
    )
    wait_for_writes_to_sync()


@pytest.mark.dependency(depends=["test_suspend"])
def test_hard_delete(graph_client, custom_user_session):
    # assert initial access
    assert getUserId(custom_user_session) == {"urn": user_urn}

    graph_client.hard_delete_entity(urn=user_urn)
    wait_for_writes_to_sync()

    with pytest.raises(HTTPError) as req_info:
        getUserId(custom_user_session)
    assert "403 Client Error: Forbidden" in str(req_info.value)
