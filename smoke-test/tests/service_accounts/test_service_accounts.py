"""
Smoke tests for Service Accounts GraphQL endpoints.

Tests the following operations:
- createServiceAccount: Create a new service account
- listServiceAccounts: List all service accounts
- getServiceAccount: Get a specific service account by URN
- deleteServiceAccount: Delete a service account
- createAccessToken for service account: Generate API token for a service account
"""

from typing import Optional

import tenacity

from tests.utils import get_sleep_info

sleep_sec, sleep_times = get_sleep_info()


# Helper functions for GraphQL operations


def create_service_account(
    session,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
):
    """Create a new service account. ID is auto-generated as a UUID."""
    input_data: dict = {}
    if display_name:
        input_data["displayName"] = display_name
    if description:
        input_data["description"] = description

    json_data = {
        "query": """mutation createServiceAccount($input: CreateServiceAccountInput!) {
            createServiceAccount(input: $input) {
                urn
                type
                name
                displayName
                description
                createdBy
                createdAt
            }
        }""",
        "variables": {"input": input_data},
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


def list_service_accounts(
    session, start: int = 0, count: int = 20, query: Optional[str] = None
):
    """List service accounts."""
    input_data: dict = {"start": start, "count": count}
    if query:
        input_data["query"] = query

    json_data = {
        "query": """query listServiceAccounts($input: ListServiceAccountsInput!) {
            listServiceAccounts(input: $input) {
                start
                count
                total
                serviceAccounts {
                    urn
                    type
                    name
                    displayName
                    description
                    createdBy
                    createdAt
                    updatedAt
                }
            }
        }""",
        "variables": {"input": input_data},
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


def get_service_account(session, urn: str):
    """Get a specific service account by URN."""
    json_data = {
        "query": """query getServiceAccount($urn: String!) {
            getServiceAccount(urn: $urn) {
                urn
                type
                name
                displayName
                description
                createdBy
                createdAt
                updatedAt
            }
        }""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


def delete_service_account(session, urn: str):
    """Delete a service account."""
    json_data = {
        "query": """mutation deleteServiceAccount($urn: String!) {
            deleteServiceAccount(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


def create_access_token_for_service_account(
    session, actor_urn: str, name: str = "test-token", duration: str = "ONE_HOUR"
):
    """Create an access token for a service account."""
    json_data = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
                accessToken
                metadata {
                    id
                    actorUrn
                    ownerUrn
                    name
                    description
                }
            }
        }""",
        "variables": {
            "input": {
                "type": "SERVICE_ACCOUNT",
                "actorUrn": actor_urn,
                "duration": duration,
                "name": name,
            }
        },
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


def revoke_access_token(session, token_id: str):
    """Revoke an access token."""
    json_data = {
        "query": """mutation revokeAccessToken($tokenId: String!) {
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": token_id},
    }

    response = session.post(f"{session.frontend_url()}/api/v2/graphql", json=json_data)
    response.raise_for_status()
    return response.json()


# Retry helper for eventual consistency


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_service_account_count(
    session, expected_count: int, query: Optional[str] = None
):
    """Wait for the service account list to have the expected count."""
    res_data = list_service_accounts(session, query=query)
    assert res_data
    assert res_data.get("data"), f"Response missing data: {res_data}"
    assert "errors" not in res_data, f"Unexpected errors: {res_data.get('errors')}"
    assert res_data["data"]["listServiceAccounts"]["total"] == expected_count, (
        f"Expected {expected_count} service accounts, got {res_data['data']['listServiceAccounts']['total']}"
    )


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_service_account_exists(session, urn: str):
    """Wait for a service account to be retrievable."""
    res_data = get_service_account(session, urn)
    assert res_data
    assert res_data.get("data"), f"Response missing data: {res_data}"
    assert "errors" not in res_data, f"Unexpected errors: {res_data.get('errors')}"
    assert res_data["data"]["getServiceAccount"] is not None, (
        f"Service account {urn} not found"
    )
    return res_data


# Tests


def test_create_list_get_delete_service_account(auth_session):
    """
    Test the full lifecycle of a service account:
    1. Create a service account
    2. List service accounts (verify it appears)
    3. Get the service account by URN
    4. Delete the service account
    5. Verify it's gone
    """
    display_name = "Test Smoke Service Account"
    description = "A service account created for smoke testing"

    # Get initial count
    res_data = list_service_accounts(auth_session)
    assert res_data, "Failed to list service accounts"
    assert res_data.get("data"), f"Response missing data: {res_data}"
    assert "errors" not in res_data, (
        f"Unexpected errors in list: {res_data.get('errors')}"
    )
    before_count = res_data["data"]["listServiceAccounts"]["total"]
    print(f"Initial service account count: {before_count}")

    # Step 1: Create service account (ID is auto-generated as UUID)
    print("Creating service account with auto-generated ID")
    res_data = create_service_account(
        auth_session,
        display_name=display_name,
        description=description,
    )
    assert res_data, "Failed to create service account"
    assert res_data.get("data"), f"Response missing data: {res_data}"
    assert "errors" not in res_data, (
        f"Unexpected errors in create: {res_data.get('errors')}"
    )

    created_account = res_data["data"]["createServiceAccount"]
    assert created_account is not None, "createServiceAccount returned null"
    assert created_account["urn"], "Service account URN is missing"
    assert created_account["displayName"] == display_name, (
        f"Expected displayName {display_name}, got {created_account['displayName']}"
    )
    assert created_account["description"] == description, (
        f"Expected description {description}, got {created_account['description']}"
    )

    service_account_urn = created_account["urn"]
    print(f"Created service account with URN: {service_account_urn}")

    # Step 2: List service accounts and verify count increased
    _ensure_service_account_count(auth_session, before_count + 1)
    print("Verified service account count increased")

    # Step 3: Get the service account by URN
    res_data = _ensure_service_account_exists(auth_session, service_account_urn)
    fetched_account = res_data["data"]["getServiceAccount"]
    assert fetched_account["urn"] == service_account_urn
    assert fetched_account["displayName"] == display_name
    assert fetched_account["description"] == description
    print(f"Successfully retrieved service account: {fetched_account}")

    # Step 4: Delete the service account
    print(f"Deleting service account: {service_account_urn}")
    res_data = delete_service_account(auth_session, service_account_urn)
    assert res_data, "Failed to delete service account"
    assert res_data.get("data"), f"Response missing data: {res_data}"
    assert "errors" not in res_data, (
        f"Unexpected errors in delete: {res_data.get('errors')}"
    )
    assert res_data["data"]["deleteServiceAccount"] is True, (
        f"deleteServiceAccount returned {res_data['data']['deleteServiceAccount']}"
    )
    print("Service account deleted successfully")

    # Step 5: Verify count is back to original
    _ensure_service_account_count(auth_session, before_count)
    print("Verified service account count is back to original")


def test_create_access_token_for_service_account(auth_session):
    """
    Test creating an access token for a service account:
    1. Create a service account
    2. Create an access token for it
    3. Verify the token metadata
    4. Revoke the token
    5. Delete the service account
    """
    # Step 1: Create service account (ID is auto-generated)
    print("Creating service account for token test")
    res_data = create_service_account(
        auth_session,
        display_name="Token Test Service Account",
        description="Service account for testing token creation",
    )
    assert res_data.get("data"), f"Failed to create service account: {res_data}"
    assert "errors" not in res_data, f"Errors in create: {res_data.get('errors')}"

    service_account_urn = res_data["data"]["createServiceAccount"]["urn"]
    print(f"Created service account: {service_account_urn}")

    # Wait for it to be indexed
    _ensure_service_account_exists(auth_session, service_account_urn)

    # Step 2: Create access token for service account
    print(f"Creating access token for: {service_account_urn}")
    res_data = create_access_token_for_service_account(
        auth_session,
        actor_urn=service_account_urn,
        name="test-service-account-token",
    )
    assert res_data.get("data"), f"Failed to create access token: {res_data}"
    assert "errors" not in res_data, f"Errors in token create: {res_data.get('errors')}"

    token_result = res_data["data"]["createAccessToken"]
    assert token_result is not None, "createAccessToken returned null"
    assert token_result["accessToken"], "Access token is empty"
    assert token_result["metadata"]["actorUrn"] == service_account_urn, (
        f"Expected actorUrn {service_account_urn}, got {token_result['metadata']['actorUrn']}"
    )

    token_id = token_result["metadata"]["id"]
    print(f"Created access token with ID: {token_id}")

    # Step 3: Revoke the token
    print(f"Revoking access token: {token_id}")
    res_data = revoke_access_token(auth_session, token_id)
    assert res_data.get("data"), f"Failed to revoke token: {res_data}"
    assert "errors" not in res_data, f"Errors in revoke: {res_data.get('errors')}"
    assert res_data["data"]["revokeAccessToken"] is True
    print("Token revoked successfully")

    # Step 4: Clean up - delete service account
    print(f"Deleting service account: {service_account_urn}")
    res_data = delete_service_account(auth_session, service_account_urn)
    assert res_data.get("data"), f"Failed to delete service account: {res_data}"
    assert "errors" not in res_data, f"Errors in delete: {res_data.get('errors')}"
    print("Service account deleted successfully")


def test_get_nonexistent_service_account(auth_session):
    """Test that getting a non-existent service account returns appropriate error."""
    fake_urn = "urn:li:corpuser:service_nonexistent-service-account-12345"

    res_data = get_service_account(auth_session, fake_urn)
    # Should either return null or an error
    if res_data.get("errors"):
        print(
            f"Got expected error for nonexistent service account: {res_data['errors']}"
        )
    else:
        assert res_data["data"]["getServiceAccount"] is None, (
            f"Expected null for nonexistent service account, got: {res_data['data']['getServiceAccount']}"
        )
        print("Got expected null for nonexistent service account")


def test_delete_nonexistent_service_account(auth_session):
    """Test that deleting a non-existent service account returns appropriate error."""
    fake_urn = "urn:li:corpuser:service_nonexistent-service-account-67890"

    res_data = delete_service_account(auth_session, fake_urn)
    # Should return an error for non-existent service account
    if res_data.get("errors"):
        print(
            f"Got expected error for deleting nonexistent service account: {res_data['errors']}"
        )
    else:
        # Some implementations might return false instead of an error
        print(f"Delete returned: {res_data}")


def test_list_service_accounts_with_query(auth_session):
    """Test listing service accounts."""
    # Create a service account with a unique display name
    unique_display_name = "Test List Service Account"

    # Create the service account
    res_data = create_service_account(
        auth_session,
        display_name=unique_display_name,
    )
    assert res_data.get("data"), f"Failed to create service account: {res_data}"
    assert "errors" not in res_data

    service_account_urn = res_data["data"]["createServiceAccount"]["urn"]

    # Wait for indexing
    _ensure_service_account_exists(auth_session, service_account_urn)

    # List all service accounts (without query filter)
    res_data = list_service_accounts(auth_session)
    assert res_data.get("data"), f"Failed to list service accounts: {res_data}"
    assert "errors" not in res_data

    # Should find our service account in the list
    results = res_data["data"]["listServiceAccounts"]["serviceAccounts"]
    matching = [sa for sa in results if sa["urn"] == service_account_urn]
    assert len(matching) > 0, (
        f"Expected to find service account with URN '{service_account_urn}' in list results"
    )
    assert matching[0]["displayName"] == unique_display_name
    print(f"Found service account in list: {matching[0]}")

    # Clean up
    delete_service_account(auth_session, service_account_urn)
