import uuid
from typing import Any, List

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import run_datahub_cmd


def generate_test_email() -> str:
    """Generate a unique email for testing to avoid conflicts."""
    return f"test-user-{uuid.uuid4()}@example.com"


def datahub_user_add(
    auth_session: Any,
    email: str,
    display_name: str,
    password: str,
    role: str | None = None,
) -> Any:
    """Run the datahub user add command."""
    add_args: List[str] = [
        "user",
        "add",
        "--email",
        email,
        "--display-name",
        display_name,
        "--password",
    ]
    if role:
        add_args.extend(["--role", role])

    gms_url = auth_session.gms_url()
    gms_token = auth_session.gms_token()
    token_preview = f"{gms_token[:20]}..." if gms_token else "None"

    print(
        f"[DEBUG TEST] Running user add command with: "
        f"GMS_URL={gms_url}, TOKEN={token_preview}, email={email}"
    )

    result = run_datahub_cmd(
        add_args,
        input=f"{password}\n{password}\n",
        env={
            "DATAHUB_GMS_URL": gms_url,
            "DATAHUB_GMS_TOKEN": gms_token,
        },
    )

    if result.exit_code != 0:
        print(f"[DEBUG TEST] User add failed with exit code {result.exit_code}")
        print(f"[DEBUG TEST] Output: {result.output[:500]}")

    return result


@pytest.fixture
def test_users_cleanup(auth_session: Any, graph_client: DataHubGraph):
    """Fixture to track and clean up test users."""
    created_users: List[str] = []

    def add_user_to_cleanup(email: str):
        user_urn = f"urn:li:corpuser:{email}"
        created_users.append(user_urn)

    yield add_user_to_cleanup

    for user_urn in created_users:
        try:
            graph_client.hard_delete_entity(user_urn)
        except Exception as e:
            print(f"Failed to clean up user {user_urn}: {e}")

    wait_for_writes_to_sync()


def test_user_add_without_role(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user without specifying a role."""
    email = generate_test_email()
    display_name = "Test User No Role"
    password = "testpassword123"

    test_users_cleanup(email)

    result = datahub_user_add(auth_session, email, display_name, password)

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert email in result.output
    assert "URN:" in result.output

    wait_for_writes_to_sync()


def test_user_add_with_admin_role(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user with Admin role."""
    email = generate_test_email()
    display_name = "Test Admin User"
    password = "adminpass123"

    test_users_cleanup(email)

    result = datahub_user_add(auth_session, email, display_name, password, role="Admin")

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert email in result.output
    assert "Admin" in result.output
    assert "URN:" in result.output

    wait_for_writes_to_sync()


def test_user_add_with_editor_role(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user with Editor role."""
    email = generate_test_email()
    display_name = "Test Editor User"
    password = "editorpass123"

    test_users_cleanup(email)

    result = datahub_user_add(
        auth_session, email, display_name, password, role="editor"
    )

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert email in result.output
    assert "Editor" in result.output
    assert "URN:" in result.output

    wait_for_writes_to_sync()


def test_user_add_with_reader_role(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user with Reader role."""
    email = generate_test_email()
    display_name = "Test Reader User"
    password = "readerpass123"

    test_users_cleanup(email)

    result = datahub_user_add(
        auth_session, email, display_name, password, role="READER"
    )

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert email in result.output
    assert "Reader" in result.output
    assert "URN:" in result.output

    wait_for_writes_to_sync()


def test_user_add_duplicate_user(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test that creating a duplicate user fails gracefully."""
    email = generate_test_email()
    display_name = "Test Duplicate User"
    password = "duplicatepass123"

    test_users_cleanup(email)

    first_result = datahub_user_add(auth_session, email, display_name, password)
    assert first_result.exit_code == 0

    wait_for_writes_to_sync()

    second_result = datahub_user_add(auth_session, email, display_name, password)
    assert second_result.exit_code == 0
    assert "already exists" in second_result.output
    assert email in second_result.output


def test_user_add_without_password_flag(auth_session: Any) -> None:
    """Test that not providing --password flag results in error."""
    email = generate_test_email()
    display_name = "Test User No Password Flag"

    add_args: List[str] = [
        "user",
        "add",
        "--email",
        email,
        "--display-name",
        display_name,
    ]

    result = run_datahub_cmd(
        add_args,
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    assert result.exit_code == 1
    assert "password flag is required" in result.output.lower()
