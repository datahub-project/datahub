import logging
import uuid
from typing import Any, List

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import run_datahub_cmd

logger = logging.getLogger(__name__)


def generate_test_email() -> str:
    """Generate a unique email for testing to avoid conflicts."""
    return f"test-user-{uuid.uuid4()}@example.com"


def datahub_user_add(
    auth_session: Any,
    email: str,
    display_name: str,
    password: str,
    role: str | None = None,
    user_id: str | None = None,
    email_as_id: bool = False,
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

    if user_id:
        add_args.extend(["--id", user_id])
    elif email_as_id:
        add_args.append("--email-as-id")

    if role:
        add_args.extend(["--role", role])

    result = run_datahub_cmd(
        add_args,
        input=f"{password}\n{password}\n",
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )
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
            logger.warning(f"Failed to clean up user {user_urn}: {e}")

    wait_for_writes_to_sync()


def test_user_add_without_role(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user without specifying a role."""
    email = generate_test_email()
    display_name = "Test User No Role"
    password = "testpassword123"

    test_users_cleanup(email)

    result = datahub_user_add(
        auth_session, email, display_name, password, email_as_id=True
    )

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

    result = datahub_user_add(
        auth_session, email, display_name, password, role="Admin", email_as_id=True
    )

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
        auth_session, email, display_name, password, role="editor", email_as_id=True
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
        auth_session, email, display_name, password, role="READER", email_as_id=True
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

    first_result = datahub_user_add(
        auth_session, email, display_name, password, email_as_id=True
    )
    assert first_result.exit_code == 0

    wait_for_writes_to_sync()

    second_result = datahub_user_add(
        auth_session, email, display_name, password, email_as_id=True
    )
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
        "--email-as-id",
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


def test_user_add_with_explicit_id(auth_session: Any, test_users_cleanup: Any) -> None:
    """Test creating a user with explicit --id option."""
    email = generate_test_email()
    user_id = f"testuser_{uuid.uuid4().hex[:8]}"
    display_name = "Test User With ID"
    password = "testpassword123"

    # Cleanup with the user ID, not email
    test_users_cleanup(user_id)

    result = datahub_user_add(
        auth_session, email, display_name, password, user_id=user_id
    )

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert user_id in result.output
    assert "URN:" in result.output

    wait_for_writes_to_sync()


def test_user_add_without_id_or_flag(auth_session: Any) -> None:
    """Test that not providing --id or --email-as-id results in error."""
    email = generate_test_email()
    display_name = "Test User No ID"

    add_args: List[str] = [
        "user",
        "add",
        "--email",
        email,
        "--display-name",
        display_name,
        "--password",
    ]

    result = run_datahub_cmd(
        add_args,
        input="password123\npassword123\n",
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    assert result.exit_code == 1
    assert "must specify either --id or --email-as-id" in result.output.lower()


def test_user_add_with_both_id_and_flag(auth_session: Any) -> None:
    """Test that providing both --id and --email-as-id results in error."""
    email = generate_test_email()
    display_name = "Test User Both Options"

    add_args: List[str] = [
        "user",
        "add",
        "--email",
        email,
        "--id",
        "testuser",
        "--email-as-id",
        "--display-name",
        display_name,
        "--password",
    ]

    result = run_datahub_cmd(
        add_args,
        input="password123\npassword123\n",
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    assert result.exit_code == 1
    assert "cannot specify both --id and --email-as-id" in result.output.lower()


def test_user_add_id_different_from_email(
    auth_session: Any, test_users_cleanup: Any
) -> None:
    """Test creating a user where ID is different from email."""
    email = "john.doe@company.com"
    user_id = "jdoe"
    display_name = "John Doe"
    password = "securepass123"

    test_users_cleanup(user_id)

    result = datahub_user_add(
        auth_session, email, display_name, password, user_id=user_id, role="Editor"
    )

    assert result.exit_code == 0
    assert "Successfully created user" in result.output
    assert user_id in result.output
    assert "Editor" in result.output
    assert f"urn:li:corpuser:{user_id}" in result.output

    wait_for_writes_to_sync()
