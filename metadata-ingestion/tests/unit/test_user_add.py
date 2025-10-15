from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_success(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "test@example.com"
    display_name = "Test User"
    password = "testpassword123"

    with (
        patch.object(graph, "execute_graphql") as mock_graphql,
        patch.object(graph._session, "post") as mock_session_post,
    ):
        mock_graphql.return_value = {
            "getInviteToken": {"inviteToken": "test-token-123"}
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session_post.return_value = mock_response

        user_urn = graph.create_native_user(
            email=email, display_name=display_name, password=password
        )

        assert user_urn == f"urn:li:corpuser:{email}"
        assert mock_graphql.call_count == 1
        assert mock_session_post.call_count == 1

        signup_call_args = mock_session_post.call_args
        assert signup_call_args[1]["json"]["email"] == email
        assert signup_call_args[1]["json"]["fullName"] == display_name
        assert signup_call_args[1]["json"]["password"] == password
        assert signup_call_args[1]["json"]["inviteToken"] == "test-token-123"


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_with_role(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "admin@example.com"
    display_name = "Admin User"
    password = "adminpass123"
    role = "admin"

    with (
        patch.object(graph, "execute_graphql") as mock_graphql,
        patch.object(graph._session, "post") as mock_session_post,
    ):
        mock_graphql.side_effect = [
            {"getInviteToken": {"inviteToken": "test-token-123"}},
            {"batchAssignRole": True},
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session_post.return_value = mock_response

        user_urn = graph.create_native_user(
            email=email, display_name=display_name, password=password, role=role
        )

        assert user_urn == f"urn:li:corpuser:{email}"
        assert mock_graphql.call_count == 2
        assert mock_session_post.call_count == 1

        role_call = mock_graphql.call_args_list[1]
        assert (
            role_call[1]["variables"]["input"]["roleUrn"] == "urn:li:dataHubRole:Admin"
        )
        assert role_call[1]["variables"]["input"]["actors"] == [user_urn]


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_role_normalization(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "editor@example.com"
    display_name = "Editor User"
    password = "editorpass123"

    test_cases = [
        ("ADMIN", "Admin"),
        ("admin", "Admin"),
        ("Admin", "Admin"),
        ("EDITOR", "Editor"),
        ("editor", "Editor"),
        ("READER", "Reader"),
        ("reader", "Reader"),
    ]

    for input_role, expected_role in test_cases:
        with (
            patch.object(graph, "execute_graphql") as mock_graphql,
            patch.object(graph._session, "post") as mock_session_post,
        ):
            mock_graphql.side_effect = [
                {"getInviteToken": {"inviteToken": "test-token-123"}},
                {"batchAssignRole": True},
            ]
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = ""
            mock_session_post.return_value = mock_response

            graph.create_native_user(
                email=email,
                display_name=display_name,
                password=password,
                role=input_role,
            )

            role_call = mock_graphql.call_args_list[1]
            assert (
                role_call[1]["variables"]["input"]["roleUrn"]
                == f"urn:li:dataHubRole:{expected_role}"
            )


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_invalid_role(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "test@example.com"
    display_name = "Test User"
    password = "testpass123"
    invalid_role = "InvalidRole"

    with (
        patch.object(graph, "execute_graphql") as mock_graphql,
        patch.object(graph._session, "post") as mock_session_post,
    ):
        mock_graphql.return_value = {
            "getInviteToken": {"inviteToken": "test-token-123"}
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session_post.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid role"):
            graph.create_native_user(
                email=email,
                display_name=display_name,
                password=password,
                role=invalid_role,
            )


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_no_invite_token(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "test@example.com"
    display_name = "Test User"
    password = "testpass123"

    with patch.object(graph, "execute_graphql") as mock_graphql:
        mock_graphql.return_value = {"getInviteToken": {}}

        with pytest.raises(OperationalError, match="invite token"):
            graph.create_native_user(
                email=email, display_name=display_name, password=password
            )


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_signup_failure(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "test@example.com"
    display_name = "Test User"
    password = "testpass123"

    with (
        patch.object(graph, "execute_graphql") as mock_graphql,
        patch.object(graph._session, "post") as mock_session_post,
    ):
        mock_graphql.return_value = {
            "getInviteToken": {"inviteToken": "test-token-123"}
        }
        mock_session_post.side_effect = Exception("Backend error")

        with pytest.raises(OperationalError, match="Failed to create user"):
            graph.create_native_user(
                email=email, display_name=display_name, password=password
            )


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_create_native_user_role_assignment_failure(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))

    email = "test@example.com"
    display_name = "Test User"
    password = "testpass123"
    role = "admin"

    with (
        patch.object(graph, "execute_graphql") as mock_graphql,
        patch.object(graph._session, "post") as mock_session_post,
        patch("datahub.ingestion.graph.client.logger") as mock_logger,
    ):
        mock_graphql.side_effect = [
            {"getInviteToken": {"inviteToken": "test-token-123"}},
            Exception("Role assignment failed"),
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session_post.return_value = mock_response

        user_urn = graph.create_native_user(
            email=email, display_name=display_name, password=password, role=role
        )

        assert user_urn == f"urn:li:corpuser:{email}"
        # Should be called twice: once in _assign_role_to_user and once in create_native_user
        assert mock_logger.warning.call_count == 2
        warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("role assignment failed" in msg.lower() for msg in warning_messages)
