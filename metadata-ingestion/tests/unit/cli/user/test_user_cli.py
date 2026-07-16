from unittest.mock import MagicMock

import pytest

from datahub.cli.specific.user_cli import (
    create_native_user_in_datahub,
    validate_user_id_options,
)
from datahub.configuration.common import OperationalError


class TestValidateUserIdOptions:
    def test_valid_with_user_id(self):
        """Test validation succeeds when --id is provided."""
        result = validate_user_id_options(
            user_id="jdoe", email_as_id=False, email="john@example.com"
        )
        assert result == "jdoe"

    def test_valid_with_email_as_id(self):
        """Test validation succeeds when --email-as-id is provided."""
        result = validate_user_id_options(
            user_id=None, email_as_id=True, email="john@example.com"
        )
        assert result == "john@example.com"

    def test_error_when_neither_provided(self):
        """Test validation fails when neither --id nor --email-as-id is provided."""
        with pytest.raises(ValueError) as exc_info:
            validate_user_id_options(
                user_id=None, email_as_id=False, email="john@example.com"
            )
        assert (
            "must specify either --id or --email-as-id" in str(exc_info.value).lower()
        )

    def test_error_when_both_provided(self):
        """Test validation fails when both --id and --email-as-id are provided."""
        with pytest.raises(ValueError) as exc_info:
            validate_user_id_options(
                user_id="jdoe", email_as_id=True, email="john@example.com"
            )
        assert "cannot specify both" in str(exc_info.value).lower()


class TestCreateNativeUserInDatahub:
    def test_successful_user_creation(self):
        """Test successful user creation without role."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_graph.create_native_user.return_value = "urn:li:corpuser:jdoe"

        result = create_native_user_in_datahub(
            graph=mock_graph,
            user_id="jdoe",
            email="john@example.com",
            display_name="John Doe",
            password="securepass123",
            role=None,
        )

        assert result == "urn:li:corpuser:jdoe"
        mock_graph.exists.assert_called_once_with("urn:li:corpuser:jdoe")
        mock_graph.create_native_user.assert_called_once_with(
            user_id="jdoe",
            email="john@example.com",
            display_name="John Doe",
            password="securepass123",
            role=None,
        )

    def test_successful_user_creation_with_role(self):
        """Test successful user creation with role."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_graph.create_native_user.return_value = "urn:li:corpuser:jdoe"

        result = create_native_user_in_datahub(
            graph=mock_graph,
            user_id="jdoe",
            email="john@example.com",
            display_name="John Doe",
            password="securepass123",
            role="Admin",
        )

        assert result == "urn:li:corpuser:jdoe"
        mock_graph.create_native_user.assert_called_once_with(
            user_id="jdoe",
            email="john@example.com",
            display_name="John Doe",
            password="securepass123",
            role="Admin",
        )

    def test_user_already_exists(self):
        """Test error when user already exists."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True

        with pytest.raises(ValueError) as exc_info:
            create_native_user_in_datahub(
                graph=mock_graph,
                user_id="jdoe",
                email="john@example.com",
                display_name="John Doe",
                password="securepass123",
                role=None,
            )

        assert "already exists" in str(exc_info.value).lower()
        assert "jdoe" in str(exc_info.value)
        mock_graph.create_native_user.assert_not_called()

    def test_invalid_role(self):
        """Test error handling for invalid role."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_graph.create_native_user.side_effect = ValueError(
            "Invalid role 'InvalidRole'"
        )

        with pytest.raises(ValueError) as exc_info:
            create_native_user_in_datahub(
                graph=mock_graph,
                user_id="jdoe",
                email="john@example.com",
                display_name="John Doe",
                password="securepass123",
                role="InvalidRole",
            )

        assert "invalid role" in str(exc_info.value).lower()

    def test_operational_error(self):
        """Test handling of OperationalError from graph client."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_graph.create_native_user.side_effect = OperationalError(
            "Failed to create user: Network error", {"status_code": 500}
        )

        with pytest.raises(OperationalError) as exc_info:
            create_native_user_in_datahub(
                graph=mock_graph,
                user_id="jdoe",
                email="john@example.com",
                display_name="John Doe",
                password="securepass123",
                role=None,
            )

        assert "network error" in str(exc_info.value).lower()

    def test_email_as_user_id(self):
        """Test user creation where email is used as user ID."""
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_graph.create_native_user.return_value = "urn:li:corpuser:john@example.com"

        result = create_native_user_in_datahub(
            graph=mock_graph,
            user_id="john@example.com",  # Email as ID
            email="john@example.com",
            display_name="John Doe",
            password="securepass123",
            role="Editor",
        )

        assert result == "urn:li:corpuser:john@example.com"
        mock_graph.exists.assert_called_once_with("urn:li:corpuser:john@example.com")
        mock_graph.create_native_user.assert_called_once()
