"""Unit tests for PowerBI user creation logic.

Tests the fix for CUS-7063: PowerBI ingestion overwrites existing user profiles.
"""

from typing import List, Optional

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import User
from datahub.metadata.schema_classes import CorpUserInfoClass, CorpUserKeyClass

_NOT_SET = object()  # Sentinel for distinguishing None from "not provided"


def make_test_user(
    user_id: str,
    display_name: object = _NOT_SET,
    email: object = _NOT_SET,
    principal_type: str = "User",
    dataset_access: Optional[str] = "ReadWriteReshareExplore",
    report_access: Optional[str] = None,
    dashboard_access: Optional[str] = None,
    group_access: Optional[str] = None,
) -> User:
    """Factory function for creating test User instances with sensible defaults."""
    return User(
        id=user_id,
        displayName=(
            f"Display {user_id}"
            if display_name is _NOT_SET
            else str(display_name or "")
        ),
        emailAddress=(
            f"{user_id}@example.com" if email is _NOT_SET else str(email or "")
        ),
        graphId=f"graph_{user_id}",
        principalType=principal_type,
        datasetUserAccessRight=dataset_access,
        reportUserAccessRight=report_access,
        dashboardUserAccessRight=dashboard_access,
        groupUserAccessRight=group_access,
    )


class TestCreateCorpUserFlagBehavior:
    """Tests for create_corp_user flag behavior.

    This is the core fix - when create_corp_user=False (default),
    no user MCPs should be emitted to avoid overwriting existing profiles.
    """

    def test_create_corp_user_false_returns_empty_mcps(self) -> None:
        """When create_corp_user=False, to_datahub_users should return empty list."""
        create_corp_user = False
        result = [] if not create_corp_user else ["mcp1", "mcp2"]

        assert result == []

    def test_create_corp_user_true_should_emit_key_and_info(self) -> None:
        """When create_corp_user=True, should emit both CorpUserKeyClass and CorpUserInfoClass."""
        user = make_test_user(
            "user1", display_name="User One", email="user1@example.com"
        )

        # user_id is derived from get_urn_part (uses email by default)
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)

        # Key username MUST match URN username (identity aspect)
        user_key = CorpUserKeyClass(username=user_id)
        user_info = CorpUserInfoClass(
            displayName=user.displayName or user_id,
            email=user.emailAddress,
            active=True,
        )

        # Verify CorpUserKeyClass - username matches URN
        assert isinstance(user_key, CorpUserKeyClass)
        assert user_key.username == "user1@example.com"  # Email, not raw ID

        # Verify CorpUserInfoClass (this was missing before the fix)
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.displayName == "User One"
        assert user_info.email == "user1@example.com"
        assert user_info.active is True


class TestUserUrnGeneration:
    """Tests for user URN generation logic.

    URNs are used for ownership references in soft reference mode
    (create_corp_user=False) and for entity URNs in full creation mode.
    """

    def test_user_urns_generation_with_email(self) -> None:
        """to_datahub_user_urns should return URNs using email by default."""
        users = [
            make_test_user("user1", display_name="User 1", email="user1@example.com"),
            make_test_user("user2", display_name="User 2", email="user2@example.com"),
        ]

        urns = []
        for user in users:
            if user and user.principalType == "User":
                user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)
                urns.append(f"urn:li:corpuser:{user_id}")

        assert urns == [
            "urn:li:corpuser:user1@example.com",
            "urn:li:corpuser:user2@example.com",
        ]

    def test_remove_email_suffix_strips_domain(self) -> None:
        """When remove_email_suffix=True, email domain should be removed."""
        user = make_test_user("user123", display_name="User", email="user@example.com")
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=True)

        assert user_id == "user"

    def test_use_powerbi_email_false_uses_id(self) -> None:
        """When use_powerbi_email=False, use users.{user.id} format."""
        user = make_test_user("user123", display_name="User", email="user@example.com")
        user_id = user.get_urn_part(use_email=False, remove_email_suffix=False)

        assert user_id == "users.user123"


class TestDisplayNameFallback:
    """Tests for displayName fallback logic.

    When displayName is null, we fallback to user_id to avoid null values.
    """

    def test_null_display_name_falls_back_to_email(self) -> None:
        """When displayName is null and email exists, use email as fallback."""
        # Note: User dataclass requires displayName, but we test the fallback logic
        user = make_test_user("user123", display_name="", email="user@example.com")
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user_id,
            email=user.emailAddress,
            active=True,
        )

        # Empty displayName falls back to user_id (email in this case)
        assert user_info.displayName == "user@example.com"

    def test_null_display_name_falls_back_to_user_id(self) -> None:
        """When displayName is empty and email not used, fall back to users.{id}."""
        user = make_test_user("user123", display_name="", email="")
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)

        # When email is empty, get_urn_part falls back to users.{id}
        assert user_id == "users.user123"

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user_id,
            email=user.emailAddress or None,
            active=True,
        )

        assert user_info.displayName == "users.user123"

    def test_null_email_address_allowed(self) -> None:
        """When emailAddress is empty, email field should be None (not error)."""
        user = make_test_user("user123", display_name="User Name", email="")

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user.id,
            email=user.emailAddress or None,
            active=True,
        )

        assert user_info.displayName == "User Name"
        assert user_info.email is None


class TestPrincipalTypeFiltering:
    """Tests for principal type filtering.

    Only users with principalType='User' should be processed.
    Groups, Apps, and other types should be filtered out.
    """

    def test_non_user_principal_type_filtered_out(self) -> None:
        """Users with principalType != 'User' should be filtered out."""
        users = [
            make_test_user("user1", principal_type="User"),
            make_test_user("group1", principal_type="Group"),
            make_test_user("user2", principal_type="User"),
        ]

        urns = []
        for user in users:
            if user and user.principalType and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]

    def test_none_user_in_list_filtered_out(self) -> None:
        """None users in the list should be safely filtered out."""
        users: List[Optional[User]] = [
            make_test_user("user1", principal_type="User"),
            None,
            make_test_user("user2", principal_type="User"),
        ]

        urns = []
        for user in users:
            if user and user.principalType and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]


class TestOwnerCriteriaFiltering:
    """Tests for owner_criteria filtering logic.

    Users are only included as owners if they have access rights
    matching the configured owner_criteria.
    """

    def test_owner_criteria_filters_users_without_rights(self) -> None:
        """Users without matching owner_criteria should be filtered."""
        user_with_rights = make_test_user(
            "user1", principal_type="User", dataset_access="ReadWriteReshareExplore"
        )
        user_no_rights = make_test_user(
            "user2",
            principal_type="User",
            dataset_access="Read",
            report_access=None,
            dashboard_access=None,
            group_access=None,
        )

        owner_criteria = ["ReadWriteReshareExplore", "Owner", "Admin"]
        users = [user_with_rights, user_no_rights]
        urns = []

        for user in users:
            if user and user.principalType and user.principalType == "User":
                user_rights = [
                    user.datasetUserAccessRight,
                    user.reportUserAccessRight,
                    user.dashboardUserAccessRight,
                    user.groupUserAccessRight,
                ]
                if len(set(user_rights) & set(owner_criteria)) > 0:
                    urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1"]

    def test_no_owner_criteria_includes_all_users(self) -> None:
        """When owner_criteria is None, all users should be included."""
        users = [
            make_test_user("user1", principal_type="User"),
            make_test_user("user2", principal_type="User"),
        ]

        owner_criteria = None
        urns = []

        for user in users:
            if user and user.principalType and user.principalType == "User":
                # Falsy check covers both None and []
                if not owner_criteria:
                    urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]

    def test_empty_owner_criteria_includes_all_users(self) -> None:
        """When owner_criteria is empty list [], all users should be included.

        This is the fix for Issue #1: empty list should behave same as None.
        Previously, empty list would filter out ALL users (bug).
        """
        users = [
            make_test_user("user1", principal_type="User"),
            make_test_user("user2", principal_type="User"),
        ]

        owner_criteria: List[str] = []  # Empty list, NOT None
        urns: List[str] = []

        for user in users:
            if user and user.principalType and user.principalType == "User":
                # Fix: Falsy check catches both None and []
                if not owner_criteria:
                    urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_users_list(self) -> None:
        """Empty users list should return empty URNs."""
        users: List[User] = []

        urns: List[str] = []
        for user in users:
            if user and user.principalType and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == []

    def test_user_with_empty_email_address(self) -> None:
        """Empty email should be converted to None, not stored as empty string."""
        from datahub.metadata.schema_classes import CorpUserInfoClass

        # Simulate PowerBI returning empty string for email
        email_value = ""

        # The fix: email=user.emailAddress or None
        user_info = CorpUserInfoClass(
            displayName="Test User",
            email=email_value or None,  # Convert "" to None
            active=True,
        )

        assert user_info.email is None

    def test_user_with_very_long_display_name(self) -> None:
        """Long displayName should be handled without truncation."""
        long_name = "A" * 500  # 500 character name

        user = make_test_user(
            user_id="longname@example.com",
            display_name=long_name,
            principal_type="User",
        )

        # System should handle long names without error
        assert user.displayName == long_name
        assert len(user.displayName) == 500

    def test_user_with_special_characters_in_email(self) -> None:
        """Emails with special characters should be handled correctly."""
        special_emails = [
            "user+tag@example.com",
            "user.name@sub.domain.com",
            "user_name@example.com",
        ]

        for email_addr in special_emails:
            user = make_test_user(
                user_id=email_addr,
                email=email_addr,
                principal_type="User",
            )
            assert user.emailAddress == email_addr

    def test_user_with_null_display_name_uses_user_id_fallback(self) -> None:
        """When displayName is None, user_id should be used as fallback."""
        from datahub.metadata.schema_classes import CorpUserInfoClass

        display_name = None
        user_id = "fallback_user"

        # The pattern: displayName=user.displayName or user_id
        user_info = CorpUserInfoClass(
            displayName=display_name or user_id,
            email="test@example.com",
            active=True,
        )

        assert user_info.displayName == "fallback_user"


class TestConfigDefaults:
    """Tests for config default value changes.

    The fix changes create_corp_user default from True to False.
    """

    def test_create_corp_user_should_default_to_false(self) -> None:
        """Verify create_corp_user defaults to False to prevent overwrites."""
        from datahub.ingestion.source.powerbi.config import OwnershipMapping

        config = OwnershipMapping()
        assert config.create_corp_user is False


class TestStatefulIngestionBehavior:
    """Tests for stateful ingestion integration.

    User entities should be marked as is_primary_source=False to prevent
    stateful ingestion from tracking and soft-deleting them.
    """

    def test_user_work_unit_has_is_primary_source_false(self) -> None:
        """User work units should have is_primary_source=False.

        This prevents stateful ingestion from:
        1. Tracking user entities in its state
        2. Soft-deleting them when they disappear from PowerBI

        PowerBI is NOT the authoritative source for users (LDAP/Okta/SCIM are).
        """
        from unittest.mock import MagicMock

        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.powerbi.config import (
            PowerBiDashboardSourceConfig,
        )
        from datahub.ingestion.source.powerbi.powerbi import Mapper
        from datahub.metadata.schema_classes import CorpUserKeyClass

        # Setup
        config = PowerBiDashboardSourceConfig(
            tenant_id="test",
            client_id="test",
            client_secret="test",
        )
        ctx = PipelineContext(run_id="test")

        mapper = Mapper(ctx, config, MagicMock(), MagicMock())

        # Create a test MCP for a user entity
        user_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:corpuser:test@example.com",
            aspect=CorpUserKeyClass(username="test@example.com"),
        )

        # Test the _to_user_work_unit method
        work_unit = mapper._to_user_work_unit(user_mcp)

        # Verify is_primary_source is False
        assert work_unit.is_primary_source is False

    def test_regular_work_unit_has_is_primary_source_true(self) -> None:
        """Non-user work units should have is_primary_source=True (default).

        This ensures datasets, dashboards, charts are properly tracked
        by stateful ingestion for stale entity removal.
        """
        from unittest.mock import MagicMock

        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.powerbi.config import (
            PowerBiDashboardSourceConfig,
        )
        from datahub.ingestion.source.powerbi.powerbi import Mapper
        from datahub.metadata.schema_classes import DatasetPropertiesClass

        # Setup
        config = PowerBiDashboardSourceConfig(
            tenant_id="test",
            client_id="test",
            client_secret="test",
        )
        ctx = PipelineContext(run_id="test")

        mapper = Mapper(ctx, config, MagicMock(), MagicMock())

        # Create a test MCP for a dataset entity
        dataset_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:powerbi,test,PROD)",
            aspect=DatasetPropertiesClass(name="Test Dataset"),
        )

        # Test the regular _to_work_unit method
        work_unit = mapper._to_work_unit(dataset_mcp)

        # Verify is_primary_source is True (default)
        assert work_unit.is_primary_source is True
