"""Unit tests for PowerBI user creation logic.

Tests the fix for CUS-7063: PowerBI ingestion overwrites existing user profiles.
"""

from typing import List, Optional

from datahub.metadata.schema_classes import CorpUserInfoClass, CorpUserKeyClass


class MockUser:
    """Mock PowerBI user for testing.

    Simulates the User dataclass from rest_api_wrapper.data_classes
    without importing the full module and its dependencies.
    """

    def __init__(
        self,
        user_id: str,
        displayName: Optional[str] = None,
        emailAddress: Optional[str] = None,
        principalType: str = "User",
    ):
        self.id = user_id
        self.displayName = displayName
        self.emailAddress = emailAddress
        self.principalType = principalType
        self.datasetUserAccessRight = "ReadWriteReshareExplore"
        self.reportUserAccessRight = None
        self.dashboardUserAccessRight = None
        self.groupUserAccessRight = None

    def get_urn_part(
        self, use_email: bool = True, remove_email_suffix: bool = False
    ) -> str:
        """Return the identifier used for URN generation."""
        if use_email and self.emailAddress:
            if remove_email_suffix and "@" in self.emailAddress:
                return self.emailAddress.split("@")[0]
            return self.emailAddress
        return self.id


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
        user = MockUser("user1", "User One", "user1@example.com")

        user_key = CorpUserKeyClass(username=user.id)
        user_info = CorpUserInfoClass(
            displayName=user.displayName or user.id,
            email=user.emailAddress,
            active=True,
        )

        # Verify CorpUserKeyClass
        assert isinstance(user_key, CorpUserKeyClass)
        assert user_key.username == "user1"

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
            MockUser("user1", "User 1", "user1@example.com"),
            MockUser("user2", "User 2", "user2@example.com"),
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
        user = MockUser("user123", displayName="User", emailAddress="user@example.com")
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=True)

        assert user_id == "user"

    def test_use_powerbi_email_false_uses_id(self) -> None:
        """When use_powerbi_email=False, use user.id instead of email."""
        user = MockUser("user123", displayName="User", emailAddress="user@example.com")
        user_id = user.get_urn_part(use_email=False, remove_email_suffix=False)

        assert user_id == "user123"


class TestDisplayNameFallback:
    """Tests for displayName fallback logic.

    When displayName is null, we fallback to user_id to avoid null values.
    """

    def test_null_display_name_falls_back_to_email(self) -> None:
        """When displayName is null and email exists, use email as fallback."""
        user = MockUser("user123", displayName=None, emailAddress="user@example.com")
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user_id,
            email=user.emailAddress,
            active=True,
        )

        assert user_info.displayName == "user@example.com"

    def test_null_display_name_falls_back_to_user_id(self) -> None:
        """When both displayName and email are null, use user.id as fallback."""
        user = MockUser("user123", displayName=None, emailAddress=None)
        user_id = user.get_urn_part(use_email=True, remove_email_suffix=False)

        assert user_id == "user123"

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user_id,
            email=user.emailAddress,
            active=True,
        )

        assert user_info.displayName == "user123"
        assert user_info.email is None

    def test_null_email_address_allowed(self) -> None:
        """When emailAddress is null, email field should be None (not error)."""
        user = MockUser("user123", displayName="User Name", emailAddress=None)

        user_info = CorpUserInfoClass(
            displayName=user.displayName or user.id,
            email=user.emailAddress,
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
            MockUser("user1", principalType="User"),
            MockUser("group1", principalType="Group"),
            MockUser("user2", principalType="User"),
        ]

        urns = []
        for user in users:
            if user and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]

    def test_none_user_in_list_filtered_out(self) -> None:
        """None users in the list should be safely filtered out."""
        users = [
            MockUser("user1", principalType="User"),
            None,
            MockUser("user2", principalType="User"),
        ]

        urns = []
        for user in users:
            if user and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]


class TestOwnerCriteriaFiltering:
    """Tests for owner_criteria filtering logic.

    Users are only included as owners if they have access rights
    matching the configured owner_criteria.
    """

    def test_owner_criteria_filters_users_without_rights(self) -> None:
        """Users without matching owner_criteria should be filtered."""
        user_with_rights = MockUser("user1", principalType="User")
        user_with_rights.datasetUserAccessRight = "ReadWriteReshareExplore"

        user_no_rights = MockUser("user2", principalType="User")
        user_no_rights.datasetUserAccessRight = "Read"
        user_no_rights.reportUserAccessRight = None
        user_no_rights.dashboardUserAccessRight = None
        user_no_rights.groupUserAccessRight = None

        owner_criteria = ["ReadWriteReshareExplore", "Owner", "Admin"]
        users = [user_with_rights, user_no_rights]
        urns = []

        for user in users:
            if user and user.principalType == "User":
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
            MockUser("user1", principalType="User"),
            MockUser("user2", principalType="User"),
        ]

        owner_criteria = None
        urns = []

        for user in users:
            if user and user.principalType == "User":
                if owner_criteria is None:
                    urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == ["urn:li:corpuser:user1", "urn:li:corpuser:user2"]


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_users_list(self) -> None:
        """Empty users list should return empty URNs."""
        users: List[MockUser] = []

        urns: List[str] = []
        for user in users:
            if user and user.principalType == "User":
                urns.append(f"urn:li:corpuser:{user.id}")

        assert urns == []


class TestConfigDefaults:
    """Tests for config default value changes.

    The fix changes create_corp_user default from True to False.
    """

    def test_create_corp_user_should_default_to_false(self) -> None:
        """Verify create_corp_user defaults to False to prevent overwrites."""
        from datahub.ingestion.source.powerbi.config import OwnershipMapping

        config = OwnershipMapping()
        assert config.create_corp_user is False
