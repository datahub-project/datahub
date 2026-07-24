"""
Unit tests for CorpGroup ownership behavior (Issue #14758).

Tests that group upsert is additive for owners, consistent with members.
"""

from unittest.mock import MagicMock

import pytest

from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)
from datahub.configuration.common import ConfigurationError
from datahub.metadata.schema_classes import (
    GroupMembershipClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class TestCorpGroupOwnershipAdditive:
    """Test that group ownership upsert is additive (Issue #14758)"""

    def test_upsert_owners_additive(self) -> None:
        """Test that upserting owners adds to existing, doesn't replace"""
        # Setup: Group already has bob and alice as owners
        mock_graph = MagicMock()
        existing_ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:bob",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                ),
                OwnerClass(
                    owner="urn:li:corpuser:alice",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                ),
            ]
        )

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == OwnershipClass and urn == "urn:li:corpGroup:test_group":
                return existing_ownership
            elif aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Upsert with only charlie as owner
        group = CorpGroup(id="test_group", owners=["charlie"], members=[])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: All three owners should be present (additive)
        ownership_mcp = next(
            (mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)), None
        )
        assert ownership_mcp is not None
        assert isinstance(ownership_mcp.aspect, OwnershipClass)
        owner_urns = {owner.owner for owner in ownership_mcp.aspect.owners}
        assert owner_urns == {
            "urn:li:corpuser:bob",
            "urn:li:corpuser:alice",
            "urn:li:corpuser:charlie",
        }

    def test_upsert_owners_dedup_same_owner(self) -> None:
        """Test that upserting same owner twice doesn't duplicate"""
        # Setup: Group already has bob as owner
        mock_graph = MagicMock()
        existing_ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:bob",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        )

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == OwnershipClass and urn == "urn:li:corpGroup:test_group":
                return existing_ownership
            elif aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Upsert with bob again
        group = CorpGroup(id="test_group", owners=["bob"], members=[])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: Only one bob entry
        ownership_mcp = next(
            (mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)), None
        )
        assert ownership_mcp is not None
        assert isinstance(ownership_mcp.aspect, OwnershipClass)
        assert len(ownership_mcp.aspect.owners) == 1
        assert ownership_mcp.aspect.owners[0].owner == "urn:li:corpuser:bob"

    def test_upsert_owners_no_existing(self) -> None:
        """Test that upserting owners works when no existing owners"""
        # Setup: Group has no existing owners
        mock_graph = MagicMock()

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Upsert with bob as owner
        group = CorpGroup(id="test_group", owners=["bob"], members=[])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: Bob should be the only owner
        ownership_mcp = next(
            (mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)), None
        )
        assert ownership_mcp is not None
        assert isinstance(ownership_mcp.aspect, OwnershipClass)
        assert len(ownership_mcp.aspect.owners) == 1
        assert ownership_mcp.aspect.owners[0].owner == "urn:li:corpuser:bob"

    def test_upsert_owners_empty_list(self) -> None:
        """Test that upserting with empty owners list doesn't emit ownership MCP"""
        # Setup: Group has existing owners
        mock_graph = MagicMock()
        existing_ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:bob",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        )

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == OwnershipClass and urn == "urn:li:corpGroup:test_group":
                return existing_ownership
            elif aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Upsert with empty owners list
        group = CorpGroup(id="test_group", owners=[], members=[])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: No ownership MCP should be emitted
        ownership_mcp = next(
            (mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)), None
        )
        assert ownership_mcp is None

    def test_upsert_owners_no_graph_raises_error(self):
        """Test that error is raised when graph is not provided but owners exist"""
        # Action: Try to upsert with owners but no graph
        group = CorpGroup(id="test_group", owners=["bob"], members=[])
        config = CorpGroupGenerationConfig(override_editable=False, datahub_graph=None)

        # Assert: Should raise ConfigurationError
        with pytest.raises(ConfigurationError) as exc_info:
            list(group.generate_mcp(generation_config=config))

        assert "Unable to emit ownership" in str(exc_info.value)
        assert "DataHubGraph instance was not provided" in str(exc_info.value)

    def test_upsert_members_still_additive(self) -> None:
        """Test that member behavior is unchanged after ownership fix"""
        # Setup: User joe is already in another group
        mock_graph = MagicMock()
        existing_membership = GroupMembershipClass(
            groups=["urn:li:corpGroup:other_group"]
        )

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == GroupMembershipClass and urn == "urn:li:corpuser:joe":
                return existing_membership
            elif aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Add joe to test_group
        group = CorpGroup(id="test_group", owners=[], members=["joe"])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: Joe should be in both groups (additive)
        joe_membership_mcp = next(
            (
                mcp
                for mcp in mcps
                if isinstance(mcp.aspect, GroupMembershipClass)
                and mcp.entityUrn == "urn:li:corpuser:joe"
            ),
            None,
        )
        assert joe_membership_mcp is not None
        assert isinstance(joe_membership_mcp.aspect, GroupMembershipClass)
        assert "urn:li:corpGroup:other_group" in joe_membership_mcp.aspect.groups
        assert "urn:li:corpGroup:test_group" in joe_membership_mcp.aspect.groups

    def test_upsert_owners_and_members_together(self) -> None:
        """Test that both owners and members work additively together"""
        # Setup: Group has existing owners and members
        mock_graph = MagicMock()
        existing_ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:alice",
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        )
        existing_membership_joe = GroupMembershipClass(
            groups=["urn:li:corpGroup:other_group"]
        )

        def mock_get_aspect(urn: str, aspect_class: type) -> object:
            if aspect_class == OwnershipClass and urn == "urn:li:corpGroup:test_group":
                return existing_ownership
            elif aspect_class == GroupMembershipClass and urn == "urn:li:corpuser:joe":
                return existing_membership_joe
            elif aspect_class == GroupMembershipClass:
                return GroupMembershipClass(groups=[])
            return None

        mock_graph.get_aspect = mock_get_aspect

        # Action: Upsert with new owner (bob) and new member (joe)
        group = CorpGroup(id="test_group", owners=["bob"], members=["joe"])
        config = CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=mock_graph
        )
        mcps = list(group.generate_mcp(generation_config=config))

        # Assert: Both alice and bob should be owners
        ownership_mcp = next(
            (mcp for mcp in mcps if isinstance(mcp.aspect, OwnershipClass)), None
        )
        assert ownership_mcp is not None
        assert isinstance(ownership_mcp.aspect, OwnershipClass)
        owner_urns = {owner.owner for owner in ownership_mcp.aspect.owners}
        assert owner_urns == {"urn:li:corpuser:alice", "urn:li:corpuser:bob"}

        # Assert: Joe should be in both groups
        joe_membership_mcp = next(
            (
                mcp
                for mcp in mcps
                if isinstance(mcp.aspect, GroupMembershipClass)
                and mcp.entityUrn == "urn:li:corpuser:joe"
            ),
            None,
        )
        assert joe_membership_mcp is not None
        assert isinstance(joe_membership_mcp.aspect, GroupMembershipClass)
        assert "urn:li:corpGroup:other_group" in joe_membership_mcp.aspect.groups
        assert "urn:li:corpGroup:test_group" in joe_membership_mcp.aspect.groups
