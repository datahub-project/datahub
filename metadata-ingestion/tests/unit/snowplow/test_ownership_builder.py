"""Unit tests for OwnershipBuilder service."""

from typing import Dict, List

import pytest

from datahub.ingestion.source.snowplow.builders.ownership_builder import (
    OwnershipBuilder,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataStructureDeployment,
    User,
)
from datahub.ingestion.source.snowplow.snowplow_config import (
    SnowplowBDPConnectionConfig,
    SnowplowSourceConfig,
)
from datahub.metadata.schema_classes import (
    OwnershipClass,
    OwnershipTypeClass,
)


class TestOwnershipBuilderUserResolution:
    """Tests for _resolve_user_email method."""

    @pytest.fixture
    def config(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="key-id",
                api_key="secret",
            ),
        )

    @pytest.fixture
    def sample_users(self) -> List[User]:
        """Create sample users for testing."""
        return [
            User(id="user-1", name="Alice Smith", email="alice@example.com"),
            User(id="user-2", name="Bob Jones", email="bob@example.com"),
            User(id="user-3", name="Charlie Brown", email="charlie@example.com"),
            User(
                id="user-4", name="Alice Smith", email="alice2@other.com"
            ),  # Duplicate name!
        ]

    @pytest.fixture
    def user_cache(self, sample_users: List[User]) -> Dict[str, User]:
        """Create user cache by ID."""
        return {u.id: u for u in sample_users if u.id}

    @pytest.fixture
    def user_name_cache(self, sample_users: List[User]) -> Dict[str, List[User]]:
        """Create user cache by name."""
        cache: Dict[str, List[User]] = {}
        for user in sample_users:
            if user.name:
                if user.name not in cache:
                    cache[user.name] = []
                cache[user.name].append(user)
        return cache

    def test_resolve_by_id_returns_email(self, config, user_cache, user_name_cache):
        """Test that user is resolved by ID and email is returned."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        result = builder._resolve_user_email("user-1", "Alice Smith")

        assert result == "alice@example.com"

    def test_resolve_by_id_returns_name_when_no_email(self, config):
        """Test that name is returned when user has no email."""
        user_no_email = User(id="user-5", name="Dave Nomail", email=None)
        user_cache = {"user-5": user_no_email}
        builder = OwnershipBuilder(config, user_cache, {})

        result = builder._resolve_user_email("user-5", "Dave Nomail")

        assert result == "Dave Nomail"

    def test_resolve_by_name_single_match(self, config, user_cache, user_name_cache):
        """Test that single name match resolves to email."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        # Bob Jones has unique name
        result = builder._resolve_user_email(None, "Bob Jones")

        assert result == "bob@example.com"

    def test_resolve_by_name_ambiguous_returns_name(
        self, config, user_cache, user_name_cache
    ):
        """Test that ambiguous name match returns the name directly."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        # Alice Smith has multiple users
        result = builder._resolve_user_email(None, "Alice Smith")

        # Should return the name directly due to ambiguity
        assert result == "Alice Smith"

    def test_resolve_unknown_id_falls_back_to_name(
        self, config, user_cache, user_name_cache
    ):
        """Test that unknown ID falls back to name resolution."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        result = builder._resolve_user_email("unknown-id", "Bob Jones")

        # Falls back to name resolution
        assert result == "bob@example.com"

    def test_resolve_unknown_user_returns_name_directly(
        self, config, user_cache, user_name_cache
    ):
        """Test that completely unknown user returns name directly."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        result = builder._resolve_user_email("unknown-id", "Unknown User")

        # Returns name directly as last resort
        assert result == "Unknown User"

    def test_resolve_with_no_info_returns_none(
        self, config, user_cache, user_name_cache
    ):
        """Test that None is returned when no info provided."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        result = builder._resolve_user_email(None, None)

        assert result is None

    def test_resolve_id_takes_precedence_over_name(
        self, config, user_cache, user_name_cache
    ):
        """Test that ID resolution takes precedence over name lookup."""
        builder = OwnershipBuilder(config, user_cache, user_name_cache)

        # ID points to user-1 (alice@example.com), but name is different
        result = builder._resolve_user_email("user-1", "Bob Jones")

        # Should use ID resolution, not name
        assert result == "alice@example.com"

    def test_resolve_empty_caches(self, config):
        """Test resolution works with empty caches."""
        builder = OwnershipBuilder(config, {}, {})

        result = builder._resolve_user_email("user-1", "Some User")

        # Falls back to name
        assert result == "Some User"


class TestOwnershipBuilderExtractFromDeployments:
    """Tests for extract_ownership_from_deployments method."""

    @pytest.fixture
    def config(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="key-id",
                api_key="secret",
            ),
        )

    @pytest.fixture
    def user_cache(self) -> Dict[str, User]:
        """Create user cache."""
        return {
            "creator-id": User(
                id="creator-id", name="Creator", email="creator@example.com"
            ),
            "modifier-id": User(
                id="modifier-id", name="Modifier", email="modifier@example.com"
            ),
        }

    def test_empty_deployments_returns_none(self, config, user_cache):
        """Test that empty deployments list returns (None, None)."""
        builder = OwnershipBuilder(config, user_cache, {})

        created_by, modified_by = builder.extract_ownership_from_deployments([])

        assert created_by is None
        assert modified_by is None

    def test_extracts_creator_from_oldest_deployment(self, config, user_cache):
        """Test that creator is extracted from oldest deployment."""
        deployments = [
            DataStructureDeployment(
                version="1-0-0",
                env="PROD",
                ts="2024-01-15T10:00:00Z",
                initiator_id="creator-id",
                initiator="Creator",
            ),
            DataStructureDeployment(
                version="1-0-1",
                env="PROD",
                ts="2024-01-20T10:00:00Z",
                initiator_id="modifier-id",
                initiator="Modifier",
            ),
        ]
        builder = OwnershipBuilder(config, user_cache, {})

        created_by, _ = builder.extract_ownership_from_deployments(deployments)

        assert created_by == "creator@example.com"

    def test_extracts_modifier_from_newest_deployment(self, config, user_cache):
        """Test that modifier is extracted from newest deployment."""
        deployments = [
            DataStructureDeployment(
                version="1-0-0",
                env="PROD",
                ts="2024-01-15T10:00:00Z",
                initiator_id="creator-id",
                initiator="Creator",
            ),
            DataStructureDeployment(
                version="1-0-1",
                env="PROD",
                ts="2024-01-20T10:00:00Z",
                initiator_id="modifier-id",
                initiator="Modifier",
            ),
        ]
        builder = OwnershipBuilder(config, user_cache, {})

        _, modified_by = builder.extract_ownership_from_deployments(deployments)

        assert modified_by == "modifier@example.com"

    def test_single_deployment_same_creator_and_modifier(self, config, user_cache):
        """Test that single deployment returns same creator and modifier."""
        deployments = [
            DataStructureDeployment(
                version="1-0-0",
                env="PROD",
                ts="2024-01-15T10:00:00Z",
                initiator_id="creator-id",
                initiator="Creator",
            ),
        ]
        builder = OwnershipBuilder(config, user_cache, {})

        created_by, modified_by = builder.extract_ownership_from_deployments(
            deployments
        )

        # Both should be the same for single deployment
        assert created_by == "creator@example.com"
        assert modified_by == "creator@example.com"

    def test_deployments_sorted_by_timestamp(self, config, user_cache):
        """Test that deployments are sorted by timestamp regardless of input order."""
        deployments = [
            DataStructureDeployment(
                version="1-0-1",
                env="PROD",
                ts="2024-01-20T10:00:00Z",  # Newer
                initiator_id="modifier-id",
                initiator="Modifier",
            ),
            DataStructureDeployment(
                version="1-0-0",
                env="PROD",
                ts="2024-01-15T10:00:00Z",  # Older
                initiator_id="creator-id",
                initiator="Creator",
            ),
        ]
        builder = OwnershipBuilder(config, user_cache, {})

        created_by, modified_by = builder.extract_ownership_from_deployments(
            deployments
        )

        # Should correctly identify oldest and newest regardless of input order
        assert created_by == "creator@example.com"
        assert modified_by == "modifier@example.com"

    def test_deployments_with_missing_timestamp(self, config, user_cache):
        """Test handling of deployments with missing timestamps."""
        deployments = [
            DataStructureDeployment(
                version="1-0-0",
                env="PROD",
                ts=None,  # Missing timestamp
                initiator_id="creator-id",
                initiator="Creator",
            ),
            DataStructureDeployment(
                version="1-0-1",
                env="PROD",
                ts="2024-01-20T10:00:00Z",
                initiator_id="modifier-id",
                initiator="Modifier",
            ),
        ]
        builder = OwnershipBuilder(config, user_cache, {})

        # Should not raise - missing ts is treated as empty string
        created_by, modified_by = builder.extract_ownership_from_deployments(
            deployments
        )

        # With empty string, the None ts deployment sorts first
        assert created_by == "creator@example.com"


class TestOwnershipBuilderBuildOwnershipList:
    """Tests for build_ownership_list method."""

    @pytest.fixture
    def config(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="key-id",
                api_key="secret",
            ),
        )

    def test_creates_dataowner_for_creator(self, config):
        """Test that DATAOWNER is created for creator."""
        builder = OwnershipBuilder(config, {}, {})

        owners = builder.build_ownership_list(
            created_by="creator@example.com",
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert owners is not None
        assert len(owners) == 1
        assert owners[0].type == OwnershipTypeClass.DATAOWNER
        assert "creator@example.com" in owners[0].owner

    def test_creates_producer_for_modifier(self, config):
        """Test that PRODUCER is created for modifier different from creator."""
        builder = OwnershipBuilder(config, {}, {})

        owners = builder.build_ownership_list(
            created_by="creator@example.com",
            modified_by="modifier@example.com",
            schema_identifier="com.acme/event",
        )

        assert owners is not None
        assert len(owners) == 2

        # Find owner types
        owner_types = {o.type for o in owners}
        assert OwnershipTypeClass.DATAOWNER in owner_types
        assert OwnershipTypeClass.PRODUCER in owner_types

    def test_no_producer_when_same_as_creator(self, config):
        """Test that no PRODUCER is created when modifier is same as creator."""
        builder = OwnershipBuilder(config, {}, {})

        owners = builder.build_ownership_list(
            created_by="same@example.com",
            modified_by="same@example.com",
            schema_identifier="com.acme/event",
        )

        assert owners is not None
        assert len(owners) == 1
        assert owners[0].type == OwnershipTypeClass.DATAOWNER

    def test_returns_none_when_no_owners(self, config):
        """Test that None is returned when no owner info available."""
        builder = OwnershipBuilder(config, {}, {})

        owners = builder.build_ownership_list(
            created_by=None,
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert owners is None

    def test_ownership_source_includes_bdp_url(self, config):
        """Test that ownership source includes BDP URL."""
        builder = OwnershipBuilder(config, {}, {})

        owners = builder.build_ownership_list(
            created_by="owner@example.com",
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert owners is not None
        assert owners[0].source is not None
        assert owners[0].source.url is not None
        assert "console.snowplowanalytics.com" in owners[0].source.url


class TestOwnershipBuilderEmitOwnership:
    """Tests for emit_ownership method."""

    @pytest.fixture
    def config(self):
        """Create config with BDP connection."""
        return SnowplowSourceConfig(
            bdp_connection=SnowplowBDPConnectionConfig(
                organization_id="test-org",
                api_key_id="key-id",
                api_key="secret",
            ),
        )

    def test_emits_workunit_with_ownership(self, config):
        """Test that MetadataWorkUnit with ownership is emitted."""
        builder = OwnershipBuilder(config, {}, {})

        workunit = builder.emit_ownership(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event,PROD)",
            created_by="owner@example.com",
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert workunit is not None
        assert hasattr(workunit, "metadata")

    def test_returns_none_when_no_ownership(self, config):
        """Test that None is returned when no ownership data."""
        builder = OwnershipBuilder(config, {}, {})

        workunit = builder.emit_ownership(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event,PROD)",
            created_by=None,
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert workunit is None

    def test_workunit_contains_ownership_aspect(self, config):
        """Test that workunit contains OwnershipClass aspect."""
        builder = OwnershipBuilder(config, {}, {})

        workunit = builder.emit_ownership(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event,PROD)",
            created_by="owner@example.com",
            modified_by="modifier@example.com",
            schema_identifier="com.acme/event",
        )

        assert workunit is not None
        assert hasattr(workunit.metadata, "aspect")
        assert isinstance(workunit.metadata.aspect, OwnershipClass)

    def test_ownership_aspect_has_correct_owners(self, config):
        """Test that ownership aspect has correct owners."""
        builder = OwnershipBuilder(config, {}, {})

        workunit = builder.emit_ownership(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event,PROD)",
            created_by="owner@example.com",
            modified_by="modifier@example.com",
            schema_identifier="com.acme/event",
        )

        assert workunit is not None
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        assert isinstance(workunit.metadata, MetadataChangeProposalWrapper)
        assert isinstance(workunit.metadata.aspect, OwnershipClass)
        aspect = workunit.metadata.aspect
        assert aspect.owners is not None
        assert len(aspect.owners) == 2

        # Check both owners are present
        owner_urns = [o.owner for o in aspect.owners]
        assert any("owner@example.com" in urn for urn in owner_urns)
        assert any("modifier@example.com" in urn for urn in owner_urns)


class TestOwnershipBuilderWithoutBDP:
    """Tests for OwnershipBuilder without BDP connection."""

    @pytest.fixture
    def config_no_bdp(self):
        """Create config without BDP connection."""
        from datahub.ingestion.source.snowplow.snowplow_config import (
            IgluConnectionConfig,
        )

        return SnowplowSourceConfig(
            iglu_connection=IgluConnectionConfig(
                iglu_server_url="https://iglu.example.com",
            ),
        )

    def test_ownership_source_url_is_none_without_bdp(self, config_no_bdp):
        """Test that ownership source URL is None without BDP connection."""
        builder = OwnershipBuilder(config_no_bdp, {}, {})

        url = builder._get_schema_url_for_ownership("com.acme/event")

        assert url is None

    def test_ownership_still_works_without_source_url(self, config_no_bdp):
        """Test that ownership building works even without source URL."""
        builder = OwnershipBuilder(config_no_bdp, {}, {})

        owners = builder.build_ownership_list(
            created_by="owner@example.com",
            modified_by=None,
            schema_identifier="com.acme/event",
        )

        assert owners is not None
        assert len(owners) == 1
        # Source URL should be None
        assert owners[0].source is not None
        assert owners[0].source.url is None
