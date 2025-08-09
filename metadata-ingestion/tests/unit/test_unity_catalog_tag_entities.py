from unittest.mock import Mock

import pytest

from datahub.api.entities.external.external_entities import LinkedResourceSet
from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
    UnityCatalogTagSyncContext,
    get_unity_catalog_tag_cache_info,
)
from datahub.metadata.urns import TagUrn
from datahub.utilities.urns.error import InvalidUrnError


class TestUnityCatalogTagSyncContext:
    def test_context_creation(self):
        """Test creating sync context."""
        context = UnityCatalogTagSyncContext()
        assert context.platform_instance is None

        context_with_instance = UnityCatalogTagSyncContext(platform_instance="prod")
        assert context_with_instance.platform_instance == "prod"


class TestUnityCatalogTagPlatformResourceId:
    def test_basic_properties(self):
        """Test basic properties and methods."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )

        assert tag_id.tag_key == "env"
        assert tag_id.tag_value == "prod"
        assert tag_id.platform_instance == "test"
        assert (
            UnityCatalogTagPlatformResourceId._RESOURCE_TYPE()
            == "UnityCatalogTagPlatformResource"
        )

    def test_equality(self):
        """Test equality for resource IDs."""
        id1 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        id2 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        id3 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="dev", platform_instance="test"
        )

        assert id1 == id2
        assert id1 != id3

    def test_platform_resource_key_conversion(self):
        """Test conversion to platform resource key."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="environment",
            tag_value="production",
            platform_instance="workspace1",
        )

        key = tag_id.to_platform_resource_key()
        assert key.platform == "databricks"
        assert key.resource_type == "UnityCatalogTagPlatformResource"
        assert key.primary_key == "environment:production"
        assert key.platform_instance == "workspace1"

    def test_platform_resource_key_with_none_value(self):
        """Test conversion when tag value is None."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="department", tag_value=None, platform_instance="workspace1"
        )

        key = tag_id.to_platform_resource_key()
        assert key.primary_key == "department:None"

    def test_from_datahub_tag(self):
        """Test creating from DataHub tag URN."""
        tag_urn = TagUrn.from_string("urn:li:tag:environment:staging")
        context = UnityCatalogTagSyncContext(platform_instance="workspace1")

        result = UnityCatalogTagPlatformResourceId.from_datahub_tag(tag_urn, context)

        assert result.tag_key == "environment"
        assert result.tag_value == "staging"
        assert result.platform_instance == "workspace1"
        assert result.exists_in_unity_catalog is False


class TestUnityCatalogTagPlatformResource:
    def test_platform_resource_creation(self):
        """Test creating Unity Catalog tag platform resource."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        urns = LinkedResourceSet(urns=["urn:li:tag:env:prod"])

        resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=urns,
            managed_by_datahub=True,
            allowed_values=["dev", "staging", "prod"],
        )

        assert resource.get_id() == tag_id
        assert resource.is_managed_by_datahub() is True
        assert resource.datahub_linked_resources() == urns
        assert resource.allowed_values == ["dev", "staging", "prod"]

    def test_as_platform_resource_conversion(self):
        """Test conversion to platform resource."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="team", tag_value="data", platform_instance="prod"
        )
        urns = LinkedResourceSet(urns=["urn:li:tag:team:data"])

        resource = UnityCatalogTagPlatformResource(
            id=tag_id, datahub_urns=urns, managed_by_datahub=False
        )

        platform_resource = resource.as_platform_resource()

        assert platform_resource.resource_info is not None
        assert (
            platform_resource.resource_info.resource_type
            == "UnityCatalogTagPlatformResource"
        )
        assert platform_resource.resource_info.primary_key == "team:data"
        assert platform_resource.resource_info.secondary_keys == [
            "urn:li:tag:team:data"
        ]


class TestIntegrationScenario:
    def test_complete_tag_workflow(self):
        """Test complete tag synchronization workflow with concrete objects."""
        unity_tag = UnityCatalogTag(key="environment", value="production")
        context = UnityCatalogTagSyncContext(platform_instance="prod-workspace")

        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key=str(unity_tag.key),
            tag_value=str(unity_tag.value) if unity_tag.value else None,
            platform_instance=context.platform_instance,
            exists_in_unity_catalog=False,
            persisted=False,
        )

        assert tag_id.tag_key == "environment"
        assert tag_id.tag_value == "production"
        assert tag_id.platform_instance == "prod-workspace"
        assert tag_id.exists_in_unity_catalog is False
        assert tag_id.persisted is False

        urns = LinkedResourceSet(urns=["urn:li:tag:environment:production"])
        platform_resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=urns,
            managed_by_datahub=True,
            allowed_values=["dev", "staging", "production"],
        )

        stored_resource = platform_resource.as_platform_resource()

        assert stored_resource.resource_info is not None
        assert (
            stored_resource.resource_info.resource_type
            == "UnityCatalogTagPlatformResource"
        )
        assert stored_resource.resource_info.primary_key == "environment:production"
        assert stored_resource.data_platform_instance is not None
        assert stored_resource.resource_info is not None
        assert (
            stored_resource.resource_info.resource_type
            == "UnityCatalogTagPlatformResource"
        )
        assert stored_resource.resource_info.primary_key == "environment:production"
        assert stored_resource.resource_info.secondary_keys == [
            "urn:li:tag:environment:production"
        ]
        assert stored_resource.resource_info.value is not None
        # The value is serialized, so we can check the resource info instead of direct comparison

    def test_tag_workflow_with_none_value(self) -> None:
        """Test workflow with tag that has None value."""
        unity_tag = UnityCatalogTag(key="flag", value=None)
        context = UnityCatalogTagSyncContext(platform_instance="workspace1")

        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key=str(unity_tag.key),
            tag_value=str(unity_tag.value) if unity_tag.value else None,
            platform_instance=context.platform_instance,
        )

        assert tag_id.tag_key == "flag"
        assert tag_id.tag_value is None
        assert tag_id.platform_instance == "workspace1"

        platform_resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
        )

        stored_resource = platform_resource.as_platform_resource()
        assert stored_resource.resource_info is not None
        assert stored_resource.resource_info.primary_key == "flag:None"

    def test_urn_lifecycle_management(self) -> None:
        """Test URN lifecycle in Unity Catalog tag resources."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="status", tag_value="active", platform_instance="workspace1"
        )

        # Start with empty URNs
        urns = LinkedResourceSet(urns=[])
        resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=urns,
            managed_by_datahub=True,
        )

        # Add URN
        urn = "urn:li:tag:status:active"
        urns.add(urn)
        assert urn in resource.datahub_linked_resources().urns

        # Convert to platform resource
        platform_resource = resource.as_platform_resource()
        assert platform_resource.resource_info is not None
        assert platform_resource.resource_info.secondary_keys is not None
        assert urn in platform_resource.resource_info.secondary_keys

    def test_tag_synchronization_flags_workflow(self) -> None:
        """Test the workflow for Unity Catalog synchronization flags."""
        # Create tag ID in initial state
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="priority",
            tag_value="high",
            platform_instance="workspace1",
            exists_in_unity_catalog=False,
            persisted=False,
        )

        # Simulate finding it in Unity Catalog
        tag_id.exists_in_unity_catalog = True

        # Simulate persisting to DataHub
        tag_id.persisted = True

        assert tag_id.exists_in_unity_catalog is True
        assert tag_id.persisted is True

        # Create platform resource
        resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=["urn:li:tag:priority:high"]),
            managed_by_datahub=False,
        )

        # Should maintain flag states
        assert resource.id.exists_in_unity_catalog is True
        assert resource.id.persisted is True


class TestGetUnityCatalogTagCacheInfo:
    """Tests for get_unity_catalog_tag_cache_info function."""

    def test_get_cache_info(self) -> None:
        """Test getting cache info from platform resource repository."""
        mock_repository = Mock()
        expected_cache_info = {
            "search_by_urn_cache": {"hits": 10, "misses": 2},
            "get_from_datahub_cache": {"hits": 15, "misses": 5},
        }
        mock_repository.get_entity_cache_info.return_value = expected_cache_info

        result = get_unity_catalog_tag_cache_info(mock_repository)

        assert result == expected_cache_info
        mock_repository.get_entity_cache_info.assert_called_once()

    def test_get_cache_info_delegates_correctly(self) -> None:
        """Test that function properly delegates to repository method."""
        mock_repository = Mock()

        get_unity_catalog_tag_cache_info(mock_repository)

        # Should call the repository method exactly once
        mock_repository.get_entity_cache_info.assert_called_once_with()


class TestEdgeCasesAndErrorHandling:
    """Tests for edge cases and error handling."""

    def test_tag_id_with_special_characters(self) -> None:
        """Test tag ID with special characters in key/value."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="project:name",
            tag_value="data-pipeline@v2",
            platform_instance="workspace-1",
        )

        key = tag_id.to_platform_resource_key()
        assert key.primary_key == "project:name:data-pipeline@v2"

    def test_tag_resource_with_large_allowed_values_list(self) -> None:
        """Test tag resource with large allowed values list."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="region", tag_value="us-west-1"
        )

        large_allowed_values = [f"region-{i}" for i in range(100)]
        resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=True,
            allowed_values=large_allowed_values,
        )

        assert resource.allowed_values is not None
        assert len(resource.allowed_values) == 100
        assert "region-0" in resource.allowed_values
        assert "region-99" in resource.allowed_values

    def test_resource_equality_and_hashing(self) -> None:
        """Test equality and hashing behavior of tag resources."""
        tag_id1 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="ws1"
        )
        tag_id2 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="ws1"
        )

        resource1 = UnityCatalogTagPlatformResource(
            id=tag_id1, datahub_urns=LinkedResourceSet(urns=[]), managed_by_datahub=True
        )
        resource2 = UnityCatalogTagPlatformResource(
            id=tag_id2, datahub_urns=LinkedResourceSet(urns=[]), managed_by_datahub=True
        )

        # Resources with same content should be equal
        assert resource1 == resource2

    def test_sync_context_validation(self) -> None:
        """Test sync context validation and edge cases."""
        # Test with very long platform instance name
        long_name = "workspace-" + "a" * 100
        context = UnityCatalogTagSyncContext(platform_instance=long_name)
        assert context.platform_instance == long_name

        # Test with special characters
        special_name = "workspace@prod#2024"
        context_special = UnityCatalogTagSyncContext(platform_instance=special_name)
        assert context_special.platform_instance == special_name


class TestUnityCatalogTagEntitiesIntegrationWithRepository:
    """Integration tests with Unity Catalog platform resource repository."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def mock_repository(self) -> Mock:
        """Mock Unity Catalog platform resource repository."""
        repo = Mock()
        repo.get_resource_type.return_value = "UnityCatalogTagPlatformResource"
        repo.get_entity_class.return_value = UnityCatalogTagPlatformResource
        return repo

    def test_tag_creation_with_repository_integration(
        self, mock_repository: Mock
    ) -> None:
        """Test tag creation integrated with repository."""
        # Create Unity Catalog tag
        unity_tag = UnityCatalogTag(key="environment", value="staging")

        # Mock repository responses
        mock_repository.search_entity_by_urn.return_value = None
        mock_repository.platform_instance = "workspace1"

        # Create tag ID from Unity Catalog tag
        tag_id = UnityCatalogTagPlatformResourceId.from_tag(
            unity_tag, mock_repository, exists_in_unity_catalog=True
        )

        assert tag_id.tag_key == "environment"
        assert tag_id.tag_value == "staging"
        assert tag_id.platform_instance == "workspace1"
        assert tag_id.exists_in_unity_catalog is True

        # Create platform resource
        platform_resource = UnityCatalogTagPlatformResource(
            id=tag_id, datahub_urns=LinkedResourceSet(urns=[]), managed_by_datahub=False
        )

        # Get from DataHub using repository
        mock_repository.get_entity_from_datahub.return_value = platform_resource

        result = UnityCatalogTagPlatformResource.get_from_datahub(
            tag_id, mock_repository, False
        )

        assert result == platform_resource
        mock_repository.get_entity_from_datahub.assert_called_once_with(tag_id, False)

    def test_error_handling_in_from_datahub_urn(
        self, mock_repository: Mock, mock_graph: Mock
    ) -> None:
        """Test error handling in from_datahub_urn method."""
        context = UnityCatalogTagSyncContext(platform_instance="workspace1")
        mock_repository.search_entity_by_urn.return_value = None

        # Test with malformed URN that will cause generate_tag_id to fail
        with pytest.raises(
            (ValueError, InvalidUrnError),
            match="(Unable to create Unity Catalog tag ID|Invalid urn string)",
        ):
            UnityCatalogTagPlatformResourceId.from_datahub_urn(
                "malformed-urn", context, mock_repository, mock_graph
            )

    def test_cache_integration_workflow(self, mock_repository: Mock) -> None:
        """Test workflow that involves caching."""
        # Setup repository with cache info
        mock_repository.get_entity_cache_info.return_value = {
            "search_by_urn_cache": {"hits": 5, "misses": 1},
            "get_from_datahub_cache": {"hits": 3, "misses": 2},
        }

        # Get cache info
        cache_info = get_unity_catalog_tag_cache_info(mock_repository)

        assert cache_info["search_by_urn_cache"]["hits"] == 5
        assert cache_info["get_from_datahub_cache"]["hits"] == 3
