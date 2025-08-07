from unittest.mock import MagicMock, patch

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


class TestUnityCatalogTagSyncContext:
    def test_context_creation_default(self):
        """Test creating sync context with default values."""
        context = UnityCatalogTagSyncContext()
        assert context.platform_instance is None

    def test_context_creation_with_platform_instance(self):
        """Test creating sync context with platform instance."""
        context = UnityCatalogTagSyncContext(platform_instance="prod")
        assert context.platform_instance == "prod"

    def test_context_immutability(self):
        """Test that the sync context is immutable (frozen)."""
        context = UnityCatalogTagSyncContext(platform_instance="test")
        with pytest.raises(TypeError):
            context.platform_instance = "changed"  # type: ignore[misc]

    def test_context_hashable(self):
        """Test that the sync context is hashable."""
        context1 = UnityCatalogTagSyncContext(platform_instance="test")
        context2 = UnityCatalogTagSyncContext(platform_instance="test")
        context3 = UnityCatalogTagSyncContext(platform_instance="prod")

        # Should be hashable and usable as dict keys
        contexts = {context1: "value1", context2: "value2", context3: "value3"}
        assert len(contexts) == 2  # context1 and context2 should be the same hash


class TestUnityCatalogTagPlatformResourceId:
    def test_hash_function(self):
        """Test hash function for platform resource ID."""
        id1 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        id2 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        id3 = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="dev", platform_instance="test"
        )

        assert hash(id1) == hash(id2)
        assert hash(id1) != hash(id3)

    def test_resource_type_static_method(self):
        """Test the static resource type method."""
        resource_type = UnityCatalogTagPlatformResourceId._RESOURCE_TYPE()
        assert resource_type == "UnityCatalogTagPlatformResource"

    def test_to_platform_resource_key(self):
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

    def test_to_platform_resource_key_none_value(self):
        """Test conversion to platform resource key with None tag value."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="department", tag_value=None, platform_instance="workspace1"
        )

        key = tag_id.to_platform_resource_key()
        assert key.primary_key == "department:None"

    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.search_by_urn"
    )
    def test_from_tag_with_existing_resource(self, mock_search):
        """Test creating from tag when resource already exists."""
        # Mock existing resource
        existing_resource = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        mock_search.return_value = existing_resource

        # Create real Unity Catalog tag - no mocking needed
        unity_tag = UnityCatalogTag(key="env", value="prod")
        mock_repo = MagicMock()

        result = UnityCatalogTagPlatformResourceId.from_tag(
            unity_tag, "test", mock_repo, exists_in_unity_catalog=True
        )

        assert result == existing_resource
        mock_search.assert_called_once()

    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.search_by_urn"
    )
    def test_from_tag_without_existing_resource(self, mock_search):
        """Test creating from tag when no existing resource."""
        mock_search.return_value = None

        # Create real Unity Catalog tag with None value
        unity_tag = UnityCatalogTag(key="department", value=None)
        mock_repo = MagicMock()

        result = UnityCatalogTagPlatformResourceId.from_tag(
            unity_tag, "workspace1", mock_repo, exists_in_unity_catalog=True
        )

        assert result.tag_key == "department"
        assert result.tag_value is None
        assert result.platform_instance == "workspace1"
        assert result.exists_in_unity_catalog is True
        assert result.persisted is False

    def test_search_by_urn_cache_key_consistency(self):
        """Test that cache keys are consistent for search_by_urn."""
        # Clear cache first
        UnityCatalogTagPlatformResourceId.search_by_urn.cache_clear()

        mock_repo = MagicMock()
        mock_repo.search_by_filter.return_value = []

        context = UnityCatalogTagSyncContext(platform_instance="test")

        # Call with same parameters multiple times
        result1 = UnityCatalogTagPlatformResourceId.search_by_urn(
            "urn:li:tag:env:prod", mock_repo, context
        )
        result2 = UnityCatalogTagPlatformResourceId.search_by_urn(
            "urn:li:tag:env:prod", mock_repo, context
        )

        # Should be cached (second call shouldn't trigger repository search again)
        assert mock_repo.search_by_filter.call_count == 1
        assert result1 == result2

    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.generate_tag_id"
    )
    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.search_by_urn"
    )
    def test_from_datahub_urn_existing_resource(self, mock_search, mock_generate):
        """Test creating from DataHub URN with existing resource."""
        existing_resource = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="prod", platform_instance="test"
        )
        mock_search.return_value = existing_resource

        mock_repo = MagicMock()
        mock_graph = MagicMock(spec=DataHubGraph)
        context = UnityCatalogTagSyncContext(platform_instance="test")

        result = UnityCatalogTagPlatformResourceId.from_datahub_urn(
            "urn:li:tag:env:prod", mock_repo, context, mock_graph
        )

        assert result == existing_resource
        mock_generate.assert_not_called()

    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.generate_tag_id"
    )
    @patch(
        "datahub.ingestion.source.unity.tag_entities.UnityCatalogTagPlatformResourceId.search_by_urn"
    )
    def test_from_datahub_urn_new_resource(self, mock_search, mock_generate):
        """Test creating from DataHub URN with new resource."""
        mock_search.return_value = None

        new_tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="dev", platform_instance="test"
        )
        mock_generate.return_value = new_tag_id

        mock_repo = MagicMock()
        mock_repo.get.return_value = MagicMock()  # Resource exists in Unity Catalog

        mock_graph = MagicMock(spec=DataHubGraph)
        context = UnityCatalogTagSyncContext(platform_instance="test")

        result = UnityCatalogTagPlatformResourceId.from_datahub_urn(
            "urn:li:tag:env:dev", mock_repo, context, mock_graph
        )

        assert result == new_tag_id
        assert result.exists_in_unity_catalog is True

    def test_generate_tag_id_unsupported_entity_type(self):
        """Test generate_tag_id with unsupported entity type."""
        mock_graph = MagicMock(spec=DataHubGraph)
        context = UnityCatalogTagSyncContext(platform_instance="test")

        with pytest.raises(ValueError, match="Unsupported entity type dataset"):
            UnityCatalogTagPlatformResourceId.generate_tag_id(
                mock_graph,
                context,
                "urn:li:dataset:(urn:li:dataPlatform:spark,test,PROD)",
            )

    @patch("datahub.ingestion.source.unity.tag_entities.UnityCatalogTag")
    def test_from_datahub_tag(self, mock_uc_tag_class):
        """Test creating from DataHub tag URN."""
        mock_uc_tag = MagicMock()
        mock_uc_tag.key = "environment"
        mock_uc_tag.value = "staging"
        mock_uc_tag_class.from_urn.return_value = mock_uc_tag

        tag_urn = TagUrn.from_string("urn:li:tag:environment:staging")
        context = UnityCatalogTagSyncContext(platform_instance="workspace1")

        result = UnityCatalogTagPlatformResourceId.from_datahub_tag(tag_urn, context)

        assert result.tag_key == "environment"
        assert result.tag_value == "staging"
        assert result.platform_instance == "workspace1"
        assert result.exists_in_unity_catalog is False


class TestUnityCatalogTagPlatformResource:
    def test_platform_resource_creation(self):
        """Test creation of Unity Catalog tag platform resource."""
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

    def test_as_platform_resource(self):
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
            == tag_id.to_platform_resource_key().resource_type
        )
        assert (
            platform_resource.resource_info.primary_key
            == tag_id.to_platform_resource_key().primary_key
        )
        assert platform_resource.resource_info.secondary_keys == [
            "urn:li:tag:team:data"
        ]

    def test_get_from_datahub_cache_consistency(self):
        """Test that get_from_datahub cache is consistent."""
        # Clear cache first
        UnityCatalogTagPlatformResource.get_from_datahub.cache_clear()

        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="env", tag_value="test", platform_instance="workspace1"
        )

        mock_repo = MagicMock()
        mock_repo.search_by_filter.return_value = []

        # Call multiple times with same parameters
        result1 = UnityCatalogTagPlatformResource.get_from_datahub(
            tag_id, mock_repo, managed_by_datahub=True
        )
        result2 = UnityCatalogTagPlatformResource.get_from_datahub(
            tag_id, mock_repo, managed_by_datahub=True
        )

        # Should be cached
        assert mock_repo.search_by_filter.call_count == 1
        assert result1.id == result2.id

    def test_get_from_datahub_with_existing_resource(self):
        """Test get_from_datahub with existing platform resource."""
        tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="cost_center", tag_value="engineering", platform_instance="prod"
        )

        # Mock existing platform resource
        mock_platform_resource = MagicMock()
        mock_platform_resource.resource_info.value.as_pydantic_object.return_value.dict.return_value = {
            "id": {
                "tag_key": "cost_center",
                "tag_value": "engineering",
                "platform_instance": "prod",
                "exists_in_unity_catalog": True,
                "persisted": True,
            },
            "datahub_urns": {"urns": ["urn:li:tag:cost_center:engineering"]},
            "managed_by_datahub": True,
            "allowed_values": ["engineering", "marketing", "sales"],
        }

        mock_repo = MagicMock()
        mock_repo.search_by_filter.return_value = [mock_platform_resource]

        result = UnityCatalogTagPlatformResource.get_from_datahub(
            tag_id, mock_repo, managed_by_datahub=False
        )

        assert result.id.tag_key == "cost_center"
        assert result.id.tag_value == "engineering"
        assert result.managed_by_datahub is True


class TestCacheInfoFunction:
    def test_get_unity_catalog_tag_cache_info(self):
        """Test getting cache statistics."""
        # Clear caches first
        UnityCatalogTagPlatformResourceId.search_by_urn.cache_clear()
        UnityCatalogTagPlatformResource.get_from_datahub.cache_clear()

        cache_info = get_unity_catalog_tag_cache_info()

        # Check structure
        assert "search_by_urn_cache" in cache_info
        assert "get_from_datahub_cache" in cache_info

        # Check all expected fields are present
        for cache_name in ["search_by_urn_cache", "get_from_datahub_cache"]:
            cache_stats = cache_info[cache_name]
            assert "hits" in cache_stats
            assert "misses" in cache_stats
            assert "current_size" in cache_stats
            assert "max_size" in cache_stats

            # Initially should be empty
            assert cache_stats["current_size"] == 0


class TestIntegrationScenarios:
    def test_full_tag_sync_workflow(self):
        """Test complete tag synchronization workflow."""
        # Create real Unity Catalog tag - no mocking needed
        unity_tag = UnityCatalogTag(key="environment", value="production")

        # Mock platform resource repository
        mock_repo = MagicMock()
        mock_repo.search_by_filter.return_value = []
        mock_repo.get.return_value = None  # Tag doesn't exist in Unity Catalog

        # Test creating new tag resource ID
        tag_id = UnityCatalogTagPlatformResourceId.from_tag(
            unity_tag, "prod-workspace", mock_repo, exists_in_unity_catalog=False
        )

        assert tag_id.tag_key == "environment"
        assert tag_id.tag_value == "production"
        assert tag_id.platform_instance == "prod-workspace"
        assert tag_id.exists_in_unity_catalog is False
        assert tag_id.persisted is False

        # Test creating platform resource from tag ID
        urns = LinkedResourceSet(urns=["urn:li:tag:environment:production"])
        platform_resource = UnityCatalogTagPlatformResource(
            id=tag_id,
            datahub_urns=urns,
            managed_by_datahub=True,
            allowed_values=["dev", "staging", "production"],
        )

        # Convert to platform resource for storage
        stored_resource = platform_resource.as_platform_resource()

        assert stored_resource.resource_info is not None
        assert (
            stored_resource.resource_info.resource_type
            == "UnityCatalogTagPlatformResource"
        )
        assert stored_resource.resource_info.primary_key == "environment:production"
        assert stored_resource.resource_info.secondary_keys == [
            "urn:li:tag:environment:production"
        ]

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        # Test ValueError for invalid URN in from_datahub_urn
        mock_repo = MagicMock()
        mock_graph = MagicMock()
        context = UnityCatalogTagSyncContext(platform_instance="test")

        with (
            patch.object(
                UnityCatalogTagPlatformResourceId, "search_by_urn", return_value=None
            ),
            patch.object(
                UnityCatalogTagPlatformResourceId, "generate_tag_id", return_value=None
            ),
            pytest.raises(ValueError, match="Unable to create Unity Catalog tag ID"),
        ):
            UnityCatalogTagPlatformResourceId.from_datahub_urn(
                "urn:li:tag:invalid", mock_repo, context, mock_graph
            )
