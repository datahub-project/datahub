from typing import List, Optional
from unittest.mock import Mock, PropertyMock, patch

import cachetools
import pytest

# Import the classes from your module
from datahub.api.entities.external.external_entities import (
    CaseSensitivity,
    ExternalEntityId,
    LinkedResourceSet,
    MissingExternalEntity,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.platform_resource_repository import (
    UnityCatalogPlatformResourceRepository,
)
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
)
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.error import InvalidUrnError


class TestPlatformResourceRepository:
    """Tests for PlatformResourceRepository class using Unity Catalog implementation."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        """Create a mock DataHubGraph."""
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> UnityCatalogPlatformResourceRepository:
        """Create a UnityCatalogPlatformResourceRepository instance."""
        return UnityCatalogPlatformResourceRepository(mock_graph)

    @pytest.fixture
    def mock_platform_resource(self) -> Mock:
        """Create a mock PlatformResource."""
        resource: Mock = Mock(spec=PlatformResource)
        resource.id = "test-resource-id"
        resource.resource_info = None  # Simulate no resource_info for simple test
        return resource

    def test_init(self, mock_graph: Mock) -> None:
        """Test repository initialization."""
        repo = UnityCatalogPlatformResourceRepository(mock_graph)
        assert repo.graph == mock_graph
        assert isinstance(repo.urn_search_cache, cachetools.LRUCache)
        assert isinstance(repo.external_entity_cache, cachetools.LRUCache)
        assert repo.urn_search_cache.maxsize == 1000
        assert repo.external_entity_cache.maxsize == 1000
        # Verify thread safety infrastructure
        assert hasattr(repo, "_cache_lock")
        # Verify statistics tracking
        assert hasattr(repo, "urn_search_cache_hits")
        assert hasattr(repo, "external_entity_cache_hits")

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_search_by_filter_with_cache(
        self,
        mock_search: Mock,
        repository: UnityCatalogPlatformResourceRepository,
        mock_platform_resource: Mock,
    ) -> None:
        """Test search_by_filter with caching enabled."""
        mock_filter = Mock(spec=ElasticDocumentQuery)
        mock_search.return_value = [mock_platform_resource]

        results = list(repository.search_by_filter(mock_filter, add_to_cache=True))

        assert len(results) == 1
        assert results[0] == mock_platform_resource
        # Note: search_by_filter no longer caches platform resources since we eliminated platform_resource_cache
        mock_search.assert_called_once_with(repository.graph, mock_filter)

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_search_by_filter_without_cache(
        self,
        mock_search: Mock,
        repository: UnityCatalogPlatformResourceRepository,
        mock_platform_resource: Mock,
    ) -> None:
        """Test search_by_filter with caching disabled."""
        mock_filter = Mock(spec=ElasticDocumentQuery)
        mock_search.return_value = [mock_platform_resource]

        results = list(repository.search_by_filter(mock_filter, add_to_cache=False))

        assert len(results) == 1
        assert results[0] == mock_platform_resource
        # Note: search_by_filter no longer uses caching since we eliminated platform_resource_cache
        mock_search.assert_called_once_with(repository.graph, mock_filter)

    def test_create(
        self,
        repository: UnityCatalogPlatformResourceRepository,
        mock_platform_resource: Mock,
    ) -> None:
        """Test create method."""
        repository.create(mock_platform_resource)

        mock_platform_resource.to_datahub.assert_called_once_with(repository.graph)
        # Note: create method no longer caches platform resources directly since we eliminated platform_resource_cache

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_get_existing(
        self, mock_search: Mock, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test get method for existing resource."""
        mock_platform_resource_key: PlatformResourceKey = PlatformResourceKey(
            platform="test-platform",
            resource_type="test-resource-type",
            platform_instance="test-instance",
            primary_key="test-primary-key",
        )

        mock_platform_resource = PlatformResource.create(
            key=mock_platform_resource_key, secondary_keys=[], value={}
        )
        mock_search.return_value = [mock_platform_resource]

        result: Optional[PlatformResource] = repository.get(mock_platform_resource_key)

        assert result == mock_platform_resource

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_get_non_existing(
        self, mock_search: Mock, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test get method for non-existing resource."""
        mock_platform_resource_key: PlatformResourceKey = PlatformResourceKey(
            platform="test-platform",
            resource_type="test-resource-type",
            platform_instance="test-instance",
            primary_key="test-primary-key",
        )
        mock_search.return_value = []

        result: Optional[PlatformResource] = repository.get(mock_platform_resource_key)

        assert result is None

    def test_delete(self, repository: UnityCatalogPlatformResourceRepository) -> None:
        """Test delete method."""
        mock_platform_resource_key: PlatformResourceKey = PlatformResourceKey(
            platform="test-platform",
            resource_type="test-resource-type",
            platform_instance="test-instance",
            primary_key="test-primary-key",
        )

        # Add item to external entity cache first
        mock_tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="test", platform_instance=None
        )
        mock_entity = UnityCatalogTagPlatformResource.create_default(
            mock_tag_id, managed_by_datahub=False
        )
        repository.external_entity_cache[mock_platform_resource_key.id] = mock_entity

        repository.delete(mock_platform_resource_key)

        repository.graph.delete_entity.assert_called_once_with(  # type: ignore[attr-defined]
            urn=PlatformResourceUrn(mock_platform_resource_key.id).urn(), hard=True
        )
        assert mock_platform_resource_key.id not in repository.external_entity_cache

    def test_get_resource_type(
        self, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test get_resource_type returns Unity Catalog resource type."""
        result = repository.get_resource_type()
        assert result == "UnityCatalogTagPlatformResource"

    def test_create_default_entity(
        self, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test create_default_entity method."""
        entity_id = UnityCatalogTagPlatformResourceId(
            tag_key="test-key",
            tag_value="test-value",
            platform_instance="test-instance",
        )
        result = repository.create_default_entity(entity_id, True)

        assert isinstance(result, UnityCatalogTagPlatformResource)
        assert result.id == entity_id
        assert result.managed_by_datahub is True
        assert isinstance(result.datahub_urns, LinkedResourceSet)
        assert result.datahub_urns.urns == []

    def test_platform_instance_direct_access(
        self, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test direct access to platform_instance attribute."""
        result = repository.platform_instance
        assert (
            result is None
        )  # Unity Catalog repository's platform_instance is None in this case


class TestCaseSensitivity:
    """Tests for CaseSensitivity enum and methods."""

    def test_detect_case_sensitivity_upper(self) -> None:
        """Test detecting uppercase strings."""
        result: CaseSensitivity = CaseSensitivity.detect_case_sensitivity("HELLO")
        assert result == CaseSensitivity.UPPER

    def test_detect_case_sensitivity_lower(self) -> None:
        """Test detecting lowercase strings."""
        result: CaseSensitivity = CaseSensitivity.detect_case_sensitivity("hello")
        assert result == CaseSensitivity.LOWER

    def test_detect_case_sensitivity_mixed(self) -> None:
        """Test detecting mixed case strings."""
        result: CaseSensitivity = CaseSensitivity.detect_case_sensitivity("Hello")
        assert result == CaseSensitivity.MIXED

    def test_detect_for_many_empty_list(self) -> None:
        """Test detect_for_many with empty list."""
        result: CaseSensitivity = CaseSensitivity.detect_for_many([])
        assert result == CaseSensitivity.MIXED

    def test_detect_for_many_all_upper(self) -> None:
        """Test detect_for_many with all uppercase strings."""
        result: CaseSensitivity = CaseSensitivity.detect_for_many(
            ["HELLO", "WORLD", "TEST"]
        )
        assert result == CaseSensitivity.UPPER

    def test_detect_for_many_all_lower(self) -> None:
        """Test detect_for_many with all lowercase strings."""
        result: CaseSensitivity = CaseSensitivity.detect_for_many(
            ["hello", "world", "test"]
        )
        assert result == CaseSensitivity.LOWER

    def test_detect_for_many_mixed_cases(self) -> None:
        """Test detect_for_many with mixed case strings."""
        result: CaseSensitivity = CaseSensitivity.detect_for_many(
            ["HELLO", "world", "Test"]
        )
        assert result == CaseSensitivity.MIXED


class TestLinkedResourceSet:
    """Tests for LinkedResourceSet class."""

    @pytest.fixture
    def empty_resource_set(self) -> LinkedResourceSet:
        """Create an empty LinkedResourceSet."""
        return LinkedResourceSet(urns=[])

    @pytest.fixture
    def populated_resource_set(self) -> LinkedResourceSet:
        """Create a LinkedResourceSet with some URNs."""
        return LinkedResourceSet(
            urns=[
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table2,PROD)",
            ]
        )

    def test_has_conflict_duplicate_urn(
        self, populated_resource_set: LinkedResourceSet
    ) -> None:
        """Test _has_conflict with duplicate URN."""
        urn: Urn = Urn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table1,PROD)"
        )

        result: bool = populated_resource_set._has_conflict(urn)

        assert result is False  # No conflict for duplicate URNs

    def test_has_conflict_different_entity_types(
        self, empty_resource_set: LinkedResourceSet
    ) -> None:
        """Test _has_conflict with different entity types."""
        # Setup existing URN
        empty_resource_set.urns = ["urn:li:tag:test"]

        # New URN with different entity type
        new_urn: Urn = Urn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        )

        result: bool = empty_resource_set._has_conflict(new_urn)

        assert result is True

    def test_has_conflict_invalid_existing_urn(
        self, empty_resource_set: LinkedResourceSet
    ) -> None:
        """Test _has_conflict with invalid existing URN."""
        empty_resource_set.urns = ["invalid-urn"]
        new_urn: Urn = Urn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        )
        with pytest.raises(InvalidUrnError):
            empty_resource_set._has_conflict(new_urn)

    def test_add_new_urn_string(self, empty_resource_set: LinkedResourceSet) -> None:
        """Test adding a new URN as string."""
        empty_resource_set.urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        ]
        result: bool = empty_resource_set.add(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table2,PROD)"
        )

        assert result is True
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table2,PROD)"
            )
            == 1
        )
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
            == 1
        )

    def test_add_new_urn_object(self, empty_resource_set: LinkedResourceSet) -> None:
        """Test adding a new URN as Urn object."""
        empty_resource_set.urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        ]
        result: bool = empty_resource_set.add(
            Urn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table2,PROD)"
            )
        )

        assert result is True
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table2,PROD)"
            )
            == 1
        )
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
            == 1
        )

    def test_add_duplicate_urn(self, empty_resource_set: LinkedResourceSet) -> None:
        """Test adding a duplicate URN."""
        empty_resource_set.urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        ]
        result: bool = empty_resource_set.add(
            Urn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
        )

        assert result is False  # Already exists
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
            == 1
        )

    def test_add_conflicting_urn(self, empty_resource_set: LinkedResourceSet) -> None:
        """Test adding a conflicting URN raises ValueError."""
        empty_resource_set.urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        ]

        with pytest.raises(ValueError, match="Conflict detected"):
            empty_resource_set.add("urn:li:tag:myTag")

    def test_add_deduplicates_urns(self, empty_resource_set: LinkedResourceSet) -> None:
        """Test that add method deduplicates existing URNs."""
        empty_resource_set.urns = [
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)",
        ]
        result: bool = empty_resource_set.add(
            Urn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
        )

        assert result is False  # Already exists after deduplication
        assert (
            empty_resource_set.urns.count(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )
            == 1
        )


class TestMissingExternalEntity:
    """Tests for MissingExternalEntity class."""

    @pytest.fixture
    def mock_external_entity_id(self) -> ExternalEntityId:
        """Create a concrete ExternalEntityId for testing."""
        return MockExternalEntityId(
            primary_key="test-key", platform_instance="test-instance"
        )

    @pytest.fixture
    def missing_entity(
        self, mock_external_entity_id: ExternalEntityId
    ) -> MissingExternalEntity:
        """Create a MissingExternalEntity instance."""
        return MissingExternalEntity(id=mock_external_entity_id)

    def test_is_managed_by_datahub(self, missing_entity: MissingExternalEntity) -> None:
        """Test is_managed_by_datahub returns False."""
        result: bool = missing_entity.is_managed_by_datahub()
        assert result is False

    def test_datahub_linked_resources(
        self, missing_entity: MissingExternalEntity
    ) -> None:
        """Test datahub_linked_resources returns empty LinkedResourceSet."""
        result: LinkedResourceSet = missing_entity.datahub_linked_resources()
        assert isinstance(result, LinkedResourceSet)
        assert result.urns == []

    def test_as_platform_resource(self, missing_entity: MissingExternalEntity) -> None:
        """Test as_platform_resource returns None."""
        result: Optional[PlatformResource] = missing_entity.as_platform_resource()
        assert result is None

    def test_get_id(
        self,
        missing_entity: MissingExternalEntity,
        mock_external_entity_id: ExternalEntityId,
    ) -> None:
        """Test get_id returns the correct id."""
        result: ExternalEntityId = missing_entity.get_id()
        assert result == mock_external_entity_id


class TestExternalEntityId:
    """Tests for ExternalEntityId abstract class."""

    def test_external_entity_id_is_abstract(self) -> None:
        """Test that ExternalEntityId cannot be instantiated directly."""
        # ExternalEntityId doesn't have __init__ defined, so we test through inheritance
        assert hasattr(ExternalEntityId, "to_platform_resource_key")
        # platform_instance is a Pydantic field, check it exists in the model fields
        assert "platform_instance" in ExternalEntityId.__fields__


class MockExternalEntityId(ExternalEntityId):
    """Concrete implementation of ExternalEntityId for testing."""

    primary_key: str
    platform_instance: Optional[str] = None
    persisted: bool = False

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="test-platform",
            resource_type="TestResource",
            primary_key=self.primary_key,
            platform_instance=self.platform_instance,
        )


class TestUnityCatalogTagPlatformResource:
    """Tests for UnityCatalogTagPlatformResource class."""

    @pytest.fixture
    def external_entity_id(self) -> UnityCatalogTagPlatformResourceId:
        return UnityCatalogTagPlatformResourceId(
            tag_key="test-key",
            tag_value="test-value",
            platform_instance="test-instance",
        )

    @pytest.fixture
    def unity_resource(
        self, external_entity_id: UnityCatalogTagPlatformResourceId
    ) -> UnityCatalogTagPlatformResource:
        return UnityCatalogTagPlatformResource(
            id=external_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
        )

    def test_get_id(
        self,
        unity_resource: UnityCatalogTagPlatformResource,
        external_entity_id: UnityCatalogTagPlatformResourceId,
    ) -> None:
        """Test get_id method."""
        assert unity_resource.get_id() == external_entity_id

    def test_is_managed_by_datahub_false(
        self, unity_resource: UnityCatalogTagPlatformResource
    ) -> None:
        """Test is_managed_by_datahub when False."""
        assert unity_resource.is_managed_by_datahub() is False

    def test_is_managed_by_datahub_true(
        self, external_entity_id: UnityCatalogTagPlatformResourceId
    ) -> None:
        """Test is_managed_by_datahub when True."""
        resource = UnityCatalogTagPlatformResource(
            id=external_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=True,
        )
        assert resource.is_managed_by_datahub() is True

    def test_datahub_linked_resources(
        self, unity_resource: UnityCatalogTagPlatformResource
    ) -> None:
        """Test datahub_linked_resources method."""
        linked_resources = unity_resource.datahub_linked_resources()
        assert isinstance(linked_resources, LinkedResourceSet)
        assert linked_resources.urns == []

    def test_as_platform_resource(
        self,
        unity_resource: UnityCatalogTagPlatformResource,
        external_entity_id: UnityCatalogTagPlatformResourceId,
    ) -> None:
        """Test as_platform_resource method."""
        platform_resource = unity_resource.as_platform_resource()
        assert isinstance(platform_resource, PlatformResource)
        assert platform_resource.id == external_entity_id.to_platform_resource_key().id
        assert platform_resource.resource_info is not None
        assert (
            platform_resource.resource_info.secondary_keys == []
        )  # Empty URNs should result in empty secondary_keys


class MockSyncContext:
    """Mock implementation of SyncContext for testing."""

    def __init__(self, platform_instance: Optional[str] = None):
        self.platform_instance = platform_instance


class TestPlatformResourceRepositoryAdvanced:
    """Advanced tests for PlatformResourceRepository abstract methods."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> UnityCatalogPlatformResourceRepository:
        return UnityCatalogPlatformResourceRepository(mock_graph)

    def test_as_obj_cache_info(
        self, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test as_obj method returns cache statistics."""
        # Simulate some cache activity
        repository.urn_search_cache_hits = 5
        repository.urn_search_cache_misses = 3
        repository.external_entity_cache_hits = 7
        repository.external_entity_cache_misses = 2
        from datahub.api.entities.external.external_entities import UrnCacheKey

        cache_key = UrnCacheKey(urn="test1", platform_instance=None)
        mock_tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="test", platform_instance=None
        )
        repository.urn_search_cache[cache_key] = mock_tag_id
        mock_entity = UnityCatalogTagPlatformResource.create_default(
            mock_tag_id, managed_by_datahub=False
        )
        repository.external_entity_cache["test2"] = mock_entity

        cache_info = repository.as_obj()

        assert cache_info["search_by_urn_cache"]["hits"] == 5
        assert cache_info["search_by_urn_cache"]["misses"] == 3
        assert cache_info["search_by_urn_cache"]["current_size"] == 1
        assert cache_info["search_by_urn_cache"]["max_size"] == 1000
        assert cache_info["external_entity_cache"]["hits"] == 7
        assert cache_info["external_entity_cache"]["misses"] == 2
        assert cache_info["external_entity_cache"]["current_size"] == 1
        assert cache_info["external_entity_cache"]["max_size"] == 1000


class TestLinkedResourceSetAdvanced:
    """Advanced tests for LinkedResourceSet."""

    def test_add_with_conflicting_entity_types_raises_error(self) -> None:
        """Test that adding URNs with conflicting entity types raises ValueError."""
        resource_set = LinkedResourceSet(urns=["urn:li:tag:test1"])

        with pytest.raises(ValueError, match="Conflict detected"):
            resource_set.add(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
            )

    def test_has_conflict_with_valid_same_entity_types(self) -> None:
        """Test _has_conflict with valid same entity types."""
        resource_set = LinkedResourceSet(urns=["urn:li:tag:test1"])
        new_urn = Urn.from_string("urn:li:tag:test2")

        result = resource_set._has_conflict(new_urn)
        assert result is False

    def test_has_conflict_with_invalid_new_urn(self) -> None:
        """Test _has_conflict with invalid new URN."""
        resource_set = LinkedResourceSet(urns=["urn:li:tag:test1"])

        # Create a mock URN that will raise ValueError when accessing entity_type
        mock_urn = Mock()
        mock_urn.urn.return_value = "invalid-urn"
        # Use a side effect to raise ValueError when entity_type is accessed
        type(mock_urn).entity_type = PropertyMock(side_effect=ValueError("Invalid URN"))

        # This should return True (conflict detected) due to the ValueError
        result = resource_set._has_conflict(mock_urn)
        assert result is True  # Should detect conflict due to error

    def test_add_deduplicates_concurrent_duplicates(self) -> None:
        """Test that add method handles concurrent duplicates correctly."""
        resource_set = LinkedResourceSet(
            urns=[
                "urn:li:tag:test1",
                "urn:li:tag:test1",  # Duplicate
                "urn:li:tag:test2",
            ]
        )

        # Adding an existing URN should return False and not add duplicates
        result = resource_set.add("urn:li:tag:test1")

        assert result is False
        assert resource_set.urns.count("urn:li:tag:test1") == 1
        assert resource_set.urns.count("urn:li:tag:test2") == 1


class TestSyncContextProtocol:
    """Tests for SyncContext protocol."""

    def test_mock_sync_context_implements_protocol(self) -> None:
        """Test that MockSyncContext implements SyncContext protocol."""
        sync_context = MockSyncContext("test-instance")

        # Should be able to access platform_instance attribute
        assert sync_context.platform_instance == "test-instance"

        # Test with None
        sync_context_none = MockSyncContext(None)
        assert sync_context_none.platform_instance is None


class TestIntegration:
    """Integration tests combining multiple classes."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> UnityCatalogPlatformResourceRepository:
        return UnityCatalogPlatformResourceRepository(mock_graph)

    def test_repository_with_linked_resource_set(
        self, repository: UnityCatalogPlatformResourceRepository
    ) -> None:
        """Test repository operations with LinkedResourceSet."""
        # Create a mock platform resource with linked URNs
        mock_resource: Mock = Mock(spec=PlatformResource)
        mock_resource.id = "test-resource"
        mock_resource.resource_info = None  # Simulate no resource_info for simple test

        # Test create method calls DataHub
        repository.create(mock_resource)
        mock_resource.to_datahub.assert_called_once_with(repository.graph)

    def test_case_sensitivity_with_linked_resources(self) -> None:
        """Test case sensitivity detection with LinkedResourceSet."""
        # Test with mixed case detection
        test_values: List[str] = ["TABLE1", "table2", "Table3"]
        result: CaseSensitivity = CaseSensitivity.detect_for_many(test_values)
        assert result == CaseSensitivity.MIXED


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_cache_behavior_with_unity_catalog_repository(self) -> None:
        """Test cache behavior with Unity Catalog repository."""
        mock_graph: Mock = Mock(spec=DataHubGraph)
        repo = UnityCatalogPlatformResourceRepository(mock_graph)

        # Add item to external entity cache
        mock_tag_id = UnityCatalogTagPlatformResourceId(
            tag_key="test", platform_instance=None
        )
        mock_entity = UnityCatalogTagPlatformResource.create_default(
            mock_tag_id, managed_by_datahub=False
        )
        repo.external_entity_cache["test-key"] = mock_entity
        assert "test-key" in repo.external_entity_cache

        # Cache should still contain the item
        cached_value = repo.external_entity_cache.get("test-key")
        assert cached_value == mock_entity

    def test_linked_resource_set_with_empty_urns(self) -> None:
        """Test LinkedResourceSet behavior with empty URN list."""
        test_urn: str = "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        resource_set: LinkedResourceSet = LinkedResourceSet(urns=[])

        result: bool = resource_set.add(test_urn)

        assert result is True
        assert test_urn in resource_set.urns

    def test_case_sensitivity_enum_values(self) -> None:
        """Test CaseSensitivity enum values."""
        assert CaseSensitivity.UPPER.value == "upper"
        assert CaseSensitivity.LOWER.value == "lower"
        assert CaseSensitivity.MIXED.value == "mixed"
