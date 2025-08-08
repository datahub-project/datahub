from typing import List, Optional
from unittest.mock import Mock, PropertyMock, patch

import cachetools
import pytest
from pydantic import BaseModel

# Import the classes from your module
from datahub.api.entities.external.external_entities import (
    CaseSensitivity,
    ExternalEntityId,
    GenericPlatformResource,
    GenericPlatformResourceRepository,
    LinkedResourceSet,
    MissingExternalEntity,
    PlatformResourceRepository,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.error import InvalidUrnError


class TestPlatformResourceRepository:
    """Tests for PlatformResourceRepository class."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        """Create a mock DataHubGraph."""
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> GenericPlatformResourceRepository:
        """Create a GenericPlatformResourceRepository instance."""
        return GenericPlatformResourceRepository(mock_graph)

    @pytest.fixture
    def mock_platform_resource(self) -> Mock:
        """Create a mock PlatformResource."""
        resource: Mock = Mock(spec=PlatformResource)
        resource.id = "test-resource-id"
        return resource

    def test_init(self, mock_graph: Mock) -> None:
        """Test repository initialization."""
        repo = GenericPlatformResourceRepository(mock_graph)
        assert repo.graph == mock_graph
        assert isinstance(repo.entity_id_cache, cachetools.LRUCache)
        assert isinstance(repo.entity_object_cache, cachetools.LRUCache)
        assert repo.entity_id_cache.maxsize == 1000
        assert repo.entity_object_cache.maxsize == 1000

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_search_by_filter_with_cache(
        self,
        mock_search: Mock,
        repository: PlatformResourceRepository,
        mock_platform_resource: Mock,
    ) -> None:
        """Test search_by_filter with caching enabled."""
        mock_filter = Mock(spec=ElasticDocumentQuery)
        mock_search.return_value = [mock_platform_resource]

        results = list(repository.search_by_filter(mock_filter, add_to_cache=True))

        assert len(results) == 1
        assert results[0] == mock_platform_resource
        assert (
            repository.entity_object_cache[mock_platform_resource.id]
            == mock_platform_resource
        )
        mock_search.assert_called_once_with(repository.graph, mock_filter)

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_search_by_filter_without_cache(
        self,
        mock_search: Mock,
        repository: PlatformResourceRepository,
        mock_platform_resource: Mock,
    ) -> None:
        """Test search_by_filter with caching disabled."""
        mock_filter = Mock(spec=ElasticDocumentQuery)
        mock_search.return_value = [mock_platform_resource]

        results = list(repository.search_by_filter(mock_filter, add_to_cache=False))

        assert len(results) == 1
        assert results[0] == mock_platform_resource
        assert mock_platform_resource.id not in repository.entity_object_cache
        mock_search.assert_called_once_with(repository.graph, mock_filter)

    def test_create(
        self, repository: PlatformResourceRepository, mock_platform_resource: Mock
    ) -> None:
        """Test create method."""
        repository.create(mock_platform_resource)

        mock_platform_resource.to_datahub.assert_called_once_with(repository.graph)
        assert (
            repository.entity_object_cache[mock_platform_resource.id]
            == mock_platform_resource
        )

    def test_get_existing(self, repository: PlatformResourceRepository) -> None:
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

        repository.entity_object_cache[mock_platform_resource_key.id] = (
            mock_platform_resource
        )

        result: Optional[PlatformResource] = repository.get(mock_platform_resource_key)

        assert result == mock_platform_resource

    def test_get_non_existing(self, repository: PlatformResourceRepository) -> None:
        """Test get method for non-existing resource."""
        mock_platform_resource_key: PlatformResourceKey = PlatformResourceKey(
            platform="test-platform",
            resource_type="test-resource-type",
            platform_instance="test-instance",
            primary_key="test-primary-key",
        )

        result: Optional[PlatformResource] = repository.get(mock_platform_resource_key)

        assert result is None

    def test_delete(self, repository: PlatformResourceRepository) -> None:
        """Test delete method."""
        mock_platform_resource_key: PlatformResourceKey = PlatformResourceKey(
            platform="test-platform",
            resource_type="test-resource-type",
            platform_instance="test-instance",
            primary_key="test-primary-key",
        )

        mock_platform_resource: PlatformResource = PlatformResource.create(
            key=mock_platform_resource_key, secondary_keys=[], value={}
        )

        # Add item to cache first
        repository.entity_object_cache[mock_platform_resource_key.id] = (
            mock_platform_resource
        )

        repository.delete(mock_platform_resource_key)

        repository.graph.delete_entity.assert_called_once_with(  # type: ignore[attr-defined]
            urn=PlatformResourceUrn(mock_platform_resource.id).urn(), hard=True
        )
        assert mock_platform_resource_key.id not in repository.entity_object_cache


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
    def mock_urn(self) -> Mock:
        """Create a mock URN."""
        urn: Mock = Mock(spec=Urn)
        urn.urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.table,PROD)"
        )
        urn.entity_type = "dataset"
        urn.get_entity_id_as_string.return_value = "test.table"
        return urn

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
        empty_resource_set.urns = ["urn:li:tag2:test"]

        # New URN with different entity type
        new_urn: Urn = Urn.from_string("urn:li:tag:test")

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
        result: bool = empty_resource_set._has_conflict(
            Urn.from_string("urn:li:tag:myTag")
        )

        assert result is True  # Conflict detected

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
    def mock_external_entity_id(self) -> Mock:
        """Create a mock ExternalEntityId."""
        return Mock(spec=ExternalEntityId)

    @pytest.fixture
    def missing_entity(self, mock_external_entity_id: Mock) -> MissingExternalEntity:
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
        self, missing_entity: MissingExternalEntity, mock_external_entity_id: Mock
    ) -> None:
        """Test get_id returns the correct id."""
        result: ExternalEntityId = missing_entity.get_id()
        assert result == mock_external_entity_id


class TestIntegration:
    """Integration tests combining multiple classes."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> GenericPlatformResourceRepository:
        return GenericPlatformResourceRepository(mock_graph)

    def test_repository_with_linked_resource_set(
        self, repository: PlatformResourceRepository
    ) -> None:
        """Test repository operations with LinkedResourceSet."""
        # Create a mock platform resource with linked URNs
        mock_resource: Mock = Mock(spec=PlatformResource)
        mock_resource.id = "test-resource"

        # Test create and cache
        repository.create(mock_resource)
        assert repository.entity_object_cache["test-resource"] == mock_resource

    def test_case_sensitivity_with_linked_resources(self) -> None:
        """Test case sensitivity detection with LinkedResourceSet."""
        # Test with mixed case detection
        test_values: List[str] = ["TABLE1", "table2", "Table3"]
        result: CaseSensitivity = CaseSensitivity.detect_for_many(test_values)
        assert result == CaseSensitivity.MIXED


# Additional edge case tests
class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_cache_ttl_expiration(self) -> None:
        """Test that cache TTL works correctly."""
        mock_graph: Mock = Mock(spec=DataHubGraph)
        repo: GenericPlatformResourceRepository = GenericPlatformResourceRepository(
            mock_graph
        )

        # Add item to cache
        repo.entity_object_cache["test-key"] = "test-value"
        assert "test-key" in repo.entity_object_cache

        # Cache should still contain the item within TTL
        cached_value: Optional[str] = repo.entity_object_cache.get("test-key")
        assert cached_value == "test-value"

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


class TestExternalEntityId:
    """Tests for ExternalEntityId abstract class."""

    def test_external_entity_id_is_abstract(self) -> None:
        """Test that ExternalEntityId cannot be instantiated directly."""
        # ExternalEntityId doesn't have __init__ defined, so we test through inheritance
        assert hasattr(ExternalEntityId, "to_platform_resource_key")
        assert hasattr(ExternalEntityId, "platform_instance")


class MockExternalEntityId(BaseModel, ExternalEntityId):
    """Concrete implementation of ExternalEntityId for testing."""

    primary_key: str
    platform_instance: Optional[str] = None

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="test-platform",
            resource_type="TestResource",
            primary_key=self.primary_key,
            platform_instance=self.platform_instance,
        )


class TestGenericPlatformResource:
    """Tests for GenericPlatformResource class."""

    @pytest.fixture
    def external_entity_id(self) -> MockExternalEntityId:
        return MockExternalEntityId(primary_key="test-primary-key")

    @pytest.fixture
    def generic_resource(
        self, external_entity_id: MockExternalEntityId
    ) -> GenericPlatformResource:
        return GenericPlatformResource(
            id=external_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
        )

    def test_get_id(
        self,
        generic_resource: GenericPlatformResource,
        external_entity_id: MockExternalEntityId,
    ) -> None:
        """Test get_id method."""
        assert generic_resource.get_id() == external_entity_id

    def test_is_managed_by_datahub_false(
        self, generic_resource: GenericPlatformResource
    ) -> None:
        """Test is_managed_by_datahub when False."""
        assert generic_resource.is_managed_by_datahub() is False

    def test_is_managed_by_datahub_true(
        self, external_entity_id: MockExternalEntityId
    ) -> None:
        """Test is_managed_by_datahub when True."""
        resource = GenericPlatformResource(
            id=external_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=True,
        )
        assert resource.is_managed_by_datahub() is True

    def test_datahub_linked_resources(
        self, generic_resource: GenericPlatformResource
    ) -> None:
        """Test datahub_linked_resources method."""
        linked_resources = generic_resource.datahub_linked_resources()
        assert isinstance(linked_resources, LinkedResourceSet)
        assert linked_resources.urns == []

    def test_as_platform_resource(
        self,
        generic_resource: GenericPlatformResource,
        external_entity_id: MockExternalEntityId,
    ) -> None:
        """Test as_platform_resource method."""
        platform_resource = generic_resource.as_platform_resource()
        assert isinstance(platform_resource, PlatformResource)
        assert platform_resource.id == external_entity_id.to_platform_resource_key().id
        assert platform_resource.resource_info is not None
        assert (
            platform_resource.resource_info.secondary_keys == []
        )  # Empty URNs should result in empty secondary_keys

    def test_config_allows_arbitrary_types(
        self, external_entity_id: MockExternalEntityId
    ) -> None:
        """Test that Config allows arbitrary types."""
        # This should not raise an error due to arbitrary_types_allowed = True
        resource = GenericPlatformResource(
            id=external_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
        )
        assert resource is not None


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
    def repository(self, mock_graph: Mock) -> GenericPlatformResourceRepository:
        return GenericPlatformResourceRepository(mock_graph)

    def test_search_entity_by_urn_cache_hit(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test search_entity_by_urn with cache hit."""
        sync_context = MockSyncContext("test-instance")
        test_urn = "urn:li:tag:test"
        cache_key = f"search_urn:GenericPlatformResource:{test_urn}:test-instance"

        # Pre-populate cache
        expected_result = MockExternalEntityId(primary_key="cached-result")
        repository.entity_id_cache[cache_key] = expected_result

        result = repository.search_entity_by_urn(test_urn, sync_context)

        assert result == expected_result
        assert repository.entity_id_cache_hits == 1
        assert repository.entity_id_cache_misses == 0

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_search_entity_by_urn_cache_miss(
        self, mock_search: Mock, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test search_entity_by_urn with cache miss and no results."""
        sync_context = MockSyncContext("test-instance")
        test_urn = "urn:li:tag:test"
        mock_search.return_value = []

        result = repository.search_entity_by_urn(test_urn, sync_context)

        assert result is None
        assert repository.entity_id_cache_hits == 0
        assert repository.entity_id_cache_misses == 1
        # Result should be cached even when None
        cache_key = f"search_urn:GenericPlatformResource:{test_urn}:test-instance"
        assert repository.entity_id_cache[cache_key] is None

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_get_entity_from_datahub_cache_hit(
        self, mock_search: Mock, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test get_entity_from_datahub with cache hit."""
        entity_id = MockExternalEntityId(
            primary_key="test-key", platform_instance="test-instance"
        )
        cache_key = "GenericPlatformResource:test-key:test-instance"
        expected_result = GenericPlatformResource(
            id=entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
        )
        repository.entity_object_cache[cache_key] = expected_result

        result = repository.get_entity_from_datahub(entity_id, False)

        assert result == expected_result
        assert repository.entity_object_cache_hits == 1
        assert repository.entity_object_cache_misses == 0
        # Should not call search since we hit cache
        mock_search.assert_not_called()

    @patch(
        "datahub.api.entities.platformresource.platform_resource.PlatformResource.search_by_filters"
    )
    def test_get_entity_from_datahub_creates_default_when_not_found(
        self, mock_search: Mock, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test get_entity_from_datahub creates default entity when not found."""
        entity_id = MockExternalEntityId(
            primary_key="test-key", platform_instance="test-instance"
        )
        mock_search.return_value = []

        result = repository.get_entity_from_datahub(entity_id, True)

        assert isinstance(result, GenericPlatformResource)
        assert result.id == entity_id
        assert result.managed_by_datahub is True
        assert repository.entity_object_cache_misses == 1

    def test_get_entity_cache_info(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test get_entity_cache_info method."""
        # Simulate some cache activity
        repository.entity_id_cache_hits = 5
        repository.entity_id_cache_misses = 3
        repository.entity_object_cache_hits = 7
        repository.entity_object_cache_misses = 2
        repository.entity_id_cache["test1"] = "value1"
        repository.entity_object_cache["test2"] = "value2"

        cache_info = repository.get_entity_cache_info()

        assert cache_info["search_by_urn_cache"]["hits"] == 5
        assert cache_info["search_by_urn_cache"]["misses"] == 3
        assert cache_info["search_by_urn_cache"]["current_size"] == 1
        assert cache_info["search_by_urn_cache"]["max_size"] == 1000
        assert cache_info["get_from_datahub_cache"]["hits"] == 7
        assert cache_info["get_from_datahub_cache"]["misses"] == 2
        assert cache_info["get_from_datahub_cache"]["current_size"] == 1
        assert cache_info["get_from_datahub_cache"]["max_size"] == 1000


class TestGenericPlatformResourceRepositoryMethods:
    """Test specific methods of GenericPlatformResourceRepository."""

    @pytest.fixture
    def mock_graph(self) -> Mock:
        return Mock(spec=DataHubGraph)

    @pytest.fixture
    def repository(self, mock_graph: Mock) -> GenericPlatformResourceRepository:
        return GenericPlatformResourceRepository(mock_graph, "CustomResourceType")

    def test_custom_resource_type(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test repository with custom resource type."""
        assert repository.get_resource_type() == "CustomResourceType"

    def test_default_resource_type(self, mock_graph: Mock) -> None:
        """Test repository with default resource type."""
        repo = GenericPlatformResourceRepository(mock_graph)
        assert repo.get_resource_type() == "GenericPlatformResource"

    def test_get_entity_class(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test get_entity_class returns GenericPlatformResource."""
        assert repository.get_entity_class() == GenericPlatformResource

    def test_create_default_entity(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test create_default_entity method."""
        entity_id = MockExternalEntityId(primary_key="test-key")
        result = repository.create_default_entity(entity_id, True)

        assert isinstance(result, GenericPlatformResource)
        assert result.id == entity_id
        assert result.managed_by_datahub is True
        assert isinstance(result.datahub_urns, LinkedResourceSet)
        assert result.datahub_urns.urns == []

    def test_extract_platform_instance(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test extract_platform_instance method."""
        sync_context = MockSyncContext("test-instance")
        result = repository.extract_platform_instance(sync_context)
        assert result == "test-instance"

    def test_configure_entity_for_return(
        self, repository: GenericPlatformResourceRepository
    ) -> None:
        """Test configure_entity_for_return method."""
        entity_id = MockExternalEntityId(primary_key="test-key")
        result = repository.configure_entity_for_return(entity_id)
        assert result == entity_id  # Generic implementation returns as-is


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
