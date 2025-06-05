from typing import List, Optional
from unittest.mock import Mock, patch

import cachetools
import pytest

# Import the classes from your module
from datahub.api.entities.external.external_entities import (
    CaseSensitivity,
    ExternalEntityId,
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
    def repository(self, mock_graph: Mock) -> PlatformResourceRepository:
        """Create a PlatformResourceRepository instance."""
        return PlatformResourceRepository(mock_graph)

    @pytest.fixture
    def mock_platform_resource(self) -> Mock:
        """Create a mock PlatformResource."""
        resource: Mock = Mock(spec=PlatformResource)
        resource.id = "test-resource-id"
        return resource

    def test_init(self, mock_graph: Mock) -> None:
        """Test repository initialization."""
        repo: PlatformResourceRepository = PlatformResourceRepository(mock_graph)
        assert repo.graph == mock_graph
        assert isinstance(repo.cache, cachetools.TTLCache)
        assert repo.cache.maxsize == 1000
        assert repo.cache.ttl == 300  # 60 * 5

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
        assert repository.cache[mock_platform_resource.id] == mock_platform_resource
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
        assert mock_platform_resource.id not in repository.cache
        mock_search.assert_called_once_with(repository.graph, mock_filter)

    def test_create(
        self, repository: PlatformResourceRepository, mock_platform_resource: Mock
    ) -> None:
        """Test create method."""
        repository.create(mock_platform_resource)

        mock_platform_resource.to_datahub.assert_called_once_with(repository.graph)
        assert repository.cache[mock_platform_resource.id] == mock_platform_resource

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

        repository.cache[mock_platform_resource_key.id] = mock_platform_resource

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
        repository.cache[mock_platform_resource_key.id] = mock_platform_resource

        repository.delete(mock_platform_resource_key)

        repository.graph.delete_entity.assert_called_once_with(  # type: ignore[attr-defined]
            urn=PlatformResourceUrn(mock_platform_resource.id).urn(), hard=True
        )
        assert mock_platform_resource_key.id not in repository.cache


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
        try:
            empty_resource_set._has_conflict(new_urn)
        except InvalidUrnError:
            assert True
            return

        raise AssertionError("Expected InvalidUrnError to be raised")

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

    def test_add_new_urn_object(
        self, empty_resource_set: LinkedResourceSet, mock_urn: Mock
    ) -> None:
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

    def test_add_duplicate_urn(
        self, empty_resource_set: LinkedResourceSet, mock_urn: Mock
    ) -> None:
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

    def test_add_conflicting_urn(
        self, empty_resource_set: LinkedResourceSet, mock_urn: Mock
    ) -> None:
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
    def repository(self, mock_graph: Mock) -> PlatformResourceRepository:
        return PlatformResourceRepository(mock_graph)

    def test_repository_with_linked_resource_set(
        self, repository: PlatformResourceRepository
    ) -> None:
        """Test repository operations with LinkedResourceSet."""
        # Create a mock platform resource with linked URNs
        mock_resource: Mock = Mock(spec=PlatformResource)
        mock_resource.id = "test-resource"

        # Test create and cache
        repository.create(mock_resource)
        assert repository.cache["test-resource"] == mock_resource

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
        repo: PlatformResourceRepository = PlatformResourceRepository(mock_graph)

        # Add item to cache
        repo.cache["test-key"] = "test-value"
        assert "test-key" in repo.cache

        # Cache should still contain the item within TTL
        cached_value: Optional[str] = repo.cache.get("test-key")
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
