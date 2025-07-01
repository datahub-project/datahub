from unittest.mock import Mock, patch

from datahub.api.entities.external.external_entities import (
    LinkedResourceSet,
    PlatformResourceRepository,
)
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.source.aws.tag_entities import (
    LakeFormationTagPlatformResource,
    LakeFormationTagPlatformResourceId,
    LakeFormationTagSyncContext,
)
from datahub.metadata.urns import TagUrn


class TestLakeFormationTagSyncContext:
    """Tests for LakeFormationTagSyncContext class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default values."""
        context = LakeFormationTagSyncContext()
        assert context.platform_instance is None
        assert context.catalog is None

    def test_init_with_values(self) -> None:
        """Test initialization with provided values."""
        context = LakeFormationTagSyncContext(
            platform_instance="test_instance", catalog="test_catalog"
        )
        assert context.platform_instance == "test_instance"
        assert context.catalog == "test_catalog"


class TestLakeFormationTagPlatformResourceId:
    """Tests for LakeFormationTagPlatformResourceId class."""

    def test_init_with_required_fields(self) -> None:
        """Test initialization with required fields only."""
        resource_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key", platform_instance="test_instance"
        )
        assert resource_id.tag_key == "test_key"
        assert resource_id.tag_value is None
        assert resource_id.platform_instance == "test_instance"
        assert resource_id.catalog is None
        assert resource_id.exists_in_lake_formation is False
        assert resource_id.persisted is False

    def test_init_with_all_fields(self) -> None:
        """Test initialization with all fields provided."""
        resource_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
            exists_in_lake_formation=True,
            persisted=True,
        )
        assert resource_id.tag_key == "test_key"
        assert resource_id.tag_value == "test_value"
        assert resource_id.platform_instance == "test_instance"
        assert resource_id.catalog == "test_catalog"
        assert resource_id.exists_in_lake_formation is True
        assert resource_id.persisted is True

    def test_hash_method(self) -> None:
        """Test that hash method returns consistent values."""
        resource_id1 = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )
        resource_id2 = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )

        # Same objects should have same hash
        assert hash(resource_id1) == hash(resource_id2)

    def test_resource_type_method(self) -> None:
        """Test _RESOURCE_TYPE static method."""
        assert (
            LakeFormationTagPlatformResourceId._RESOURCE_TYPE()
            == "LakeFormationTagPlatformResource"
        )

    def test_to_platform_resource_key_with_catalog(self) -> None:
        """Test to_platform_resource_key method with catalog."""
        resource_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )

        key = resource_id.to_platform_resource_key()

        assert isinstance(key, PlatformResourceKey)
        assert key.platform == "glue"
        assert key.resource_type == "LakeFormationTagPlatformResource"
        assert key.primary_key == "test_catalog.test_key:test_value"
        assert key.platform_instance == "test_instance"

    def test_to_platform_resource_key_without_catalog(self) -> None:
        """Test to_platform_resource_key method without catalog."""
        resource_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
        )

        key = resource_id.to_platform_resource_key()

        assert key.primary_key == "test_key:test_value"

    def test_from_tag_with_no_existing_resource(self) -> None:
        """Test from_tag method when no existing resource is found."""
        mock_tag = Mock(spec=LakeFormationTag)
        mock_tag.key = "test_key"
        mock_tag.value = "test_value"
        mock_tag.to_datahub_tag_urn.return_value.urn.return_value = (
            "urn:li:tag:test_key:test_value"
        )

        mock_repo = Mock(spec=PlatformResourceRepository)

        # Mock search_by_urn to return None (no existing resource)
        with patch.object(
            LakeFormationTagPlatformResourceId, "search_by_urn", return_value=None
        ):
            result = LakeFormationTagPlatformResourceId.from_tag(
                tag=mock_tag,
                platform_instance="test_instance",
                platform_resource_repository=mock_repo,
                catalog="test_catalog",
            )

        assert result.tag_key == "test_key"
        assert result.tag_value == "test_value"
        assert result.platform_instance == "test_instance"
        assert result.catalog == "test_catalog"
        assert result.persisted is False

    def test_from_tag_with_existing_resource(self) -> None:
        """Test from_tag method when existing resource is found."""
        mock_tag = Mock(spec=LakeFormationTag)
        mock_tag.key = "test_key"
        mock_tag.value = "test_value"
        mock_tag.to_datahub_tag_urn.return_value.urn.return_value = (
            "urn:li:tag:test_key:test_value"
        )

        mock_repo = Mock(spec=PlatformResourceRepository)

        existing_resource = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
            persisted=True,
        )

        # Mock search_by_urn to return existing resource
        with patch.object(
            LakeFormationTagPlatformResourceId,
            "search_by_urn",
            return_value=existing_resource,
        ):
            result = LakeFormationTagPlatformResourceId.from_tag(
                tag=mock_tag,
                platform_instance="test_instance",
                platform_resource_repository=mock_repo,
                catalog="test_catalog",
            )

        assert result == existing_resource
        assert result.persisted is True

    def test_search_by_urn_no_results(self) -> None:
        """Test search_by_urn method when no results are found."""
        mock_repo = Mock(spec=PlatformResourceRepository)
        mock_repo.search_by_filter.return_value = []

        context = LakeFormationTagSyncContext(
            platform_instance="test_instance", catalog="test_catalog"
        )

        result = LakeFormationTagPlatformResourceId.search_by_urn(
            urn="urn:li:tag:test_key:test_value",
            platform_resource_repository=mock_repo,
            tag_sync_context=context,
        )

        assert result is None
        mock_repo.search_by_filter.assert_called_once()

    def test_search_by_urn_with_results(self) -> None:
        """Test search_by_urn method when results are found."""
        mock_repo = Mock(spec=PlatformResourceRepository)

        # Create mock platform resource
        mock_platform_resource = Mock()
        mock_platform_resource.resource_info = Mock()
        mock_platform_resource.resource_info.value = Mock()

        # Create mock LakeFormationTagPlatformResource
        mock_lf_tag_resource = Mock()
        mock_lf_tag_resource.id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )

        mock_platform_resource.resource_info.value.as_pydantic_object.return_value.dict.return_value = {
            "id": mock_lf_tag_resource.id,
            "datahub_urns": LinkedResourceSet(urns=[]),
            "managed_by_datahub": False,
            "allowed_values": None,
        }

        mock_repo.search_by_filter.return_value = [mock_platform_resource]

        context = LakeFormationTagSyncContext(
            platform_instance="test_instance", catalog="test_catalog"
        )

        with patch(
            "datahub.ingestion.source.aws.tag_entities.LakeFormationTagPlatformResource"
        ) as mock_lf_tag_platform_resource:
            mock_lf_tag_platform_resource.return_value = mock_lf_tag_resource

            result = LakeFormationTagPlatformResourceId.search_by_urn(
                urn="urn:li:tag:test_key:test_value",
                platform_resource_repository=mock_repo,
                tag_sync_context=context,
            )

        assert result is not None
        assert result.exists_in_lake_formation is True
        assert result.persisted is True

    def test_from_datahub_tag(self) -> None:
        """Test from_datahub_tag method."""
        mock_tag_urn = Mock(spec=TagUrn)
        mock_tag = Mock(spec=LakeFormationTag)
        mock_tag.key = "test_key"
        mock_tag.value = "test_value"

        context = LakeFormationTagSyncContext(
            platform_instance="test_instance", catalog="test_catalog"
        )

        with patch.object(LakeFormationTag, "from_urn", return_value=mock_tag):
            result = LakeFormationTagPlatformResourceId.from_datahub_tag(
                tag_urn=mock_tag_urn, tag_sync_context=context
            )

        assert result.tag_key == "test_key"
        assert result.tag_value == "test_value"
        assert result.platform_instance == "test_instance"
        assert result.catalog == "test_catalog"
        assert result.exists_in_lake_formation is False


class TestLakeFormationTagPlatformResource:
    """Tests for LakeFormationTagPlatformResource class."""

    def test_init(self) -> None:
        """Test initialization of LakeFormationTagPlatformResource."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
        )

        urns = LinkedResourceSet(urns=["urn:li:tag:test_key:test_value"])

        resource = LakeFormationTagPlatformResource(
            id=tag_id,
            datahub_urns=urns,
            managed_by_datahub=True,
            allowed_values=["value1", "value2"],
        )

        assert resource.id == tag_id
        assert resource.datahub_urns == urns
        assert resource.managed_by_datahub is True
        assert resource.allowed_values == ["value1", "value2"]

    def test_get_id(self) -> None:
        """Test get_id method."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key", platform_instance="test_instance"
        )

        resource = LakeFormationTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
            allowed_values=None,
        )

        assert resource.get_id() == tag_id

    def test_is_managed_by_datahub(self) -> None:
        """Test is_managed_by_datahub method."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key", platform_instance="test_instance"
        )

        resource = LakeFormationTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=True,
            allowed_values=None,
        )

        assert resource.is_managed_by_datahub() is True

    def test_datahub_linked_resources(self) -> None:
        """Test datahub_linked_resources method."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key", platform_instance="test_instance"
        )

        urns = LinkedResourceSet(urns=["urn:li:tag:test_key:test_value"])

        resource = LakeFormationTagPlatformResource(
            id=tag_id, datahub_urns=urns, managed_by_datahub=False, allowed_values=None
        )

        assert resource.datahub_linked_resources() == urns

    def test_as_platform_resource(self) -> None:
        """Test as_platform_resource method."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
        )

        urns = LinkedResourceSet(urns=["urn:li:tag:test_key:test_value"])

        resource = LakeFormationTagPlatformResource(
            id=tag_id, datahub_urns=urns, managed_by_datahub=False, allowed_values=None
        )

        with patch.object(PlatformResource, "create") as mock_create:
            mock_platform_resource = Mock(spec=PlatformResource)
            mock_create.return_value = mock_platform_resource

            result = resource.as_platform_resource()

            assert result == mock_platform_resource
            mock_create.assert_called_once_with(
                key=tag_id.to_platform_resource_key(),
                secondary_keys=["urn:li:tag:test_key:test_value"],
                value=resource,
            )

    def test_get_from_datahub_no_existing_resources(self) -> None:
        """Test get_from_datahub method when no existing resources are found."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
        )

        mock_repo = Mock(spec=PlatformResourceRepository)
        mock_repo.search_by_filter.return_value = []

        result = LakeFormationTagPlatformResource.get_from_datahub(
            lake_formation_tag_id=tag_id,
            platform_resource_repository=mock_repo,
            managed_by_datahub=True,
        )

        assert result.id == tag_id
        assert result.managed_by_datahub is True
        assert result.datahub_urns.urns == []
        assert result.allowed_values is None

    def test_get_from_datahub_with_existing_resources(self) -> None:
        """Test get_from_datahub method when existing resources are found."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )

        # Create mock platform resource
        mock_platform_resource = Mock()
        mock_platform_resource.resource_info = Mock()
        mock_platform_resource.resource_info.value = Mock()

        # Create expected LakeFormationTagPlatformResource
        expected_resource = LakeFormationTagPlatformResource(
            id=tag_id,
            datahub_urns=LinkedResourceSet(urns=["urn:li:tag:test_key:test_value"]),
            managed_by_datahub=True,
            allowed_values=["value1"],
        )

        mock_platform_resource.resource_info.value.as_pydantic_object.return_value.dict.return_value = {
            "id": tag_id,
            "datahub_urns": LinkedResourceSet(urns=["urn:li:tag:test_key:test_value"]),
            "managed_by_datahub": True,
            "allowed_values": ["value1"],
        }

        mock_repo = Mock(spec=PlatformResourceRepository)
        mock_repo.search_by_filter.return_value = [mock_platform_resource]

        with patch(
            "datahub.ingestion.source.aws.tag_entities.LakeFormationTagPlatformResource",
            return_value=expected_resource,
        ):
            result = LakeFormationTagPlatformResource.get_from_datahub(
                lake_formation_tag_id=tag_id,
                platform_resource_repository=mock_repo,
                managed_by_datahub=False,
            )

        assert result == expected_resource
        mock_repo.search_by_filter.assert_called_once()

    def test_get_from_datahub_with_mismatched_platform_instance(self) -> None:
        """Test get_from_datahub method when platform instance doesn't match."""
        tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="test_instance",
            catalog="test_catalog",
        )

        # Create mock platform resource with different platform instance
        different_tag_id = LakeFormationTagPlatformResourceId(
            tag_key="test_key",
            tag_value="test_value",
            platform_instance="different_instance",  # Different instance
            catalog="test_catalog",
        )

        mock_platform_resource = Mock()
        mock_platform_resource.resource_info = Mock()
        mock_platform_resource.resource_info.value = Mock()

        mock_lf_tag_resource = LakeFormationTagPlatformResource(
            id=different_tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=False,
            allowed_values=None,
        )

        mock_platform_resource.resource_info.value.as_pydantic_object.return_value.dict.return_value = {
            "id": different_tag_id,
            "datahub_urns": LinkedResourceSet(urns=[]),
            "managed_by_datahub": False,
            "allowed_values": None,
        }

        mock_repo = Mock(spec=PlatformResourceRepository)
        mock_repo.search_by_filter.return_value = [mock_platform_resource]

        with patch(
            "datahub.ingestion.source.aws.tag_entities.LakeFormationTagPlatformResource",
            return_value=mock_lf_tag_resource,
        ):
            result = LakeFormationTagPlatformResource.get_from_datahub(
                lake_formation_tag_id=tag_id,
                platform_resource_repository=mock_repo,
                managed_by_datahub=True,
            )

        # Should return new resource since platform instance doesn't match
        assert result.id == tag_id
        assert result.managed_by_datahub is True
        assert result.datahub_urns.urns == []
