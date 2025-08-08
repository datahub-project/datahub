import pytest

from datahub.api.entities.external.external_entities import LinkedResourceSet
from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
    UnityCatalogTagSyncContext,
)
from datahub.metadata.urns import TagUrn


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
        assert UnityCatalogTagPlatformResourceId._RESOURCE_TYPE() == "UnityCatalogTagPlatformResource"

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
            tag_key="environment", tag_value="production", platform_instance="workspace1"
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
        assert platform_resource.resource_info.resource_type == "UnityCatalogTagPlatformResource"
        assert platform_resource.resource_info.primary_key == "team:data"
        assert platform_resource.resource_info.secondary_keys == ["urn:li:tag:team:data"]


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
        assert stored_resource.resource_info.resource_type == "UnityCatalogTagPlatformResource"
        assert stored_resource.resource_info.primary_key == "environment:production"
        assert stored_resource.resource_info.secondary_keys == ["urn:li:tag:environment:production"]