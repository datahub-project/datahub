import json

from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.api.entities.platformresource.platform_resource import (
    ElasticPlatformResourceQuery,
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.utilities.search_utils import LogicalOperator


def test_platform_resource_dict():
    """
    Test that dictionaries are correctly converted to PlatformResource objects
    """

    platform_resource = PlatformResource.create(
        key=PlatformResourceKey(
            platform="test_platform",
            resource_type="test_resource_type",
            primary_key="test_primary_key",
        ),
        secondary_keys=["test_secondary_key"],
        value={"test_key": "test_value"},
    )

    mcps = [x for x in platform_resource.to_mcps()]
    status_mcp = [x for x in mcps if x.aspectName == "status"][0]
    assert isinstance(status_mcp.aspect, models.StatusClass)
    assert status_mcp.aspect.removed is False
    platform_resource_info_mcp = [
        x for x in mcps if x.aspectName == "platformResourceInfo"
    ][0]
    assert isinstance(
        platform_resource_info_mcp.aspect, models.PlatformResourceInfoClass
    )
    assert platform_resource_info_mcp.aspect.primaryKey == "test_primary_key"
    assert platform_resource_info_mcp.aspect.secondaryKeys == ["test_secondary_key"]
    assert platform_resource_info_mcp.aspect.resourceType == "test_resource_type"
    assert isinstance(
        platform_resource_info_mcp.aspect.value, models.SerializedValueClass
    )
    assert (
        platform_resource_info_mcp.aspect.value.contentType
        == models.SerializedValueContentTypeClass.JSON
    )
    assert platform_resource_info_mcp.aspect.value.blob == json.dumps(
        {"test_key": "test_value"}
    ).encode("utf-8")
    assert platform_resource_info_mcp.aspect.value.schemaType is None


def test_platform_resource_dict_removed():
    """
    Test that dictionaries are correctly converted to removed PlatformResource objects
    """

    platform_resource = PlatformResource.remove(
        key=PlatformResourceKey(
            platform="test_platform",
            resource_type="test_resource_type",
            primary_key="test_primary_key",
        )
    )

    mcps = [x for x in platform_resource.to_mcps()]
    status_mcp = [x for x in mcps if x.aspectName == "status"][0]
    assert isinstance(status_mcp.aspect, models.StatusClass)
    assert status_mcp.aspect.removed is True
    # no platformResourceInfo aspect should be present
    assert len([x for x in mcps if x.aspectName == "platformResourceInfo"]) == 0


def test_platform_resource_dictwrapper():
    """
    Test that Pegasus types (DictWrapper) are correctly converted to PlatformResource objects
    """

    user_editable_info: models.CorpUserEditableInfoClass = (
        models.CorpUserEditableInfoClass(
            pictureLink="https://example.com/picture.jpg",
            slack="U123456",
        )
    )
    platform_resource = PlatformResource.create(
        key=PlatformResourceKey(
            platform="slack",
            resource_type="user_info",
            primary_key="U123456",
        ),
        secondary_keys=["a@b.com"],
        value=user_editable_info,
    )

    mcps = [x for x in platform_resource.to_mcps()]
    status_mcp = [x for x in mcps if x.aspectName == "status"][0]
    assert isinstance(status_mcp.aspect, models.StatusClass)
    assert status_mcp.aspect.removed is False

    platform_resource_info_mcp = [
        x for x in mcps if x.aspectName == "platformResourceInfo"
    ][0]
    assert isinstance(
        platform_resource_info_mcp.aspect, models.PlatformResourceInfoClass
    )
    assert platform_resource_info_mcp.aspect.primaryKey == "U123456"
    assert platform_resource_info_mcp.aspect.secondaryKeys == ["a@b.com"]
    assert platform_resource_info_mcp.aspect.resourceType == "user_info"
    assert isinstance(
        platform_resource_info_mcp.aspect.value, models.SerializedValueClass
    )
    assert (
        platform_resource_info_mcp.aspect.value.contentType
        == models.SerializedValueContentTypeClass.JSON
    )
    assert (
        platform_resource_info_mcp.aspect.value.schemaType
        == models.SerializedValueSchemaTypeClass.PEGASUS
    )
    assert platform_resource_info_mcp.aspect.value.blob == json.dumps(
        user_editable_info.to_obj()
    ).encode("utf-8")
    assert (
        platform_resource_info_mcp.aspect.value.schemaRef
        == user_editable_info.RECORD_SCHEMA.fullname.replace("pegasus2avro.", "")
    )


def test_platform_resource_base_model():
    """
    Test that BaseModel objects are correctly converted to PlatformResource objects
    """

    class TestModel(BaseModel):
        test_field: str
        test_dict: dict
        test_int: int

    test_model = TestModel(
        test_field="test_field",
        test_dict={"test_key": "test_value"},
        test_int=42,
    )

    platform_resource = PlatformResource.create(
        key=PlatformResourceKey(
            platform="test_platform",
            resource_type="test_resource_type",
            primary_key="test_primary_key",
        ),
        secondary_keys=["test_secondary_key"],
        value=test_model,
    )

    mcps = [x for x in platform_resource.to_mcps()]

    status_mcp = [x for x in mcps if x.aspectName == "status"][0]
    assert isinstance(status_mcp.aspect, models.StatusClass)
    assert status_mcp.aspect.removed is False

    platform_resource_info_mcp = [
        x for x in mcps if x.aspectName == "platformResourceInfo"
    ][0]
    assert isinstance(
        platform_resource_info_mcp.aspect, models.PlatformResourceInfoClass
    )
    assert platform_resource_info_mcp.aspect.primaryKey == "test_primary_key"
    assert platform_resource_info_mcp.aspect.secondaryKeys == ["test_secondary_key"]
    assert platform_resource_info_mcp.aspect.resourceType == "test_resource_type"
    assert isinstance(
        platform_resource_info_mcp.aspect.value, models.SerializedValueClass
    )
    assert (
        platform_resource_info_mcp.aspect.value.contentType
        == models.SerializedValueContentTypeClass.JSON
    )
    assert platform_resource_info_mcp.aspect.value.blob == json.dumps(
        test_model.dict()
    ).encode("utf-8")
    assert platform_resource_info_mcp.aspect.value.schemaType == "JSON"
    assert platform_resource_info_mcp.aspect.value.schemaRef == TestModel.__name__


def test_platform_resource_filters():
    query = (
        ElasticPlatformResourceQuery.create_from()
        .group(LogicalOperator.AND)
        .add_field_match(PlatformResourceSearchFields.PRIMARY_KEY, "test_1")
        .add_field_match(PlatformResourceSearchFields.RESOURCE_TYPE, "server")
        .end()
    )
    assert query.build() == '(primaryKey:"test_1" AND resourceType:"server")'
