from pydantic import BaseModel

from datahub.api.entities.common.serialized_value import SerializedResourceValue


class MyTestModel(BaseModel):
    test_string_field: str
    test_int_field: int
    test_dict_field: dict


def test_base_model():
    test_base_model = MyTestModel(
        test_string_field="test_string_field",
        test_int_field=42,
        test_dict_field={"test_key": "test_value"},
    )
    assert isinstance(test_base_model, BaseModel)

    serialized_resource_value = SerializedResourceValue.create(test_base_model)

    assert serialized_resource_value.content_type == "JSON"
    # TODO: This is a bug in the code. The schema_type should not be None.
    assert serialized_resource_value.schema_type == "JSON"
    assert (
        serialized_resource_value.blob
        == b'{"test_string_field": "test_string_field", "test_int_field": 42, "test_dict_field": {"test_key": "test_value"}}'
    )
    assert serialized_resource_value.schema_ref == MyTestModel.__name__


def test_dictwrapper():
    from datahub.metadata.schema_classes import DatasetPropertiesClass

    dataset_properties = DatasetPropertiesClass(
        description="test_description",
        customProperties={"test_key": "test_value"},
    )

    serialized_resource_value = SerializedResourceValue.create(dataset_properties)

    assert serialized_resource_value.content_type == "JSON"
    assert serialized_resource_value.schema_type == "PEGASUS"
    assert (
        serialized_resource_value.blob
        == b'{"customProperties": {"test_key": "test_value"}, "description": "test_description", "tags": []}'
    )

    read_typed_resource_value = serialized_resource_value.as_pegasus_object()

    assert isinstance(read_typed_resource_value, DatasetPropertiesClass)

    assert read_typed_resource_value.description == "test_description"
    assert read_typed_resource_value.customProperties == {"test_key": "test_value"}
    assert read_typed_resource_value.tags == []


def test_raw_dictionary():
    test_object = {
        "test_string_field": "test_string_field",
        "test_int_field": 42,
        "test_dict_field": {"test_key": "test_value"},
    }

    serialized_resource_value = SerializedResourceValue.create(test_object)

    assert serialized_resource_value.content_type == "JSON"
    assert serialized_resource_value.schema_type is None
    assert (
        serialized_resource_value.blob
        == b'{"test_string_field": "test_string_field", "test_int_field": 42, "test_dict_field": {"test_key": "test_value"}}'
    )
    assert serialized_resource_value.schema_ref is None

    read_typed_resource_value = serialized_resource_value.as_raw_json()

    assert read_typed_resource_value == test_object
