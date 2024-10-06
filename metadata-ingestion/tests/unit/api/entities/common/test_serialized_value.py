from pydantic import BaseModel

from datahub.api.entities.common.serialized_value import TypedResourceValue


class TestModel(BaseModel):
    test_string_field: str
    test_int_field: int
    test_dict_field: dict


def test_base_model():

    test_base_model = TestModel(
        test_string_field="test_string_field",
        test_int_field=42,
        test_dict_field={"test_key": "test_value"},
    )
    assert isinstance(test_base_model, BaseModel)

    typed_resource_value = TypedResourceValue(object=test_base_model)

    serialized_resource_value = typed_resource_value.to_serialized_resource_value()

    assert serialized_resource_value.content_type == "JSON"
    # TODO: This is a bug in the code. The schema_type should not be None.
    assert serialized_resource_value.schema_type is None
    assert (
        serialized_resource_value.blob
        == b'{"test_string_field": "test_string_field", "test_int_field": 42, "test_dict_field": {"test_key": "test_value"}}'
    )
    assert serialized_resource_value.schema_ref is None
