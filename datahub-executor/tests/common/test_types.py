from typing import Any, Dict

import pytest
from pydantic import ValidationError

from datahub_executor.common.types import FieldValuesAssertion


class TestFieldValuesAssertion:
    def setup_method(self) -> None:
        self.data: Dict[str, Any] = {
            "field": {
                "path": "id",
                "type": "STRING",
                "nativeType": "STRING",
            },
            "transform": {"type": "LENGTH"},
            "operator": "EQUAL_TO",
            "parameters": {
                "value": {
                    "value": "10",
                    "type": "NUMBER",
                }
            },
            "failThreshold": {
                "type": "COUNT",
                "value": "1",
            },
            "excludeNulls": "false",
        }

    def test_valid_data(self) -> None:
        field_values_assertion = FieldValuesAssertion.model_validate(self.data)
        assert field_values_assertion.fail_threshold.value == 1

    def test_empty_value_less_than(self) -> None:
        self.data["operator"] = "LESS_THAN"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_less_than_or_equal(self) -> None:
        self.data["operator"] = "LESS_THAN_OR_EQUAL_TO"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_greater_than(self) -> None:
        self.data["operator"] = "GREATER_THAN"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_greater_than_or_equal(self) -> None:
        self.data["operator"] = "GREATER_THAN_OR_EQUAL_TO"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_equal(self) -> None:
        self.data["operator"] = "EQUAL_TO"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_not_equal(self) -> None:
        self.data["operator"] = "NOT_EQUAL_TO"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_type_not_equal(self) -> None:
        self.data["operator"] = "NOT_EQUAL_TO"
        self.data["parameters"]["value"]["type"] = "LIST"
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_contain(self) -> None:
        self.data["operator"] = "CONTAIN"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_end_with(self) -> None:
        self.data["operator"] = "END_WITH"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_start_widh(self) -> None:
        self.data["operator"] = "START_WITH"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_in(self) -> None:
        self.data["operator"] = "IN"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_not_in(self) -> None:
        self.data["operator"] = "NOT_IN"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_min_value_between(self) -> None:
        self.data["operator"] = "BETWEEN"
        self.data["parameters"]["minValue"] = None
        self.data["parameters"]["maxValue"] = {}
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_max_value_between(self) -> None:
        self.data["operator"] = "BETWEEN"
        self.data["parameters"]["minValue"] = {}
        self.data["parameters"]["maxValue"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_empty_value_regex(self) -> None:
        self.data["operator"] = "REGEX_MATCH"
        self.data["parameters"]["value"] = None
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_type_regex(self) -> None:
        self.data["operator"] = "REGEX_MATCH"
        self.data["parameters"]["value"]["type"] = "LIST"
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_field_type_transform(self) -> None:
        self.data["field"] = "NUMBER"
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_type_transform_value(self) -> None:
        self.data["parameters"]["value"]["type"] = "LIST"
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_type_transform_min_value(self) -> None:
        self.data["parameters"]["value"] = None
        self.data["parameters"]["minValue"] = {"type": "STRING"}
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)

    def test_invalid_type_transform_max_value(self) -> None:
        self.data["parameters"]["value"] = None
        self.data["parameters"]["minValue"] = None
        self.data["parameters"]["maxValue"] = {"type": "LIST"}
        with pytest.raises(ValidationError):
            FieldValuesAssertion.model_validate(self.data)
