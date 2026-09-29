"""Unit tests for field reference parser."""

from datahub.ingestion.source.snowplow.utils.field_reference_parser import (
    FieldReferenceConfig,
    FieldReferenceParser,
    JsonField,
    PojoField,
)


class TestJsonFieldJsonPathParsing:
    """Tests for JSONPath parsing in JsonField."""

    def test_simple_field_path(self):
        """Parse simple field path like $.email."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.email",
        )
        assert json_field.extracted_fields == ["email"]

    def test_nested_field_path(self):
        """Parse nested field path like $.customer.email."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.customer.email",
        )
        assert json_field.extracted_fields == ["customer.email"]

    def test_deeply_nested_path(self):
        """Parse deeply nested path like $.data.user.contact.email."""
        json_field = JsonField(
            json_field="unstruct_event",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.data.user.contact.email",
        )
        assert json_field.extracted_fields == ["data.user.contact.email"]

    def test_bracket_notation_single_field(self):
        """Parse bracket notation with single field."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.['email']",
        )
        assert json_field.extracted_fields == ["email"]

    def test_bracket_notation_multiple_fields(self):
        """Parse bracket notation with multiple fields like $.['clientId', 'userId']."""
        json_field = JsonField(
            json_field="unstruct_event",
            schema_criterion="iglu:com.google.analytics/user/jsonschema/1-0-0",
            json_path="$.['clientId', 'userId']",
        )
        assert json_field.extracted_fields == ["clientId", "userId"]

    def test_bracket_notation_with_prefix(self):
        """Parse bracket notation with prefix like $.data.['field1', 'field2']."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.data.['field1', 'field2']",
        )
        assert json_field.extracted_fields == ["data.field1", "data.field2"]

    def test_bracket_notation_double_quotes(self):
        """Parse bracket notation with double quotes."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path='$.["email", "phone"]',
        )
        assert json_field.extracted_fields == ["email", "phone"]

    def test_empty_json_path(self):
        """Handle empty JSONPath."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="",
        )
        assert json_field.extracted_fields == []

    def test_invalid_json_path_no_dollar(self):
        """Handle JSONPath without $ prefix."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="email",
        )
        assert json_field.extracted_fields == []

    def test_just_dollar_sign(self):
        """Handle JSONPath with just $."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$",
        )
        assert json_field.extracted_fields == []


class TestJsonFieldLeafNames:
    """Tests for get_leaf_field_names method."""

    def test_leaf_from_nested_path(self):
        """Get leaf field name from nested path."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.customer.email",
        )
        assert json_field.get_leaf_field_names() == ["email"]

    def test_leaf_from_simple_path(self):
        """Get leaf field name from simple path."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.email",
        )
        assert json_field.get_leaf_field_names() == ["email"]

    def test_leaf_from_multiple_paths(self):
        """Get leaf names from multiple fields."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/test/jsonschema/1-0-0",
            json_path="$.data.['email', 'phone']",
        )
        assert json_field.get_leaf_field_names() == ["email", "phone"]


class TestJsonFieldSchemaMatching:
    """Tests for schema criterion matching."""

    def test_exact_match(self):
        """Match exact schema URI."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-0-0",
            json_path="$.email",
        )
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-0")

    def test_wildcard_version(self):
        """Match with wildcard version pattern."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-*-*",
            json_path="$.email",
        )
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-0")
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-5")
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-2-3")
        assert not json_field.matches_schema("iglu:com.acme/checkout/jsonschema/2-0-0")

    def test_wildcard_minor_patch(self):
        """Match with wildcard for minor and patch."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-0-*",
            json_path="$.email",
        )
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-0")
        assert json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-9")
        assert not json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-1-0")

    def test_no_match_different_vendor(self):
        """No match for different vendor."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-*-*",
            json_path="$.email",
        )
        assert not json_field.matches_schema("iglu:com.other/checkout/jsonschema/1-0-0")

    def test_no_match_different_name(self):
        """No match for different schema name."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-*-*",
            json_path="$.email",
        )
        assert not json_field.matches_schema("iglu:com.acme/cart/jsonschema/1-0-0")

    def test_empty_inputs(self):
        """Handle empty inputs."""
        json_field = JsonField(
            json_field="contexts",
            schema_criterion="",
            json_path="$.email",
        )
        assert not json_field.matches_schema("iglu:com.acme/checkout/jsonschema/1-0-0")

        json_field2 = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-*-*",
            json_path="$.email",
        )
        assert not json_field2.matches_schema("")


class TestFieldReferenceParserFromPiiKey:
    """Tests for FieldReferenceParser with PII enrichment configurations."""

    def test_parse_standard_pojo_format(self):
        """Parse standard Snowplow format with pojo array using 'field' key."""
        parameters = {
            "pii": {
                "pojo": [{"field": "user_id"}, {"field": "user_ipaddress"}],
            }
        }
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 2
        assert config.pojo_fields[0].field_name == "user_id"
        assert config.pojo_fields[1].field_name == "user_ipaddress"
        assert len(config.json_fields) == 0

    def test_parse_pojo_with_ppiField_key(self):
        """Parse pojo array with 'ppiField' key variant."""
        parameters = {"pii": {"pojo": [{"ppiField": "email"}, {"ppiField": "phone"}]}}
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 2
        assert config.pojo_fields[0].field_name == "email"
        assert config.pojo_fields[1].field_name == "phone"

    def test_parse_legacy_fieldName_format(self):
        """Parse legacy format with fieldName key."""
        parameters = {"pii": {"pojo": [{"fieldName": "email"}, {"fieldName": "ssn"}]}}
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 2
        assert config.pojo_fields[0].field_name == "email"
        assert config.pojo_fields[1].field_name == "ssn"

    def test_parse_legacy_list_format(self):
        """Parse legacy list format [{'fieldName': ...}]."""
        parameters = {"pii": [{"fieldName": "email"}, {"fieldName": "phone"}]}
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 2
        assert config.pojo_fields[0].field_name == "email"
        assert config.pojo_fields[1].field_name == "phone"

    def test_parse_legacy_fieldNames_dict_format(self):
        """Parse legacy dict format with fieldNames array."""
        parameters = {"pii": {"fieldNames": ["email", "phone", "ssn"]}}
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 3
        field_names = {f.field_name for f in config.pojo_fields}
        assert field_names == {"email", "phone", "ssn"}

    def test_parse_standard_json_format(self):
        """Parse standard Snowplow format with json array."""
        parameters = {
            "pii": {
                "json": [
                    {
                        "field": "contexts",
                        "schemaCriterion": "iglu:com.acme/checkout/jsonschema/1-*-*",
                        "jsonPath": "$.customer.email",
                    },
                    {
                        "field": "unstruct_event",
                        "schemaCriterion": "iglu:com.ga/user/jsonschema/1-*-*",
                        "jsonPath": "$.['clientId', 'userId']",
                    },
                ]
            }
        }
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.json_fields) == 2
        assert config.json_fields[0].json_field == "contexts"
        assert (
            config.json_fields[0].schema_criterion
            == "iglu:com.acme/checkout/jsonschema/1-*-*"
        )
        assert config.json_fields[0].extracted_fields == ["customer.email"]
        assert config.json_fields[1].json_field == "unstruct_event"
        assert config.json_fields[1].extracted_fields == ["clientId", "userId"]

    def test_parse_mixed_pojo_and_json(self):
        """Parse config with both pojo and json fields."""
        parameters = {
            "pii": {
                "pojo": [{"field": "user_id"}],
                "json": [
                    {
                        "field": "contexts",
                        "schemaCriterion": "iglu:com.acme/test/jsonschema/1-*-*",
                        "jsonPath": "$.email",
                    }
                ],
            }
        }
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.pojo_fields) == 1
        assert len(config.json_fields) == 1
        assert config.pojo_fields[0].field_name == "user_id"
        assert config.json_fields[0].extracted_fields == ["email"]

    def test_parse_incomplete_json_config_skipped(self):
        """Incomplete JSON config (missing required fields) is skipped."""
        parameters = {
            "pii": {
                "json": [
                    {"field": "contexts"},  # Missing schemaCriterion and jsonPath
                    {
                        "field": "contexts",
                        "schemaCriterion": "iglu:com.acme/test/jsonschema/1-*-*",
                    },  # Missing jsonPath
                    {
                        "field": "contexts",
                        "schemaCriterion": "iglu:com.acme/test/jsonschema/1-*-*",
                        "jsonPath": "$.email",
                    },  # Complete - should be parsed
                ]
            }
        }
        config = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(config.json_fields) == 1
        assert config.json_fields[0].extracted_fields == ["email"]

    def test_parse_empty_parameters(self):
        """Handle empty parameters."""
        config = FieldReferenceParser.parse_from_key({}, "pii")
        assert config.is_empty()

    def test_parse_none_parameters(self):
        """Handle None parameters."""
        config = FieldReferenceParser.parse_from_key(None, "pii")  # type: ignore
        assert config.is_empty()

    def test_parse_no_pii_key(self):
        """Handle parameters without pii key."""
        parameters = {"other": "value"}
        config = FieldReferenceParser.parse_from_key(parameters, "pii")
        assert config.is_empty()


class TestFieldReferenceConfig:
    """Tests for FieldReferenceConfig methods."""

    def test_get_all_field_names(self):
        """Get all field names combines pojo and json leaf names."""
        config = FieldReferenceConfig(
            pojo_fields=[PojoField("user_id"), PojoField("user_ipaddress")],
            json_fields=[
                JsonField(
                    json_field="contexts",
                    schema_criterion="iglu:com.acme/test/jsonschema/1-*-*",
                    json_path="$.customer.email",
                )
            ],
        )
        all_names = config.get_all_field_names()
        assert all_names == {"user_id", "user_ipaddress", "email"}

    def test_get_pojo_field_names(self):
        """Get only pojo field names."""
        config = FieldReferenceConfig(
            pojo_fields=[PojoField("user_id"), PojoField("user_ipaddress")],
            json_fields=[
                JsonField(
                    json_field="contexts",
                    schema_criterion="iglu:com.acme/test/jsonschema/1-*-*",
                    json_path="$.email",
                )
            ],
        )
        pojo_names = config.get_pojo_field_names()
        assert pojo_names == {"user_id", "user_ipaddress"}

    def test_get_json_fields_for_schema(self):
        """Get JSON fields matching a specific schema."""
        json_field_checkout = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/checkout/jsonschema/1-*-*",
            json_path="$.email",
        )
        json_field_cart = JsonField(
            json_field="contexts",
            schema_criterion="iglu:com.acme/cart/jsonschema/1-*-*",
            json_path="$.phone",
        )
        config = FieldReferenceConfig(
            json_fields=[json_field_checkout, json_field_cart]
        )

        matching = config.get_json_fields_for_schema(
            "iglu:com.acme/checkout/jsonschema/1-0-0"
        )
        assert len(matching) == 1
        assert matching[0].extracted_fields == ["email"]

    def test_is_empty(self):
        """Test is_empty method."""
        assert FieldReferenceConfig().is_empty()
        assert not FieldReferenceConfig(pojo_fields=[PojoField("user_id")]).is_empty()
        assert not FieldReferenceConfig(
            json_fields=[
                JsonField(
                    json_field="contexts",
                    schema_criterion="iglu:com.acme/test/jsonschema/1-*-*",
                    json_path="$.email",
                )
            ]
        ).is_empty()


class TestFieldReferenceParser:
    """Tests for the generic FieldReferenceParser.

    This parser handles the common field reference pattern used by multiple
    Snowplow enrichments: PII Pseudonymization, SQL Enrichment, API Request.
    """

    def test_parse_direct_config(self):
        """Parse config directly without a key (like SQL enrichment inputs)."""
        config = {
            "pojo": [{"field": "user_id"}, {"field": "app_id"}],
            "json": [
                {
                    "field": "contexts",
                    "schemaCriterion": "iglu:com.acme/user/jsonschema/1-*-*",
                    "jsonPath": "$.email",
                }
            ],
        }
        result = FieldReferenceParser.parse(config)

        assert len(result.pojo_fields) == 2
        assert result.pojo_fields[0].field_name == "user_id"
        assert result.pojo_fields[1].field_name == "app_id"
        assert len(result.json_fields) == 1
        assert result.json_fields[0].extracted_fields == ["email"]

    def test_parse_from_key_pii(self):
        """Parse from 'pii' key (PII enrichment)."""
        parameters = {
            "pii": {
                "pojo": [{"field": "user_ipaddress"}],
            }
        }
        result = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert len(result.pojo_fields) == 1
        assert result.pojo_fields[0].field_name == "user_ipaddress"

    def test_parse_from_key_inputs(self):
        """Parse from 'inputs' key (SQL/API enrichment)."""
        parameters = {
            "inputs": {
                "pojo": [{"field": "collector_tstamp"}],
                "json": [
                    {
                        "field": "unstruct_event",
                        "schemaCriterion": "iglu:com.acme/event/jsonschema/1-*-*",
                        "jsonPath": "$.order_id",
                    }
                ],
            }
        }
        result = FieldReferenceParser.parse_from_key(parameters, "inputs")

        assert len(result.pojo_fields) == 1
        assert result.pojo_fields[0].field_name == "collector_tstamp"
        assert len(result.json_fields) == 1
        assert result.json_fields[0].extracted_fields == ["order_id"]

    def test_parse_from_key_missing_key(self):
        """Return empty config when key is missing."""
        parameters = {"other_key": {"pojo": [{"field": "user_id"}]}}
        result = FieldReferenceParser.parse_from_key(parameters, "pii")

        assert result.is_empty()

    def test_parse_from_key_empty_parameters(self):
        """Return empty config for empty parameters."""
        result = FieldReferenceParser.parse_from_key({}, "pii")
        assert result.is_empty()

    def test_parse_from_key_none_parameters(self):
        """Return empty config for None parameters."""
        result = FieldReferenceParser.parse_from_key(None, "pii")  # type: ignore
        assert result.is_empty()

    def test_parse_without_legacy_formats(self):
        """Parse without legacy format support."""
        config = {"fieldNames": ["email", "phone"]}
        result = FieldReferenceParser.parse(config, include_legacy_formats=False)

        assert result.is_empty()

    def test_parse_with_legacy_formats(self):
        """Parse with legacy format support (default)."""
        config = {"fieldNames": ["email", "phone"]}
        result = FieldReferenceParser.parse(config, include_legacy_formats=True)

        assert len(result.pojo_fields) == 2

    def test_parse_sql_enrichment_style_config(self):
        """Parse SQL enrichment style config with multiple inputs."""
        inputs_config = {
            "pojo": [
                {"field": "user_id"},
                {"field": "geo_country"},
            ],
            "json": [
                {
                    "field": "contexts",
                    "schemaCriterion": "iglu:com.acme/customer/jsonschema/1-*-*",
                    "jsonPath": "$.customer_id",
                },
                {
                    "field": "unstruct_event",
                    "schemaCriterion": "iglu:com.acme/order/jsonschema/1-*-*",
                    "jsonPath": "$.['order_id', 'product_id']",
                },
            ],
        }
        result = FieldReferenceParser.parse(inputs_config)

        assert len(result.pojo_fields) == 2
        assert len(result.json_fields) == 2
        assert result.json_fields[0].extracted_fields == ["customer_id"]
        assert result.json_fields[1].extracted_fields == ["order_id", "product_id"]
