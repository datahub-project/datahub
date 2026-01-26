"""Tests for ColumnValueAssertionClient."""

import pytest

from acryl_datahub_cloud.sdk.assertion.column_value_assertion import (
    ColumnValueAssertion,
)
from acryl_datahub_cloud.sdk.assertion_client.column_value import (
    ColumnValueAssertionClient,
    _is_sql_type_from_backend,
)
from acryl_datahub_cloud.sdk.assertion_input.column_value_assertion_input import (
    FailThresholdType,
    SqlExpression,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from tests.sdk.assertions.conftest import StubDataHubClient, StubEntityClient


class TestColumnValueAssertionClientCreation:
    """Tests for creating new column value assertions."""

    @pytest.fixture
    def stub_datahub_client(self) -> StubDataHubClient:
        return StubDataHubClient(entity_client=StubEntityClient())

    @pytest.fixture
    def client(
        self, stub_datahub_client: StubDataHubClient
    ) -> ColumnValueAssertionClient:
        return ColumnValueAssertionClient(stub_datahub_client)  # type: ignore[arg-type]

    def test_create_assertion_with_minimum_fields(
        self, client: ColumnValueAssertionClient
    ) -> None:
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            column_name="string_column",
            operator=models.AssertionStdOperatorClass.NOT_NULL,
            updated_by="urn:li:corpuser:test",
        )

        assert isinstance(result, ColumnValueAssertion)
        assert result.column_name == "string_column"
        assert result.operator == models.AssertionStdOperatorClass.NOT_NULL
        assert result.fail_threshold_type == FailThresholdType.COUNT
        assert result.fail_threshold_value == 0
        assert result.exclude_nulls is True

    def test_create_assertion_with_all_fields(
        self, client: ColumnValueAssertionClient
    ) -> None:
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            column_name="string_column",
            operator=models.AssertionStdOperatorClass.GREATER_THAN,
            criteria_parameters=5,
            transform="LENGTH",
            fail_threshold_type=FailThresholdType.PERCENTAGE,
            fail_threshold_value=5,
            exclude_nulls=False,
            display_name="Test Column Value Assertion",
            enabled=True,
            updated_by="urn:li:corpuser:test",
            schedule="0 0 * * *",
        )

        assert isinstance(result, ColumnValueAssertion)
        assert result.column_name == "string_column"
        assert result.operator == models.AssertionStdOperatorClass.GREATER_THAN
        assert result.criteria_parameters == 5
        assert result.transform == models.FieldTransformTypeClass.LENGTH
        assert result.fail_threshold_type == FailThresholdType.PERCENTAGE
        assert result.fail_threshold_value == 5
        assert result.exclude_nulls is False

    def test_create_assertion_with_range_operator(
        self, client: ColumnValueAssertionClient
    ) -> None:
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            column_name="number_column",
            operator=models.AssertionStdOperatorClass.BETWEEN,
            criteria_parameters=(0, 100),
            updated_by="urn:li:corpuser:test",
        )

        assert isinstance(result, ColumnValueAssertion)
        assert result.operator == models.AssertionStdOperatorClass.BETWEEN
        assert result.criteria_parameters == (0, 100)

    def test_create_assertion_requires_column_name(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                operator=models.AssertionStdOperatorClass.NOT_NULL,
                updated_by="urn:li:corpuser:test",
            )
        assert "column_name is required" in str(exc_info.value)

    def test_create_assertion_requires_operator(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                updated_by="urn:li:corpuser:test",
            )
        assert "operator is required" in str(exc_info.value)


class TestColumnValueAssertionClientUpdate:
    """Tests for updating existing column value assertions."""

    @pytest.fixture
    def stub_datahub_client(
        self, column_value_stub_datahub_client: StubDataHubClient
    ) -> StubDataHubClient:
        return column_value_stub_datahub_client

    @pytest.fixture
    def client(
        self, stub_datahub_client: StubDataHubClient
    ) -> ColumnValueAssertionClient:
        return ColumnValueAssertionClient(stub_datahub_client)  # type: ignore[arg-type]

    def test_update_existing_assertion(
        self, client: ColumnValueAssertionClient, any_assertion_urn: str
    ) -> None:
        # Update an existing assertion
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            urn=any_assertion_urn,
            display_name="Updated Column Value Assertion",
            updated_by="urn:li:corpuser:test",
        )

        assert isinstance(result, ColumnValueAssertion)
        # Properties from existing assertion should be preserved
        assert result.column_name == "string_column"
        assert result.operator == models.AssertionStdOperatorClass.GREATER_THAN

    def test_update_existing_assertion_with_new_values(
        self, client: ColumnValueAssertionClient, any_assertion_urn: str
    ) -> None:
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
            urn=any_assertion_urn,
            fail_threshold_value=10,
            exclude_nulls=False,
            updated_by="urn:li:corpuser:test",
        )

        assert isinstance(result, ColumnValueAssertion)
        assert result.fail_threshold_value == 10
        assert result.exclude_nulls is False

    def test_update_assertion_dataset_urn_mismatch(
        self, client: ColumnValueAssertionClient, any_assertion_urn: str
    ) -> None:
        """Test that updating an assertion with a different dataset URN raises an error."""
        # The existing assertion is for snowflake,table_name,PROD
        # Try to update it with a different dataset URN
        with pytest.raises(SDKUsageError, match="Dataset URN mismatch"):
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,different_table,PROD)",
                urn=any_assertion_urn,
                updated_by="urn:li:corpuser:test",
            )


class TestColumnValueAssertionClientMerging:
    """Tests for field extraction/merging from existing assertions.

    Note: These tests target internal methods (_extract_fields_from_existing_assertion)
    because the extraction logic is complex enough to warrant direct unit testing.
    Testing only through the public API would make it difficult to verify edge cases
    like missing nested fields, None entities, and user-input-wins merge semantics.

    This pattern cannot be generically extracted into a helper util across all assertion type clients.
    """

    @pytest.fixture
    def stub_datahub_client(
        self, column_value_stub_datahub_client: StubDataHubClient
    ) -> StubDataHubClient:
        return column_value_stub_datahub_client

    @pytest.fixture
    def client(
        self, stub_datahub_client: StubDataHubClient
    ) -> ColumnValueAssertionClient:
        return ColumnValueAssertionClient(stub_datahub_client)  # type: ignore[arg-type]

    def test_extract_fields_from_existing_assertion(
        self,
        client: ColumnValueAssertionClient,
        column_value_assertion_entity_with_all_fields,
    ) -> None:
        (
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
            gms_criteria_type_info,
        ) = client._extract_fields_from_existing_assertion(
            maybe_assertion_entity=column_value_assertion_entity_with_all_fields,
            column_name=None,
            operator=None,
            criteria_parameters=None,
            transform=None,
            fail_threshold_type=None,
            fail_threshold_value=None,
            exclude_nulls=None,
        )

        assert column_name == "string_column"
        assert operator == models.AssertionStdOperatorClass.GREATER_THAN
        # The extracted value is a string "5" (as stored in GMS), not converted to int
        assert criteria_parameters == "5"
        assert transform == models.FieldTransformTypeClass.LENGTH
        assert (
            fail_threshold_type == models.FieldValuesFailThresholdTypeClass.PERCENTAGE
        )
        assert fail_threshold_value == 5
        assert exclude_nulls is True

    def test_extract_fields_preserves_user_input(
        self,
        client: ColumnValueAssertionClient,
        column_value_assertion_entity_with_all_fields,
    ) -> None:
        # User-provided values should not be overwritten
        (
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
            _,
        ) = client._extract_fields_from_existing_assertion(
            maybe_assertion_entity=column_value_assertion_entity_with_all_fields,
            column_name="user_column",
            operator=models.AssertionStdOperatorClass.NOT_NULL,
            criteria_parameters=None,
            transform=None,
            fail_threshold_type=FailThresholdType.COUNT,
            fail_threshold_value=0,
            exclude_nulls=False,
        )

        # User-provided values should be preserved
        assert column_name == "user_column"
        assert operator == models.AssertionStdOperatorClass.NOT_NULL
        assert fail_threshold_type == FailThresholdType.COUNT
        assert fail_threshold_value == 0
        assert exclude_nulls is False

    def test_extract_fields_from_none_assertion(
        self, client: ColumnValueAssertionClient
    ) -> None:
        (
            column_name,
            operator,
            criteria_parameters,
            transform,
            fail_threshold_type,
            fail_threshold_value,
            exclude_nulls,
            gms_criteria_type_info,
        ) = client._extract_fields_from_existing_assertion(
            maybe_assertion_entity=None,
            column_name="test_column",
            operator=models.AssertionStdOperatorClass.NOT_NULL,
            criteria_parameters=None,
            transform=None,
            fail_threshold_type=None,
            fail_threshold_value=None,
            exclude_nulls=None,
        )

        # All values should be as provided (or None)
        assert column_name == "test_column"
        assert operator == models.AssertionStdOperatorClass.NOT_NULL
        assert criteria_parameters is None
        assert transform is None
        assert fail_threshold_type is None
        assert fail_threshold_value is None
        assert exclude_nulls is None
        assert gms_criteria_type_info is None


class TestColumnValueAssertionClientValidation:
    """Tests for validation errors in ColumnValueAssertionClient."""

    @pytest.fixture
    def stub_datahub_client(self) -> StubDataHubClient:
        return StubDataHubClient(entity_client=StubEntityClient())

    @pytest.fixture
    def client(
        self, stub_datahub_client: StubDataHubClient
    ) -> ColumnValueAssertionClient:
        return ColumnValueAssertionClient(stub_datahub_client)  # type: ignore[arg-type]

    def test_invalid_operator_for_column_type(
        self, client: ColumnValueAssertionClient
    ) -> None:
        # REGEX_MATCH is not valid for NUMBER columns
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                operator=models.AssertionStdOperatorClass.REGEX_MATCH,
                criteria_parameters="^[0-9]+$",
                updated_by="urn:li:corpuser:test",
            )
        assert "Operator" in str(exc_info.value)
        assert "not allowed" in str(exc_info.value)

    def test_length_transform_invalid_for_non_string(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                operator=models.AssertionStdOperatorClass.NOT_NULL,
                transform="LENGTH",
                updated_by="urn:li:corpuser:test",
            )
        assert "LENGTH transform is only valid for STRING columns" in str(
            exc_info.value
        )

    def test_percentage_threshold_over_100_invalid(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                operator=models.AssertionStdOperatorClass.NOT_NULL,
                fail_threshold_type=FailThresholdType.PERCENTAGE,
                fail_threshold_value=101,
                updated_by="urn:li:corpuser:test",
            )
        assert "between 0 and 100" in str(exc_info.value)

    def test_negative_threshold_value_invalid(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                operator=models.AssertionStdOperatorClass.NOT_NULL,
                fail_threshold_value=-1,
                updated_by="urn:li:corpuser:test",
            )
        assert "must be non-negative" in str(exc_info.value)


class TestIsSqlTypeFromBackend:
    """Tests for _is_sql_type_from_backend helper function."""

    def test_returns_true_for_sql_type(self) -> None:
        gms_type_info = (
            "SELECT id FROM customers",
            models.AssertionStdParameterTypeClass.SQL,
        )
        criteria_parameters = "SELECT id FROM customers"
        assert _is_sql_type_from_backend(gms_type_info, criteria_parameters) is True

    def test_returns_false_for_none_gms_type(self) -> None:
        assert _is_sql_type_from_backend(None, "some value") is False

    def test_returns_false_for_none_criteria_parameters(self) -> None:
        gms_type_info = ("value", models.AssertionStdParameterTypeClass.STRING)
        assert _is_sql_type_from_backend(gms_type_info, None) is False

    def test_returns_false_for_range_type(self) -> None:
        # Range type has nested tuples: ((min, max), (min_type, max_type))
        gms_type_info = (
            ("0", "100"),
            (
                models.AssertionStdParameterTypeClass.NUMBER,
                models.AssertionStdParameterTypeClass.NUMBER,
            ),
        )
        assert _is_sql_type_from_backend(gms_type_info, (0, 100)) is False

    def test_returns_false_for_non_sql_type(self) -> None:
        gms_type_info = (
            "just a string",
            models.AssertionStdParameterTypeClass.STRING,
        )
        assert _is_sql_type_from_backend(gms_type_info, "just a string") is False


class TestColumnValueAssertionClientWithSql:
    """Tests for creating column value assertions with SQL expressions."""

    @pytest.fixture
    def stub_datahub_client(self) -> StubDataHubClient:
        return StubDataHubClient(entity_client=StubEntityClient())

    @pytest.fixture
    def client(
        self, stub_datahub_client: StubDataHubClient
    ) -> ColumnValueAssertionClient:
        return ColumnValueAssertionClient(stub_datahub_client)  # type: ignore[arg-type]

    def test_create_assertion_with_sql_expression(
        self, client: ColumnValueAssertionClient
    ) -> None:
        sql_expr = SqlExpression("SELECT id FROM valid_customers WHERE active = true")
        result = client.sync_column_value_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            column_name="string_column",
            operator=models.AssertionStdOperatorClass.IN,
            criteria_parameters=sql_expr,
            updated_by="urn:li:corpuser:test",
        )

        assert isinstance(result, ColumnValueAssertion)
        assert result.column_name == "string_column"
        assert result.operator == models.AssertionStdOperatorClass.IN
        # When reading back from entities, SQL parameters are wrapped in SqlExpression
        assert isinstance(result.criteria_parameters, SqlExpression)
        assert result.criteria_parameters.sql == sql_expr.sql
        assert result.is_sql_criteria is True

    def test_sql_expression_invalid_with_null_operator(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                operator=models.AssertionStdOperatorClass.NULL,
                criteria_parameters=SqlExpression("SELECT id FROM customers"),
                updated_by="urn:li:corpuser:test",
            )
        assert "SQL expressions cannot be used" in str(exc_info.value)

    def test_sql_expression_invalid_with_between_operator(
        self, client: ColumnValueAssertionClient
    ) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            client.sync_column_value_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                operator=models.AssertionStdOperatorClass.BETWEEN,
                criteria_parameters=SqlExpression("SELECT val FROM range_table"),
                updated_by="urn:li:corpuser:test",
            )
        assert "SQL expressions cannot be used with range operator" in str(
            exc_info.value
        )
