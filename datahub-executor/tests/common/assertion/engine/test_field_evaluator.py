from unittest.mock import ANY, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

from datahub_executor.common.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_executor.common.assertion.types import (
    AssertionDatabaseParams,
    AssertionState,
    AssertionStateType,
)
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
    InvalidParametersException,
    SourceConnectionErrorException,
)
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.resolver.resolver import MetricResolver
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.source.source import Source
from datahub_executor.common.state.assertion_state_provider import (
    AssertionStateProvider,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationSpec,
    AssertionEvaluationSpecContext,
    AssertionInferenceDetails,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
    FieldAssertion,
    FieldAssertionType,
    FieldMetricAssertion,
    FieldMetricType,
    FieldValuesAssertion,
    FieldValuesFailThreshold,
    FieldValuesFailThresholdType,
    FreshnessFieldSpec,
    SchemaFieldSpec,
)

TEST_START = 1687643700064
TEST_END = 1687644000064
TEST_TIME = "2023-10-04 17:12:33.702000"

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)


class TestFieldEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=AssertionStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.metric_resolver = Mock(spec=MetricResolver)
        self.metric_client = Mock(spec=MetricClient)
        self.monitor_client = Mock(spec=MonitorClient)

        self.evaluator = FieldAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
            self.metric_resolver,
            self.metric_client,
            self.monitor_client,
        )

        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.FIELD,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platform_urn="urn:li:dataPlatform:snowflake",
                table_name="test_table",
                qualified_name="test_db.public.test_table",
                exists=True,
            ),
            connection_urn="urn:li:dataPlatform:snowflake",
        )

        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.connection_provider.get_connection.return_value = self.connection
        self.source = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = self.source

        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FIELD,
            dataset_field_parameters=DatasetFieldAssertionParameters(
                source_type=DatasetFieldSourceType.ALL_ROWS_QUERY,
            ),
        )
        self.database_params = AssertionDatabaseParams(
            qualified_name="test_db.public.test_table", table_name="test_table"
        )

    def test_evaluator_type(self) -> None:
        """Test the evaluator type property."""
        assert self.evaluator.type == AssertionType.FIELD

    def test_default_parameters(self) -> None:
        """Test the default_parameters property."""
        default_params = self.evaluator.default_parameters
        assert default_params.type == AssertionEvaluationParametersType.DATASET_FIELD
        assert default_params.dataset_field_parameters is not None
        assert (
            default_params.dataset_field_parameters.source_type
            == DatasetFieldSourceType.ALL_ROWS_QUERY
        )

    def test_evaluate_internal_no_field_assertion(self) -> None:
        """Test _evaluate_internal with no field assertion."""
        with pytest.raises(AssertionError, match="Missing required field assertion!"):
            self.evaluator._evaluate_internal(
                self.assertion,
                self.params,
                self.context,
            )

    def test_evaluate_internal_no_params(self) -> None:
        """Test _evaluate_internal with no dataset_field_parameters."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            fieldValuesAssertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="999", type="NUMBER"),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.params.dataset_field_parameters = None
        self.assertion.field_assertion = field_assertion

        with pytest.raises(Exception):
            self.evaluator._evaluate_internal(
                self.assertion,
                self.params,
                self.context,
            )

    def test_evaluate_field_values_assertion_exceeded_threshold(self) -> None:
        """Test _evaluate_field_values_assertion with values exceeding threshold."""
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            fieldValuesAssertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.NUMBER
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=0,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_field_values_assertion_equals_threshold(self) -> None:
        """Test _evaluate_field_values_assertion with values equal to threshold."""
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            fieldValuesAssertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 10
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_field_values_assertion_less_than_threshold(self) -> None:
        """Test _evaluate_field_values_assertion with values less than threshold."""
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            fieldValuesAssertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS
        assert self.state_provider.save_state.call_count == 0

    def test_evaluate_field_values_assertion_changed_rows_no_prev_state(self) -> None:
        """Test _evaluate_field_values_assertion with changed rows but no previous state."""
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
            native_type="TIMESTAMP",
            kind=None,
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 10
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_field_values_assertion_changed_rows_fail(self) -> None:
        """Test _evaluate_field_values_assertion with changed rows failing."""
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
            native_type="TIMESTAMP",
            kind=None,
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 20
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.FAILURE
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_field_values_assertion_changed_rows_success(self) -> None:
        """Test _evaluate_field_values_assertion with changed rows succeeding."""
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
            native_type="TIMESTAMP",
            kind=None,
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters is not None
        assert result.parameters["values_count"] == "1"
        assert result.parameters["threshold_value"] == "10"
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_invalid_metric(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with invalid metric."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.NEGATIVE_COUNT,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        with pytest.raises(
            InvalidParametersException,
            match="is not supported when using DataHub Dataset Profile",
        ):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_no_data(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with no profile data."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = None

        with pytest.raises(
            InsufficientDataException, match="Unable to find latest dataset profile"
        ):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN, self.assertion.field_assertion
            )

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_no_profile(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with no matching field profile."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(fieldProfiles=[], timestampMillis=TEST_START)
        )

        with pytest.raises(
            InsufficientDataException, match="Unable to find dataset field profile"
        ):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_no_metric_value(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with missing metric value."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=None)
                ],
                timestampMillis=TEST_START,
            )
        )

        with pytest.raises(
            InsufficientDataException, match="Unable to find dataset field profile data"
        ):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_success(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with successful evaluation."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.UNIQUE_COUNT,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=10)
                ],
                timestampMillis=TEST_START,
            )
        )

        result = (
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_datahub_dataset_profile_field_metric_assertion_failure(
        self,
    ) -> None:
        """Test _evaluate_datahub_dataset_profile_field_metric_assertion with failing evaluation."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.UNIQUE_COUNT,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=100)
                ],
                timestampMillis=TEST_START,
            )
        )

        result = (
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )
        )
        assert result.type == AssertionResultType.FAILURE

    def test_get_field_metric_value_from_dataset_field_profile(self) -> None:
        """Test _get_field_metric_value_from_dataset_field_profile for all metric types."""
        # Create a dataset field profile with all metrics populated
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id",
            uniqueCount=100,
            uniqueProportion=0.2,
            nullCount=5,
            nullProportion=0.05,
            min="1",
            max="100",
            mean="50",
            median="45",
            stdev="20",
        )

        # Test UNIQUE_COUNT
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_COUNT
            )
        )
        assert metric_value == 100.0

        # Test UNIQUE_PERCENTAGE
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_PERCENTAGE
            )
        )
        assert metric_value == 20.0

        # Test NULL_COUNT
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_COUNT
            )
        )
        assert metric_value == 5.0

        # Test NULL_PERCENTAGE
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_PERCENTAGE
            )
        )
        assert metric_value == 5.0

        # Test MIN
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MIN
            )
        )
        assert metric_value == 1.0

        # Test MAX
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MAX
            )
        )
        assert metric_value == 100.0

        # Test MEAN
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEAN
            )
        )
        assert metric_value == 50.0

        # Test MEDIAN
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEDIAN
            )
        )
        assert metric_value == 45.0

        # Test STDDEV
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.STDDEV
            )
        )
        assert metric_value == 20.0

    def test_get_field_metric_value_from_dataset_field_profile_missing_values(
        self,
    ) -> None:
        """Test _get_field_metric_value_from_dataset_field_profile with missing values."""
        # Create a dataset field profile with no metrics populated
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id",
            uniqueCount=None,
            uniqueProportion=None,
            nullCount=None,
            nullProportion=None,
            min=None,
            max=None,
            mean=None,
            median=None,
            stdev=None,
        )

        # Test all metrics return None
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_COUNT
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_PERCENTAGE
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_COUNT
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_PERCENTAGE
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MIN
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MAX
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEAN
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEDIAN
            )
            is None
        )
        assert (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.STDDEV
            )
            is None
        )

    def test_get_dataset_field_profile(self) -> None:
        """Test _get_dataset_field_profile method."""
        # Create field profiles
        field_profiles = [
            DatasetFieldProfileClass(fieldPath="id", uniqueCount=100),
            DatasetFieldProfileClass(fieldPath="name", uniqueCount=50),
            DatasetFieldProfileClass(fieldPath="age", uniqueCount=30),
        ]

        # Test finding existing field
        profile = self.evaluator._get_dataset_field_profile(field_profiles, "name")
        assert profile is not None
        assert profile.fieldPath == "name"
        assert profile.uniqueCount == 50

        # Test finding non-existent field
        profile = self.evaluator._get_dataset_field_profile(
            field_profiles, "non_existent"
        )
        assert profile is None

    def test_evaluate_field_metric_collection_step(self) -> None:
        """Test _evaluate_field_metric_collection_step method."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Create mock metric
        metric = Mock(spec=Metric)
        metric.value = 10.0
        self.metric_resolver.get_field_metric.return_value = metric

        # Test without saving
        result = self.evaluator._evaluate_field_metric_collection_step(
            assertion=self.assertion,
            parameters=self.params,
            context=self.context,
            save=False,
        )

        assert result is metric
        self.metric_resolver.get_field_metric.assert_called_once()
        self.metric_client.save_metric_value.assert_not_called()

        # Reset mocks
        self.metric_resolver.get_field_metric.reset_mock()

        # Test with saving
        result = self.evaluator._evaluate_field_metric_collection_step(
            assertion=self.assertion,
            parameters=self.params,
            context=self.context,
            save=True,
        )

        assert result is metric
        self.metric_resolver.get_field_metric.assert_called_once()
        self.metric_client.save_metric_value.assert_called_once()

    def test_evaluate_field_metric_collection_step_changed_rows(self) -> None:
        """Test _evaluate_field_metric_collection_step with changed rows query."""
        assert self.params.dataset_field_parameters is not None
        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="updated_at",
            type="TIMESTAMP",
            native_type="TIMESTAMP",
            kind=None,
        )

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Mock state with a previous high watermark value
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={"high_watermark_value": "2023-01-01"},
        )

        # Mock source.get_current_high_watermark_for_column
        self.source.get_current_high_watermark_for_column.return_value = (
            "2023-01-02",  # new high watermark value
            100,  # row count
        )

        # Create mock metric
        metric = Mock(spec=Metric)
        metric.value = 10.0
        self.metric_resolver.get_field_metric.return_value = metric

        # Call the method
        result = self.evaluator._evaluate_field_metric_collection_step(
            self.assertion, self.params, self.context, save=False
        )

        # Verify the result
        assert result is metric

        # Verify that state is saved
        self.state_provider.save_state.assert_called_once()

        # Verify that get_field_metric was called with the previous high watermark value
        self.metric_resolver.get_field_metric.assert_called_once_with(
            self.assertion.entity.urn,
            self.assertion.field_assertion.field_metric_assertion.field,  # type: ignore
            self.assertion.field_assertion.field_metric_assertion.metric,  # type: ignore
            ANY,  # database_params
            ANY,  # filter_params
            self.params.dataset_field_parameters.changed_rows_field,
            "2023-01-01",  # prev_high_watermark_value
            ANY,  # strategy
        )

    def test_evaluate_field_metric_collection_step_connection_error(self) -> None:
        """Test _evaluate_field_metric_collection_step handling of connection errors."""
        assert self.params.dataset_field_parameters is not None
        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="updated_at",
            type="TIMESTAMP",
            native_type="TIMESTAMP",
            kind=None,
        )

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    )
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Make connection provider return None to simulate connection error
        self.connection_provider.get_connection.return_value = None

        # Call the method and expect an exception
        with pytest.raises(
            SourceConnectionErrorException, match="Unable to retrieve valid connection"
        ):
            self.evaluator._evaluate_field_metric_collection_step(
                self.assertion, self.params, self.context, save=False
            )

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.field_evaluator.ONLINE_SMART_ASSERTIONS_ENABLED",
        True,
    )
    @patch(
        "datahub_executor.common.assertion.engine.evaluator.field_evaluator.is_training_required"
    )
    def test_evaluate_field_metric_assertion_step_smart_assertions(
        self, mock_is_training_required: Mock
    ) -> None:
        """Test _evaluate_field_metric_assertion_step with smart assertions."""
        # Configure smart assertions to be required
        mock_is_training_required.return_value = True

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Set up a context with no evaluation spec
        self.context.evaluation_spec = Mock(spec=AssertionEvaluationSpec)
        self.context.evaluation_spec.context = None

        # Call the method
        result = self.evaluator._evaluate_field_metric_assertion_step(
            10.0,  # metric_value
            self.assertion,
            self.context,
        )

        # Verify the result is INIT
        assert result.type == AssertionResultType.INIT
        assert result.parameters is not None
        assert result.parameters["metric_value"] == "10.0"

        # Set up a context with evaluation spec but no inference details
        self.context.evaluation_spec = Mock(spec=AssertionEvaluationSpec)
        self.context.evaluation_spec.context = None

        # Call the method
        result = self.evaluator._evaluate_field_metric_assertion_step(
            10.0,  # metric_value
            self.assertion,
            self.context,
        )

        # Verify the result is still INIT
        assert result.type == AssertionResultType.INIT

        # Set up a context with evaluation spec and inference details but generated_at = 0
        inference_details = Mock(spec=AssertionInferenceDetails)
        inference_details.generated_at = 0
        self.context.evaluation_spec = Mock(spec=AssertionEvaluationSpec)
        self.context.evaluation_spec.context = Mock(spec=AssertionEvaluationSpecContext)
        self.context.evaluation_spec.context.inference_details = inference_details

        # Call the method
        result = self.evaluator._evaluate_field_metric_assertion_step(
            10.0,  # metric_value
            self.assertion,
            self.context,
        )

        # Verify the result is still INIT
        assert result.type == AssertionResultType.INIT

        # Set up a context with valid inference details
        inference_details.generated_at = TEST_START

        # Call the method
        result = self.evaluator._evaluate_field_metric_assertion_step(
            10.0,  # metric_value
            self.assertion,
            self.context,
        )

        # Verify the result is now SUCCESS (since metric_value = 10.0 matches the expected value)
        assert result.type == AssertionResultType.SUCCESS

        # Test with non-matching value
        result = self.evaluator._evaluate_field_metric_assertion_step(
            20.0,  # metric_value
            self.assertion,
            self.context,
        )

        # Verify the result is FAILURE
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_internal_field_metric(self) -> None:
        """Test _evaluate_internal_field_metric method."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion(
                field=SchemaFieldSpec(
                    path="id", type="STRING", native_type="STRING", kind=None
                ),
                metric=FieldMetricType.MAX,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Mock the collection and evaluation steps
        with patch.object(
            self.evaluator, "_evaluate_field_metric_collection_step"
        ) as mock_collection:
            with patch.object(
                self.evaluator, "_evaluate_field_metric_assertion_step"
            ) as mock_evaluation:
                # Set up return values
                metric = Mock(spec=Metric)
                metric.value = 10.0
                metric.timestamp_ms = 123

                mock_collection.return_value = metric
                mock_evaluation.return_value = AssertionEvaluationResult(
                    AssertionResultType.SUCCESS
                )

                # Call the method
                result = self.evaluator._evaluate_internal_field_metric(
                    self.assertion, self.params, self.context
                )

                # Verify the result
                assert result.type == AssertionResultType.SUCCESS
                # Ensure metric context is appended.
                assert result.metric
                assert result.metric.value == 10.0
                assert result.metric.timestamp_ms == 123

                # Verify the mocks were called
                mock_collection.assert_called_once()
                mock_evaluation.assert_called_once_with(
                    metric_value=10.0, assertion=self.assertion, context=self.context
                )

    def test_evaluate_internal_field_values(self) -> None:
        """Test _evaluate_internal_field_values method."""
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            fieldValuesAssertion=FieldValuesAssertion(
                field=SchemaFieldSpec(
                    path="id",
                    type="STRING",
                    native_type="STRING",
                    kind=None,
                ),
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="my-id-value", type=AssertionStdParameterType.STRING
                    ),
                ),
                fail_threshold=FieldValuesFailThreshold(
                    type=FieldValuesFailThresholdType.COUNT,
                    value=10,
                ),
                exclude_nulls=False,
                transform=None,
            ),
        )
        self.assertion.field_assertion = field_assertion

        # Make connection return None to test error path
        self.connection_provider.get_connection.return_value = None

        # Verify connection error is raised
        with pytest.raises(SourceConnectionErrorException):
            self.evaluator._evaluate_internal_field_values(
                self.assertion, self.params, self.context
            )

        # Reset connection to return a valid connection
        self.connection_provider.get_connection.return_value = self.connection

        # Mock the field values assertion method
        with patch.object(
            self.evaluator, "_evaluate_field_values_assertion"
        ) as mock_evaluate:
            mock_evaluate.return_value = AssertionEvaluationResult(
                AssertionResultType.SUCCESS
            )

            # Call the method
            result = self.evaluator._evaluate_internal_field_values(
                self.assertion, self.params, self.context
            )

            # Verify the result
            assert result.type == AssertionResultType.SUCCESS

            # Verify the mock was called with the right parameters
            mock_evaluate.assert_called_once_with(
                self.assertion.entity.urn,
                self.assertion.field_assertion,
                self.params.dataset_field_parameters,
                self.source,
                ANY,  # database_params
                self.context,
            )
