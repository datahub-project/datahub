import logging
from datetime import datetime, timezone
from unittest import mock

import pandas as pd
import pytest
from great_expectations.core.batch import Batch, BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import (
    RuntimeDataBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.execution_engine.sparkdf_execution_engine import (
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionTypeClass,
    BatchSpecClass,
    DataPlatformInstanceClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    PartitionSpecClass,
)
from datahub_gx_plugin.action import DataHubValidationAction

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def ge_data_context(tmp_path: str) -> FileDataContext:
    return FileDataContext.create(tmp_path)


@pytest.fixture(scope="function")
def ge_validator_sqlalchemy() -> Validator:
    validator = Validator(
        execution_engine=SqlAlchemyExecutionEngine(
            connection_string="postgresql://localhost:5432/test"
        ),
        batches=[
            Batch(
                data=None,
                batch_request=BatchRequest(
                    datasource_name="my_postgresql_datasource",
                    data_connector_name="whole_table",
                    data_asset_name="foo2",
                ),
                batch_definition=BatchDefinition(
                    datasource_name="my_postgresql_datasource",
                    data_connector_name="whole_table",
                    data_asset_name="foo2",
                    batch_identifiers=IDDict(),
                ),
                batch_spec=SqlAlchemyDatasourceBatchSpec(
                    {
                        "data_asset_name": "foo2",
                        "table_name": "foo2",
                        "batch_identifiers": {},
                        "schema_name": "public",
                        "type": "table",
                    }
                ),
            )
        ],
    )
    return validator


@pytest.fixture(scope="function")
def ge_validator_spark() -> Validator:
    validator = Validator(execution_engine=SparkDFExecutionEngine())
    return validator


@pytest.fixture(scope="function")
def ge_validator_pandas() -> Validator:
    validator = Validator(
        execution_engine=PandasExecutionEngine(),
        batches=[
            Batch(
                data=pd.DataFrame({"foo": [10, 20], "bar": [100, 200]}),
                batch_request=BatchRequest(
                    datasource_name="my_df_datasource",
                    data_connector_name="pandas_df",
                    data_asset_name="foobar",
                ),
                batch_definition=BatchDefinition(
                    datasource_name="my_df_datasource",
                    data_connector_name="pandas_df",
                    data_asset_name="foobar",
                    batch_identifiers=IDDict(),
                ),
                batch_spec=RuntimeDataBatchSpec(
                    {
                        "data_asset_name": "foobar",
                        "batch_identifiers": {},
                        "batch_data": {},
                        "type": "pandas_dataframe",
                    }
                ),
            )
        ],
    )
    return validator


@pytest.fixture(scope="function")
def ge_validation_result_suite_pandas() -> ExpectationSuiteValidationResult:
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[
            {
                "success": True,
                "result": {},
                "expectation_config": {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {
                        "column": "column",
                        "batch_id": "010ef8c1cd417910b971f4468f024ec6",
                    },
                    "meta": {},
                },
            }
        ],
        success=True,
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 1,
            "unsuccessful_expectations": 0,
            "success_percent": 100,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_200",
            },
            "validation_time": "20211228T130000.000000Z",
        },
    )
    return validation_result_suite


@pytest.fixture(scope="function")
def ge_validation_result_suite() -> ExpectationSuiteValidationResult:
    validation_result_suite = ExpectationSuiteValidationResult(
        results=[
            {
                "success": True,
                "result": {"observed_value": 10000},
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {
                        "max_value": 10000,
                        "min_value": 10000,
                        "batch_id": "010ef8c1cd417910b971f4468f024ec5",
                    },
                    "meta": {},
                },
            }
        ],
        success=True,
        statistics={
            "evaluated_expectations": 1,
            "successful_expectations": 1,
            "unsuccessful_expectations": 0,
            "success_percent": 100,
        },
        meta={
            "great_expectations_version": "v0.13.40",
            "expectation_suite_name": "asset.default",
            "run_id": {
                "run_name": "test_100",
            },
            "validation_time": "20211228T120000.000000Z",
        },
    )
    return validation_result_suite


@pytest.fixture(scope="function")
def ge_validation_result_suite_id() -> ValidationResultIdentifier:
    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_100",
            run_time=datetime.fromtimestamp(1640701702, tz=timezone.utc),
        ),
        batch_identifier="010ef8c1cd417910b971f4468f024ec5",
    )

    return validation_result_suite_id


@pytest.fixture(scope="function")
def ge_validation_result_suite_id_pandas() -> ValidationResultIdentifier:
    validation_result_suite_id = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(
            run_name="test_200",
            run_time=datetime.fromtimestamp(1640701702, tz=timezone.utc),
        ),
        batch_identifier="010ef8c1cd417910b971f4468f024ec6",
    )

    return validation_result_suite_id


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp", autospec=True)
def test_DataHubValidationAction_sqlalchemy(
    mock_emitter: mock.MagicMock,
    ge_data_context: FileDataContext,
    ge_validator_sqlalchemy: Validator,
    ge_validation_result_suite: ExpectationSuiteValidationResult,
    ge_validation_result_suite_id: ValidationResultIdentifier,
) -> None:
    server_url = "http://localhost:9999"

    datahub_action = DataHubValidationAction(
        data_context=ge_data_context, server_url=server_url
    )

    assert datahub_action.run(
        validation_result_suite_identifier=ge_validation_result_suite_id,
        validation_result_suite=ge_validation_result_suite,
        data_asset=ge_validator_sqlalchemy,
    ) == {"datahub_notification_result": "DataHub notification succeeded"}

    mock_emitter.assert_has_calls(
        [
            mock.call(
                mock.ANY,
                MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType="UPSERT",
                    entityUrn="urn:li:assertion:8f25f50da43bf7434137dd5ab6fbdb09",
                    aspectName="assertionInfo",
                    aspect=AssertionInfoClass(
                        type=AssertionTypeClass.DATASET,
                        customProperties={"expectation_suite_name": "asset.default"},
                        datasetAssertion=DatasetAssertionInfoClass(
                            scope=DatasetAssertionScopeClass.DATASET_ROWS,
                            dataset="urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.foo2,PROD)",
                            operator="BETWEEN",
                            nativeType="expect_table_row_count_to_be_between",
                            aggregation="ROW_COUNT",
                            parameters=AssertionStdParametersClass(
                                maxValue=AssertionStdParameterClass(
                                    value="10000", type="NUMBER"
                                ),
                                minValue=AssertionStdParameterClass(
                                    value="10000", type="NUMBER"
                                ),
                            ),
                            nativeParameters={
                                "max_value": "10000",
                                "min_value": "10000",
                            },
                        ),
                    ),
                ),
            ),
            mock.call(
                mock.ANY,
                MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType="UPSERT",
                    entityUrn="urn:li:assertion:8f25f50da43bf7434137dd5ab6fbdb09",
                    aspectName="dataPlatformInstance",
                    aspect=DataPlatformInstanceClass(
                        platform="urn:li:dataPlatform:great-expectations"
                    ),
                ),
            ),
            mock.call(
                mock.ANY,
                MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType="UPSERT",
                    entityUrn="urn:li:assertion:8f25f50da43bf7434137dd5ab6fbdb09",
                    entityKeyAspect=None,
                    aspectName="assertionRunEvent",
                    aspect=AssertionRunEventClass(
                        timestampMillis=mock.ANY,
                        runId="2021-12-28T14:28:22Z",
                        partitionSpec=PartitionSpecClass(
                            type="FULL_TABLE",
                            partition="FULL_TABLE_SNAPSHOT",
                            timePartition=None,
                        ),
                        assertionUrn="urn:li:assertion:8f25f50da43bf7434137dd5ab6fbdb09",
                        asserteeUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.foo2,PROD)",
                        batchSpec=BatchSpecClass(
                            customProperties={
                                "data_asset_name": "foo2",
                                "datasource_name": "my_postgresql_datasource",
                            },
                            nativeBatchId="010ef8c1cd417910b971f4468f024ec5",
                        ),
                        status=AssertionRunStatusClass.COMPLETE,
                        result=AssertionResultClass(
                            type=AssertionResultTypeClass.SUCCESS,
                            actualAggValue=10000,
                            nativeResults={"observed_value": "10000"},
                        ),
                    ),
                ),
            ),
        ]
    )


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter.emit_mcp", autospec=True)
def test_DataHubValidationAction_pandas(
    mock_emitter: mock.MagicMock,
    ge_data_context: FileDataContext,
    ge_validator_pandas: Validator,
    ge_validation_result_suite_pandas: ExpectationSuiteValidationResult,
    ge_validation_result_suite_id_pandas: ValidationResultIdentifier,
) -> None:
    server_url = "http://localhost:9999"

    datahub_action = DataHubValidationAction(
        data_context=ge_data_context,
        server_url=server_url,
        platform_instance_map={"my_df_datasource": "custom_platefrom"},
    )

    assert datahub_action.run(
        validation_result_suite_identifier=ge_validation_result_suite_id_pandas,
        validation_result_suite=ge_validation_result_suite_pandas,
        data_asset=ge_validator_pandas,
    ) == {"datahub_notification_result": "DataHub notification succeeded"}

    mock_emitter.assert_has_calls(
        [
            mock.call(
                mock.ANY,
                MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType="UPSERT",
                    entityUrn="urn:li:assertion:7e04bcc3b85897d6d3fef6c998db6b05",
                    aspectName="assertionInfo",
                    aspect=AssertionInfoClass(
                        customProperties={"expectation_suite_name": "asset.default"},
                        type="DATASET",
                        datasetAssertion=DatasetAssertionInfoClass(
                            dataset="urn:li:dataset:(urn:li:dataPlatform:custom_platefrom,my_df_datasource,PROD)",
                            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                            operator="NOT_NULL",
                            fields=[
                                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:custom_platefrom,my_df_datasource,PROD),column)"
                            ],
                            aggregation="IDENTITY",
                            nativeType="expect_column_values_to_not_be_null",
                            nativeParameters={"column": "column"},
                        ),
                    ),
                ),
            ),
            mock.call(
                mock.ANY,
                MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType="UPSERT",
                    entityUrn="urn:li:assertion:7e04bcc3b85897d6d3fef6c998db6b05",
                    aspectName="dataPlatformInstance",
                    aspect=DataPlatformInstanceClass(
                        platform="urn:li:dataPlatform:great-expectations"
                    ),
                ),
            ),
        ]
    )


def test_DataHubValidationAction_graceful_failure(
    ge_data_context: FileDataContext,
    ge_validator_sqlalchemy: Validator,
    ge_validation_result_suite: ExpectationSuiteValidationResult,
    ge_validation_result_suite_id: ValidationResultIdentifier,
) -> None:
    server_url = "http://localhost:9999"

    datahub_action = DataHubValidationAction(
        data_context=ge_data_context, server_url=server_url
    )

    assert datahub_action.run(
        validation_result_suite_identifier=ge_validation_result_suite_id,
        validation_result_suite=ge_validation_result_suite,
        data_asset=ge_validator_sqlalchemy,
    ) == {"datahub_notification_result": "DataHub notification failed"}


def test_DataHubValidationAction_not_supported(
    ge_data_context: FileDataContext,
    ge_validator_spark: Validator,
    ge_validation_result_suite: ExpectationSuiteValidationResult,
    ge_validation_result_suite_id: ValidationResultIdentifier,
) -> None:
    server_url = "http://localhost:99199"

    datahub_action = DataHubValidationAction(
        data_context=ge_data_context, server_url=server_url
    )

    assert datahub_action.run(
        validation_result_suite_identifier=ge_validation_result_suite_id,
        validation_result_suite=ge_validation_result_suite,
        data_asset=ge_validator_spark,
    ) == {"datahub_notification_result": "none required"}
