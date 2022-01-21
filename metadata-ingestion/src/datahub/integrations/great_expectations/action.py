import logging
from datetime import timezone
from typing import Any, Dict, List, Optional, Union

from great_expectations.checkpoint.actions import ValidationAction
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator
from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.source.sql.sql_common import get_platform_from_sqlalchemy_uri
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionScope,
    AssertionStdOperator,
    AssertionType,
    BatchAssertionResult,
    BatchSpec,
    DatasetColumnAssertion,
    DatasetColumnStdAggFunc,
    DatasetRowsAssertion,
    DatasetRowsStdAggFunc,
    DatasetSchemaAssertion,
    DatasetSchemaStdAggFunc,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.schema_classes import PartitionSpecClass, PartitionTypeClass
from datahub.utilities.sql_parser import MetadataSQLSQLParser

logger = logging.getLogger(__name__)


class DatahubValidationAction(ValidationAction):
    def __init__(
        self,
        data_context: DataContext,
        server_url: str,
        env: str = builder.DEFAULT_ENV,
        graceful_exceptions: bool = True,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        super().__init__(data_context)
        self.server_url = server_url
        self.env = env
        self.graceful_exceptions = graceful_exceptions
        self.token = token
        self.timeout_sec = timeout_sec
        self.retry_status_codes = retry_status_codes
        self.retry_max_times = retry_max_times
        self.extra_headers = extra_headers

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        data_asset: Union[Validator, DataAsset, Batch],
        payload: Any = None,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier: Any = None,
    ) -> Dict:
        datasets = []
        try:
            emitter = DatahubRestEmitter(
                self.server_url,
                self.token,
                self.timeout_sec,
                self.timeout_sec,
                self.retry_status_codes,
                self.retry_max_times,
                self.extra_headers,
            )

            # Returns datasets and corresponding batch requests
            datasets = self.get_dataset_partitions(
                validation_result_suite_identifier.batch_identifier, data_asset
            )

            if len(datasets) == 0 or datasets[0]["dataset_urn"] is None:
                logger.info("Metadata not sent to datahub.")
                return {"datahub_notification_result": "none required"}

            # Returns assertion info and assertion results
            assertions = self.get_assertions_withresults(
                validation_result_suite,
                validation_result_suite_identifier,
                payload,
                datasets,
            )

            for assertion in assertions:
                # Construct a MetadataChangeProposalWrapper object.
                assertion_info_mcp = MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType=ChangeType.UPSERT,
                    entityUrn=assertion["assertionUrn"],
                    aspectName="assertionInfo",
                    aspect=assertion["assertionInfo"],
                )
                emitter.emit_mcp(assertion_info_mcp)

                # Construct a MetadataChangeProposalWrapper object.
                assertion_platform_mcp = MetadataChangeProposalWrapper(
                    entityType="assertion",
                    changeType=ChangeType.UPSERT,
                    entityUrn=assertion["assertionUrn"],
                    aspectName="dataPlatformInstance",
                    aspect=assertion["assertionPlatform"],
                )
                emitter.emit_mcp(assertion_platform_mcp)

                for assertionResult in assertion["assertionResults"]:
                    dataset_assertionResult_mcp = MetadataChangeProposalWrapper(
                        entityType="dataset",
                        changeType=ChangeType.UPSERT,
                        entityUrn=assertionResult.asserteeUrn,
                        aspectName="assertionResult",
                        aspect=assertionResult,
                    )

                    # Emit BatchAssertion Result! (timseries aspect)
                    emitter.emit_mcp(dataset_assertionResult_mcp)

            result = "Datahub notification succeeded"
        except Exception as e:
            result = "Datahub notification failed"
            if self.graceful_exceptions:
                logger.error(e)
                logger.info("Supressing error because graceful_exceptions is set")
            else:
                raise

        return {"datahub_notification_result": result}

    def get_assertions_withresults(
        self,
        validation_result_suite,
        validation_result_suite_identifier,
        payload,
        datasets,
    ):

        dataPlatformInstance = DataPlatformInstance(
            platform=builder.make_data_platform_urn("greatExpectations")
        )
        docs_link = None
        if payload:
            # process the payload
            for action_names in payload.keys():
                if payload[action_names]["class"] == "UpdateDataDocsAction":
                    data_docs_pages = payload[action_names]
                    for docs_link_key, docs_link_val in data_docs_pages.items():
                        if "file://" not in docs_link_val and docs_link_key != "class":
                            docs_link = docs_link_val

        assertions_withresults = []
        for result in validation_result_suite.results:
            expectation_config = result["expectation_config"]
            expectation_type = expectation_config["expectation_type"]
            success = True if result["success"] else False
            kwargs = expectation_config["kwargs"]

            result = result["result"]
            assertionInfo: AssertionInfo = self.get_assertion_info(
                expectation_type, kwargs
            )
            assertionInfo.datasets = [d["dataset_urn"] for d in datasets]
            if len(datasets) == 1 and "column" in kwargs:
                assertionInfo.datasetFields = [
                    builder.make_schema_field_urn(
                        datasets[0]["dataset_urn"], kwargs["column"]
                    )
                ]
            assertionInfo.customProperties = {
                "expectation_suite_name": validation_result_suite_identifier.expectation_suite_identifier.expectation_suite_name
            }

            assertionUrn = builder.make_assertion_urn(
                builder.datahub_guid(
                    {**assertionInfo.to_obj(), **dataPlatformInstance.to_obj()}
                )
            )
            run_time = validation_result_suite_identifier.run_id.run_time.astimezone(
                timezone.utc
            )
            assertionResults = []
            for dset in datasets:
                nativeResults = {
                    k: str(v)
                    for k, v in result.items()
                    if k
                    in [
                        "partial_unexpected_list",
                        "partial_unexpected_counts",
                        "details",
                    ]
                }
                actualAggValue = None
                if result.get("observed_value") is not None:
                    if isinstance(result.get("observed_value"), (int, float)):
                        actualAggValue = result.get("observed_value")
                    else:
                        nativeResults["observed_value"] = result.get("observed_value")

                # https://docs.greatexpectations.io/docs/reference/expectations/result_format/
                assertionResult = AssertionResult(
                    timestampMillis=int(run_time.timestamp() * 1000),
                    assertionUrn=assertionUrn,
                    asserteeUrn=dset["dataset_urn"],
                    nativeEvaluatorRunId=run_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    batchAssertionResult=BatchAssertionResult(
                        success=success,
                        rowCount=result.get("element_count"),
                        missingCount=result.get("missing_count"),
                        unexpectedCount=result.get("unexpected_count"),
                        actualAggValue=actualAggValue,
                        externalUrl=docs_link,
                        nativeResults=nativeResults,
                    ),
                    batchSpec=dset["batchSpec"],
                    messageId=assertionUrn,
                )
                if dset.get("partitionSpec") is not None:
                    assertionResult.partitionSpec = dset.get("partitionSpec")
                assertionResults.append(assertionResult)

            assertions_withresults.append(
                {
                    "assertionUrn": assertionUrn,
                    "assertionInfo": assertionInfo,
                    "assertionPlatform": dataPlatformInstance,
                    "assertionResults": assertionResults,
                }
            )
        return assertions_withresults

    def get_assertion_info(self, expectation_type, kwargs):

        column_aggregate_expectations = {
            "expect_column_min_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MIN,
            ),
            "expect_column_max_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MAX,
            ),
            "expect_column_median_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MEDIAN,
            ),
            "expect_column_stdev_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.STDDEV,
            ),
            "expect_column_mean_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MEAN,
            ),
            "expect_column_unique_value_count_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.UNIQUE_COUNT,
            ),
            "expect_column_proportion_of_unique_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.UNIQUE_PROPOTION,
            ),
            "expect_column_sum_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc._NATIVE_,
                nativeAggFunc="SUM",
            ),
            "expect_column_quantile_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc._NATIVE_,
                nativeAggFunc="QUANTILE",
            ),
        }

        column_map_expectations = {
            "expect_column_values_to_not_be_null": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.NOT_NULL,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_be_in_set": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.IN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
        }

        table_schema_expectations = {
            "expect_table_columns_to_match_ordered_list": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMNS,
            ),
            "expect_table_columns_to_match_set": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMNS,
            ),
            "expect_table_column_count_to_be_between": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMN_COUNT,
            ),
            "expect_table_column_count_to_equal": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMN_COUNT,
            ),
            "expect_column_to_exist": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator._NATIVE_,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc._NATIVE_,
            ),
        }

        table_expectations = {
            "expect_table_row_count_to_equal": DatasetRowsAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetRowsStdAggFunc.ROW_COUNT,
            ),
            "expect_table_row_count_to_be_between": DatasetRowsAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeOperator=expectation_type,
                stdAggFunc=DatasetRowsStdAggFunc.ROW_COUNT,
            ),
        }

        if expectation_type in column_aggregate_expectations.keys():
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_COLUMN,
                datasetColumnAssertion=column_aggregate_expectations[expectation_type],
            )
        elif expectation_type in column_map_expectations.keys():
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_COLUMN,
                datasetColumnAssertion=column_map_expectations[expectation_type],
            )
        elif expectation_type in table_schema_expectations.keys():
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_SCHEMA,
                datasetSchemaAssertion=table_schema_expectations[expectation_type],
            )
        elif expectation_type in table_expectations.keys():
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_ROWS,
                datasetRowsAssertion=table_expectations[expectation_type],
            )
        # Heuristically mapping other expectations
        elif "column" in kwargs and expectation_type.startswith("expect_column_value"):
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_COLUMN,
                datasetColumnAssertion=DatasetColumnAssertion(
                    stdOperator=AssertionStdOperator._NATIVE_,
                    nativeOperator=expectation_type,
                    stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
                ),
            )
        elif "column" in kwargs:
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_COLUMN,
                datasetColumnAssertion=DatasetColumnAssertion(
                    stdOperator=AssertionStdOperator._NATIVE_,
                    nativeOperator=expectation_type,
                    stdAggFunc=DatasetColumnStdAggFunc._NATIVE_,
                ),
            )
        else:
            assertionType = AssertionType(
                scope=AssertionScope.DATASET_ROWS,
                datasetRowsAssertion=DatasetRowsAssertion(
                    stdOperator=AssertionStdOperator._NATIVE_,
                    nativeOperator=expectation_type,
                    stdAggFunc=DatasetRowsStdAggFunc._NATIVE_,
                ),
            )

        return AssertionInfo(
            assertionType=assertionType,
            # 1. keys `max_val`, `min_val` for to_be_between constraints,
            # except for expect_column_quantile_values_to_be_between having parameter
            # keys `quantile_ranges`.quantiles` and `quantile_ranges`.`value_ranges`
            # 2. `value` for singular equal_to constraints
            # 3. `values` or `columns_set` or `values_set` or `columns_list`
            # for columns equal_to and to_be_in_set constraints
            assertionParameters={
                k: str(v) for k, v in kwargs.items() if k not in ["batch_id", "column"]
            },
        )

    def get_dataset_partitions(self, batch_identifier, data_asset):
        dataset_partitions = []

        # for now, we support only v3-api and sqlalchemy execution engine
        if isinstance(data_asset, Validator) and isinstance(
            data_asset.execution_engine, SqlAlchemyExecutionEngine
        ):
            ge_batch_spec = data_asset.active_batch_spec
            partitionSpec = None

            if isinstance(ge_batch_spec, SqlAlchemyDatasourceBatchSpec):
                # e.g. ConfiguredAssetSqlDataConnector with splitter_method or sampling_method
                schema_name = ge_batch_spec.get("schema_name")
                table_name = ge_batch_spec.get("table_name")

                dataset_urn = make_dataset_urn(
                    data_asset.execution_engine.engine.url,
                    schema_name,
                    table_name,
                    self.env,
                )
                batchSpecProperties = {}
                if data_asset.active_batch_definition.data_asset_name:
                    batchSpecProperties[
                        "data_asset_name"
                    ] = data_asset.active_batch_definition.data_asset_name
                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    customProperties=batchSpecProperties,
                )

                splitter_method = ge_batch_spec.get("splitter_method")
                if (
                    splitter_method is not None
                    and splitter_method != "_split_on_whole_table"
                ):
                    batch_identifiers = ge_batch_spec.get("batch_identifiers", {})
                    partitionSpec = PartitionSpecClass(partition=str(batch_identifiers))
                sampling_method = ge_batch_spec.get("sampling_method", "")
                if sampling_method == "_sample_using_limit":
                    batchSpec.limit = ge_batch_spec["sampling_kwargs"]["n"]

                dataset_partitions.append(
                    {
                        "dataset_urn": dataset_urn,
                        "partitionSpec": partitionSpec,
                        "batchSpec": batchSpec,
                    }
                )
            elif isinstance(ge_batch_spec, RuntimeQueryBatchSpec):
                query = data_asset.batches[
                    batch_identifier
                ].batch_request.runtime_parameters["query"]
                partitionSpec = PartitionSpecClass(
                    type=PartitionTypeClass.QUERY,
                    partition="Query_" + builder.datahub_guid(query),
                )
                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    query=query,
                )
                tables = MetadataSQLSQLParser(query).get_tables()
                for table in tables:
                    dataset_urn = make_dataset_urn(
                        data_asset.execution_engine.engine.url,
                        None,
                        table,
                        self.env,
                    )
                    dataset_partitions.append(
                        {
                            "dataset_urn": dataset_urn,
                            "partitionSpec": partitionSpec,
                            "batchSpec": batchSpec,
                        }
                    )
            else:
                logger.warning(
                    f"DatahubValidationAction does not recognize this GE batch spec type- {type(ge_batch_spec)}."
                )
        else:
            # TODO - v2-spec - SqlAlchemyDataset support
            logger.warning(
                f"DatahubValidationAction does not recognize this GE data asset type - {type(data_asset)}. \
                        This is either using v2-api or execution engine other than sqlalchemy."
            )

        return dataset_partitions


def make_dataset_urn(sqlalchemy_uri, schema_name, table_name, env):

    data_platform = get_platform_from_sqlalchemy_uri(str(sqlalchemy_uri))
    url_instance = make_url(sqlalchemy_uri)

    if schema_name is None and "." in table_name:
        schema_name, table_name = table_name.split(".")[-2:]

    if data_platform in ["redshift", "postgres"]:
        schema_name = schema_name if schema_name else "public"
        if url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate database name for {data_platform}."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform == "mssql":
        schema_name = schema_name if schema_name else "dbo"
        if url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate database name for {data_platform}."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform in ["trino", "snowflake"]:
        if schema_name is None or url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate schema name and/or database name \
                    for {data_platform}."
            )
            return None
        schema_name = "{}.{}".format(url_instance.database, schema_name)
    elif data_platform == "bigquery":
        if url_instance.host is None or url_instance.database is None:
            logger.warning(
                f"DatahubValidationAction failed to locate host and/or database name for \
                    {data_platform}. "
            )
            return None
        schema_name = "{}.{}".format(url_instance.host, url_instance.database)

    schema_name = schema_name if schema_name else url_instance.database
    if schema_name is None:
        logger.warning(
            f"DatahubValidationAction failed to locate schema name for {data_platform}."
        )
        return None

    dataset_name = "{}.{}".format(schema_name, table_name)

    dataset_urn = builder.make_dataset_urn(
        platform=data_platform, name=dataset_name, env=env
    )
    return dataset_urn
