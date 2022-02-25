import logging
import time
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
    GeCloudIdentifier,
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
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdOperator,
    AssertionType,
    BatchSpec,
    DatasetAssertionInfo,
    DatasetAssertionScope,
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
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GeCloudIdentifier
        ],
        data_asset: Union[Validator, DataAsset, Batch],
        payload: Any = None,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier: Any = None,
    ) -> Dict:
        datasets = []
        try:
            emitter = DatahubRestEmitter(
                gms_server=self.server_url,
                token=self.token,
                read_timeout_sec=self.timeout_sec,
                connect_timeout_sec=self.timeout_sec,
                retry_status_codes=self.retry_status_codes,
                retry_max_times=self.retry_max_times,
                extra_headers=self.extra_headers,
            )

            expectation_suite_name = validation_result_suite.meta.get(
                "expectation_suite_name"
            )
            run_id = validation_result_suite.meta.get("run_id")
            if hasattr(data_asset, "active_batch_id"):
                batch_identifier = data_asset.active_batch_id
            else:
                batch_identifier = data_asset.batch_id

            if isinstance(
                validation_result_suite_identifier, ValidationResultIdentifier
            ):
                expectation_suite_name = (
                    validation_result_suite_identifier.expectation_suite_identifier.expectation_suite_name
                )
                run_id = validation_result_suite_identifier.run_id
                batch_identifier = validation_result_suite_identifier.batch_identifier

            # Returns datasets and corresponding batch requests
            datasets = self.get_dataset_partitions(batch_identifier, data_asset)

            if len(datasets) == 0 or datasets[0]["dataset_urn"] is None:
                logger.info("Metadata not sent to datahub.")
                return {"datahub_notification_result": "none required"}

            # Returns assertion info and assertion results
            assertions = self.get_assertions_withresults(
                validation_result_suite,
                expectation_suite_name,
                run_id,
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
                        entityType="assertion",
                        changeType=ChangeType.UPSERT,
                        entityUrn=assertionResult.assertionUrn,
                        aspectName="assertionRunEvent",
                        aspect=assertionResult,
                    )

                    # Emit Result! (timseries aspect)
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
        expectation_suite_name,
        run_id,
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
            kwargs = {
                k: v for k, v in expectation_config["kwargs"].items() if k != "batch_id"
            }

            result = result["result"]
            assertion_datasets = [d["dataset_urn"] for d in datasets]
            if len(datasets) == 1 and "column" in kwargs:
                assertion_fields = [
                    builder.make_schema_field_urn(
                        datasets[0]["dataset_urn"], kwargs["column"]
                    )
                ]
            else:
                assertion_fields = None  # type:ignore

            # Be careful what fields to consider for creating assertion urn.
            # Any change in fields below would lead to a new assertion
            assertionUrn = builder.make_assertion_urn(
                builder.datahub_guid(
                    {
                        "platform": dataPlatformInstance.platform,
                        "datasets": assertion_datasets,
                        "fields": assertion_fields,
                        "nativeType": expectation_type,
                        "parameters": kwargs,
                    }
                )
            )
            assertionInfo: AssertionInfo = self.get_assertion_info(
                expectation_type, kwargs
            )
            assertionInfo.datasetAssertion.datasets = assertion_datasets  # type:ignore
            assertionInfo.datasetAssertion.fields = assertion_fields  # type:ignore

            assertionInfo.customProperties = {
                "expectation_suite_name": expectation_suite_name
            }

            # TODO: Understand why their run time is incorrect.
            run_time = run_id.run_time.astimezone(timezone.utc)
            assertionResults = []

            nativeResults = {
                k: str(v)
                for k, v in result.items()
                if (
                    k
                    in [
                        "observed_value",
                        "partial_unexpected_list",
                        "partial_unexpected_counts",
                        "details",
                    ]
                    and v
                )
            }

            actualAggValue = (
                result.get("observed_value")
                if isinstance(result.get("observed_value"), (int, float))
                else None
            )

            for dset in datasets:
                # https://docs.greatexpectations.io/docs/reference/expectations/result_format/
                assertionResult = AssertionRunEvent(
                    timestampMillis=int(round(time.time() * 1000)),
                    assertionUrn=assertionUrn,
                    asserteeUrn=dset["dataset_urn"],
                    runId=run_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    result=AssertionResult(
                        type=AssertionResultType.SUCCESS
                        if success
                        else AssertionResultType.FAILURE,
                        rowCount=result.get("element_count"),
                        missingCount=result.get("missing_count"),
                        unexpectedCount=result.get("unexpected_count"),
                        actualAggValue=actualAggValue,
                        externalUrl=docs_link,
                        nativeResults=nativeResults,
                    ),
                    batchSpec=dset["batchSpec"],
                    status=AssertionRunStatus.COMPLETE,
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

        column_expectations = {
            # column aggregate expectations
            "expect_column_min_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MIN,
            ),
            "expect_column_max_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MAX,
            ),
            "expect_column_median_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MEDIAN,
            ),
            "expect_column_stdev_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.STDDEV,
            ),
            "expect_column_mean_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.MEAN,
            ),
            "expect_column_unique_value_count_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.UNIQUE_COUNT,
            ),
            "expect_column_proportion_of_unique_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.UNIQUE_PROPOTION,
            ),
            "expect_column_sum_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.SUM,
            ),
            "expect_column_quantile_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc._NATIVE_,
            ),
            # column map expectations
            "expect_column_values_to_not_be_null": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.NOT_NULL,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_be_in_set": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.IN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_be_between": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_match_regex": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.REGEX_MATCH,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
            "expect_column_values_to_match_regex_list": DatasetColumnAssertion(
                stdOperator=AssertionStdOperator.REGEX_MATCH,
                nativeType=expectation_type,
                stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
            ),
        }

        table_schema_expectations = {
            "expect_table_columns_to_match_ordered_list": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeType=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMNS,
            ),
            "expect_table_columns_to_match_set": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeType=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMNS,
            ),
            "expect_table_column_count_to_be_between": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMN_COUNT,
            ),
            "expect_table_column_count_to_equal": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeType=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc.COLUMN_COUNT,
            ),
            "expect_column_to_exist": DatasetSchemaAssertion(
                stdOperator=AssertionStdOperator._NATIVE_,
                nativeType=expectation_type,
                stdAggFunc=DatasetSchemaStdAggFunc._NATIVE_,
            ),
        }

        table_expectations = {
            "expect_table_row_count_to_equal": DatasetRowsAssertion(
                stdOperator=AssertionStdOperator.EQUAL_TO,
                nativeType=expectation_type,
                stdAggFunc=DatasetRowsStdAggFunc.ROW_COUNT,
            ),
            "expect_table_row_count_to_be_between": DatasetRowsAssertion(
                stdOperator=AssertionStdOperator.BETWEEN,
                nativeType=expectation_type,
                stdAggFunc=DatasetRowsStdAggFunc.ROW_COUNT,
            ),
        }

        if expectation_type in column_expectations.keys():
            datasetAssertionInfo = DatasetAssertionInfo(
                scope=DatasetAssertionScope.DATASET_COLUMN,
                columnAssertion=column_expectations[expectation_type],
            )
        elif expectation_type in table_schema_expectations.keys():
            datasetAssertionInfo = DatasetAssertionInfo(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                schemaAssertion=table_schema_expectations[expectation_type],
            )
        elif expectation_type in table_expectations.keys():
            datasetAssertionInfo = DatasetAssertionInfo(
                scope=DatasetAssertionScope.DATASET_ROWS,
                rowsAssertion=table_expectations[expectation_type],
            )
        # Heuristically mapping other expectations
        else:
            if "column" in kwargs and expectation_type.startswith(
                "expect_column_value"
            ):
                datasetAssertionInfo = DatasetAssertionInfo(
                    scope=DatasetAssertionScope.DATASET_COLUMN,
                    columnAssertion=DatasetColumnAssertion(
                        stdOperator=AssertionStdOperator._NATIVE_,
                        nativeType=expectation_type,
                        stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
                    ),
                )
            elif "column" in kwargs:
                datasetAssertionInfo = DatasetAssertionInfo(
                    scope=DatasetAssertionScope.DATASET_COLUMN,
                    columnAssertion=DatasetColumnAssertion(
                        stdOperator=AssertionStdOperator._NATIVE_,
                        nativeType=expectation_type,
                        stdAggFunc=DatasetColumnStdAggFunc._NATIVE_,
                    ),
                )
            else:
                datasetAssertionInfo = DatasetAssertionInfo(
                    scope=DatasetAssertionScope.DATASET_ROWS,
                    rowsAssertion=DatasetRowsAssertion(
                        stdOperator=AssertionStdOperator._NATIVE_,
                        nativeType=expectation_type,
                        stdAggFunc=DatasetRowsStdAggFunc._NATIVE_,
                    ),
                )

        # 1. keys `max_val`, `min_val` for to_be_between expectations,
        # except for expect_column_quantile_values_to_be_between having `quantile_ranges`
        # 2. key `value` for singular equal_to, to_be_in_set etc expectations
        # 3. other keys may also be present for unhandled native expectations
        datasetAssertionParameters = {
            (
                "value"
                if k
                in [
                    "values",
                    "value_set",
                    "values_set",
                    "column_list",
                    "column_set",
                    "regex",
                    "regex_list",
                ]
                else k
            ): str(v)
            for k, v in kwargs.items()
            if k not in ["column"]
        }

        return AssertionInfo(
            type=AssertionType.DATASET,
            datasetAssertion=datasetAssertionInfo,
            parameters=datasetAssertionParameters,
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
