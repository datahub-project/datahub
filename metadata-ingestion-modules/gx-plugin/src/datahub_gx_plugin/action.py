import logging
import sys
from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

# Load-bearing for its import side effect, not just for has_name_positional_arg:
# it raises the "use action_v1" ImportError under GX 1.x before the
# great_expectations imports below reach the data_asset package that 1.x removed.
# Keep this import even if has_name_positional_arg ever becomes unused.
from datahub_gx_plugin._compat_gx_0x import has_name_positional_arg
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from great_expectations.checkpoint.actions import ValidationAction
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import (
    RuntimeDataBatchSpec,
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.validator.validator import Validator
from sqlalchemy.engine.base import Connection, Engine

import datahub.emitter.mce_builder as builder
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.emitter.rest_emitter import DatahubRestEmitter, EmitMode
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.graph.config import ClientMode
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import BatchSpec
from datahub.metadata.schema_classes import PartitionSpecClass, PartitionTypeClass
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Re-export helpers historically imported from this module in tests/callers.
from datahub_gx_plugin.common import (  # noqa: F401
    DataHubStdAssertion,
    DecimalEncoder,
    build_assertion_info,
    build_assertions_with_results,
    coerce_emit_mode,
    convert_to_string,
    docs_link_from_legacy_payload,
    emit_assertion_results,
    make_dataset_urn_from_sqlalchemy_uri,
    parse_int_or_default,
    warn,
)

if TYPE_CHECKING:
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )

assert MARKUPSAFE_PATCHED
logger = logging.getLogger(__name__)
if get_boolean_env_variable("DATAHUB_DEBUG", False):
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


class DataHubValidationAction(ValidationAction):
    def __init__(
        self,
        data_context: AbstractDataContext,
        # this would capture `name` positional arg added in GX 0.18.14
        *args: Union[str, Any],
        server_url: str,
        env: str = builder.DEFAULT_ENV,
        platform_alias: Optional[str] = None,
        platform_instance_map: Optional[Dict[str, str]] = None,
        graceful_exceptions: bool = True,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        exclude_dbname: Optional[bool] = None,
        parse_table_names_from_sql: bool = False,
        convert_urns_to_lowercase: bool = False,
        emit_mode: Union[str, EmitMode] = EmitMode.ASYNC,
        name: str = "DataHubValidationAction",
    ):
        if has_name_positional_arg:
            if len(args) >= 1 and isinstance(args[0], str):
                name = args[0]
            super().__init__(data_context, name)
        else:
            super().__init__(data_context)
        self.server_url = server_url
        self.env = env
        self.platform_alias = platform_alias
        self.platform_instance_map = platform_instance_map
        self.graceful_exceptions = graceful_exceptions
        self.token = token
        self.timeout_sec = timeout_sec
        self.retry_status_codes = retry_status_codes
        self.retry_max_times = retry_max_times
        self.extra_headers = extra_headers
        self.exclude_dbname = exclude_dbname
        self.parse_table_names_from_sql = parse_table_names_from_sql
        self.convert_urns_to_lowercase = convert_urns_to_lowercase
        # Coerce here because GX passes action kwargs from checkpoint YAML as
        # plain strings; the emitter needs a real EmitMode enum downstream.
        self.emit_mode = coerce_emit_mode(emit_mode)

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, "GXCloudIdentifier"
        ],
        data_asset: Union[Validator, DataAsset, Batch],
        payload: Optional[Any] = None,
        expectation_suite_identifier: Optional[ExpectationSuiteIdentifier] = None,
        checkpoint_identifier: Optional[Any] = None,
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
                client_mode=ClientMode.INGESTION,
                datahub_component="gx-plugin",
                default_emit_mode=self.emit_mode,
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
                expectation_suite_name = validation_result_suite_identifier.expectation_suite_identifier.expectation_suite_name
                run_id = validation_result_suite_identifier.run_id
                batch_identifier = validation_result_suite_identifier.batch_identifier

            # Returns datasets and corresponding batch requests
            datasets = self.get_dataset_partitions(batch_identifier, data_asset)

            if len(datasets) == 0 or datasets[0]["dataset_urn"] is None:
                warn("Metadata not sent to datahub. No datasets found.")
                return {"datahub_notification_result": "none required"}

            # Returns assertion info and assertion results
            assertions = self.get_assertions_with_results(
                validation_result_suite,
                expectation_suite_name,
                run_id,
                payload,
                datasets,
            )

            logger.info("Sending metadata to datahub ...")
            logger.info("Dataset URN - {urn}".format(urn=datasets[0]["dataset_urn"]))

            emit_assertion_results(emitter, assertions)
            logger.info("Metadata sent to datahub.")
            result = "DataHub notification succeeded"
        except Exception as e:
            result = "DataHub notification failed"
            if self.graceful_exceptions:
                logger.error(e)
                logger.info("Suppressing error because graceful_exceptions is set")
            else:
                raise

        return {"datahub_notification_result": result}

    def get_assertions_with_results(
        self,
        validation_result_suite,
        expectation_suite_name,
        run_id,
        payload,
        datasets,
    ):
        return build_assertions_with_results(
            validation_result_suite,
            expectation_suite_name,
            run_id,
            datasets,
            docs_link=docs_link_from_legacy_payload(payload),
        )

    def get_assertion_info(
        self, expectation_type, kwargs, dataset, fields, expectation_suite_name
    ):
        return build_assertion_info(
            expectation_type, kwargs, dataset, fields, expectation_suite_name
        )

    def get_dataset_partitions(self, batch_identifier, data_asset):
        dataset_partitions: List[
            Dict[str, Union[PartitionSpecClass, BatchSpec, str, None]]
        ] = []

        logger.debug("Finding datasets being validated")

        # for now, we support only v3-api and sqlalchemy execution engine,Pandas engine and Spark engine
        is_sql_alchemy = isinstance(data_asset, Validator) and (
            isinstance(data_asset.execution_engine, SqlAlchemyExecutionEngine)
        )
        is_pandas = isinstance(data_asset.execution_engine, PandasExecutionEngine)

        is_spark = isinstance(data_asset.execution_engine, SparkDFExecutionEngine)

        if is_spark:
            ge_batch_spec = data_asset.active_batch_spec
            partitionSpec = None
            batchSpecProperties = {
                "data_asset_name": str(
                    data_asset.active_batch_definition.data_asset_name
                ),
                "datasource_name": str(
                    data_asset.active_batch_definition.datasource_name
                ),
            }

            if isinstance(ge_batch_spec, RuntimeDataBatchSpec):
                data_platform = self.get_platform_instance_spark(
                    data_asset.active_batch_definition.datasource_name
                )

                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=(
                        data_platform
                        if self.platform_alias is None
                        else self.platform_alias
                    ),
                    name=data_asset.active_batch_definition.data_asset_name,
                    platform_instance="",
                    env=self.env,
                )

                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    query="",
                    customProperties=batchSpecProperties,
                )
                dataset_partitions.append(
                    {
                        "dataset_urn": dataset_urn,
                        "partitionSpec": partitionSpec,
                        "batchSpec": batchSpec,
                    }
                )
            else:
                warn(
                    "DataHubValidationAction does not recognize this GE batch spec type for SparkDFExecutionEngine- {batch_spec_type}. No action will be taken.".format(
                        batch_spec_type=type(ge_batch_spec)
                    )
                )
        elif is_sql_alchemy or is_pandas:
            ge_batch_spec = data_asset.active_batch_spec
            partitionSpec = None
            batchSpecProperties = {
                "data_asset_name": str(
                    data_asset.active_batch_definition.data_asset_name
                ),
                "datasource_name": str(
                    data_asset.active_batch_definition.datasource_name
                ),
            }
            sqlalchemy_uri = None

            if is_sql_alchemy and isinstance(
                data_asset.execution_engine.engine, Engine
            ):
                sqlalchemy_uri = data_asset.execution_engine.engine.url
            # For snowflake sqlalchemy_execution_engine.engine is actually instance of Connection
            elif is_sql_alchemy and isinstance(
                data_asset.execution_engine.engine, Connection
            ):
                sqlalchemy_uri = data_asset.execution_engine.engine.engine.url

            if isinstance(ge_batch_spec, SqlAlchemyDatasourceBatchSpec):
                # e.g. ConfiguredAssetSqlDataConnector with splitter_method or sampling_method
                schema_name = ge_batch_spec.get("schema_name")
                table_name = ge_batch_spec.get("table_name")

                dataset_urn = make_dataset_urn_from_sqlalchemy_uri(
                    sqlalchemy_uri,
                    schema_name,
                    table_name,
                    self.env,
                    self.get_platform_instance_sqlalchemy(
                        data_asset.active_batch_definition.datasource_name
                    ),
                    self.exclude_dbname,
                    self.platform_alias,
                    self.convert_urns_to_lowercase,
                )
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
                    partitionSpec = PartitionSpecClass(
                        partition=convert_to_string(batch_identifiers)
                    )
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
                if not self.parse_table_names_from_sql:
                    warn(
                        "Enable parse_table_names_from_sql in DatahubValidationAction config\
                            to try to parse the tables being asserted from SQL query"
                    )
                    return []
                query = data_asset.batches[
                    batch_identifier
                ].batch_request.runtime_parameters["query"]
                partitionSpec = PartitionSpecClass(
                    type=PartitionTypeClass.QUERY,
                    partition=f"Query_{builder.datahub_guid(pre_json_transform(query))}",
                )

                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    query=query,
                    customProperties=batchSpecProperties,
                )

                data_platform = get_platform_from_sqlalchemy_uri(str(sqlalchemy_uri))
                sql_parser_in_tables = create_lineage_sql_parsed_result(
                    query=query,
                    platform=data_platform,
                    env=self.env,
                    platform_instance=None,
                    default_db=None,
                )
                tables = [
                    DatasetUrn.from_string(table_urn).name
                    for table_urn in sql_parser_in_tables.in_tables
                ]
                if sql_parser_in_tables.debug_info.table_error:
                    logger.warning(
                        f"Sql parser failed on {query} with {sql_parser_in_tables.debug_info.table_error}"
                    )
                    tables = []

                if len(set(tables)) != 1:
                    warn(
                        "DataHubValidationAction does not support cross dataset assertions."
                    )
                    return []
                for table in tables:
                    dataset_urn = make_dataset_urn_from_sqlalchemy_uri(
                        sqlalchemy_uri,
                        None,
                        table,
                        self.env,
                        self.get_platform_instance_sqlalchemy(
                            data_asset.active_batch_definition.datasource_name
                        ),
                        self.exclude_dbname,
                        self.platform_alias,
                        self.convert_urns_to_lowercase,
                    )
                    dataset_partitions.append(
                        {
                            "dataset_urn": dataset_urn,
                            "partitionSpec": partitionSpec,
                            "batchSpec": batchSpec,
                        }
                    )
            elif isinstance(ge_batch_spec, RuntimeDataBatchSpec):
                data_platform = self.get_platform_instance_sqlalchemy(
                    data_asset.active_batch_definition.datasource_name
                )
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=(
                        data_platform
                        if self.platform_alias is None
                        else self.platform_alias
                    ),
                    name=data_asset.active_batch_definition.datasource_name,
                    platform_instance="",
                    env=self.env,
                )
                batchSpec = BatchSpec(
                    nativeBatchId=batch_identifier,
                    query="",
                    customProperties=batchSpecProperties,
                )
                dataset_partitions.append(
                    {
                        "dataset_urn": dataset_urn,
                        "partitionSpec": partitionSpec,
                        "batchSpec": batchSpec,
                    }
                )
            else:
                warn(
                    "DataHubValidationAction does not recognize this GE batch spec type- {batch_spec_type}.".format(
                        batch_spec_type=type(ge_batch_spec)
                    )
                )
        else:
            # TODO - v2-spec - SqlAlchemyDataset support
            warn(
                "DataHubValidationAction does not recognize this GE data asset type - {asset_type}.".format(
                    asset_type=type(data_asset)
                )
            )

        return dataset_partitions

    def get_platform_instance_sqlalchemy(self, datasource_name):
        if self.platform_instance_map and datasource_name in self.platform_instance_map:
            return self.platform_instance_map[datasource_name]
        else:
            warn(
                f"Datasource {datasource_name} is not present in platform_instance_map"
            )
        return None

    def get_platform_instance_spark(self, datasource_name):
        if self.platform_instance_map and datasource_name in self.platform_instance_map:
            return self.platform_instance_map[datasource_name]
        else:
            warn(
                f"Datasource {datasource_name} is not present in platform_instance_map. \
                        Data platform will be {datasource_name} by default "
            )
            return datasource_name
