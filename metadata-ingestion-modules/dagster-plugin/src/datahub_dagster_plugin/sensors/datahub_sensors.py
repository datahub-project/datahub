import os
import traceback
import warnings
from collections import defaultdict
from types import ModuleType
from typing import Dict, List, NamedTuple, Optional, Sequence, Set, Tuple, Union

from dagster import (
    AssetSelection,
    CodeLocationSelector,
    DagsterRunStatus,
    EventLogEntry,
    GraphDefinition,
    JobDefinition,
    JobSelector,
    MultiAssetSensorEvaluationContext,
    RepositorySelector,
    RunStatusSensorContext,
    SensorDefinition,
    SkipReason,
    TextMetadataValue,
    load_assets_from_modules,
    multi_asset_sensor,
    run_status_sensor,
    sensor,
)
from dagster._core.definitions.asset_selection import CoercibleToAssetSelection
from dagster._core.definitions.multi_asset_sensor_definition import (
    AssetMaterializationFunctionReturn,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

# This SensorReturnTypesUnion is from Dagster 1.9.1+ and is not available in older versions
# of Dagster. We need to import it conditionally to avoid breaking compatibility with older
try:
    from dagster._core.definitions.sensor_definition import SensorReturnTypesUnion
except ImportError:
    from dagster._core.definitions.sensor_definition import (  # type: ignore
        RawSensorEvaluationFunctionReturn as SensorReturnTypesUnion,
    )

from dagster._core.definitions.target import ExecutableDefinition
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster._core.events import DagsterEventType, HandledOutputData, LoadedInputData
from dagster._core.execution.stats import RunStepKeyStatsSnapshot

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import SubTypesClass
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError
from datahub_dagster_plugin.client.dagster_generator import (
    DATAHUB_ASSET_GROUP_NAME_CACHE,
    Constant,
    DagsterEnvironment,
    DagsterGenerator,
    DatahubDagsterSourceConfig,
)


class Lineage(NamedTuple):
    upstreams: List[str]
    downstreams: List[str]


def make_datahub_sensor(
    config: DatahubDagsterSourceConfig,
    name: Optional[str] = None,
    minimum_interval_seconds: Optional[int] = None,
    description: Optional[str] = None,
    job: Optional[ExecutableDefinition] = None,
    jobs: Optional[Sequence[ExecutableDefinition]] = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
    asset_selection: Optional[CoercibleToAssetSelection] = None,
    required_resource_keys: Optional[Set[str]] = None,
    monitored_jobs: Optional[
        Sequence[
            Union[
                JobDefinition,
                GraphDefinition,
                UnresolvedAssetJobDefinition,
                RepositorySelector,
                JobSelector,
                CodeLocationSelector,
            ]
        ]
    ] = None,
    job_selection: Optional[
        Sequence[
            Union[
                JobDefinition,
                GraphDefinition,
                UnresolvedAssetJobDefinition,
                RepositorySelector,
                JobSelector,
                CodeLocationSelector,
            ]
        ]
    ] = None,
    monitor_all_code_locations: Optional[bool] = None,
) -> SensorDefinition:
    """Create a sensor on job status change emit lineage to DataHub.

    Args:
        config (DatahubDagsterSourceConfig): DataHub Sensor config
        name: (Optional[str]): The name of the sensor. Defaults to "datahub_sensor".
        minimum_interval_seconds: (Optional[int]): The minimum number of seconds that will elapse
            between sensor evaluations.
        default_status (DefaultSensorStatus): Whether the sensor starts as running or not. The default
            status can be overridden from Dagit or via the GraphQL API.

    Examples:
        .. code-block:: python

            datahub_sensor = make_datahub_sensor(
               config
            )

            @repository
            def my_repo():
                return [my_job + datahub_sensor]


    """

    @sensor(
        name=name,
        minimum_interval_seconds=minimum_interval_seconds,
        description=description,
        job=job,
        jobs=jobs,
        default_status=default_status,
        asset_selection=asset_selection,
        required_resource_keys=required_resource_keys,
    )
    def datahub_sensor(context):
        """
        Sensor which instigate all run status sensors and trigger them based upon run status
        """
        for each in DatahubSensors(
            config,
            minimum_interval_seconds=minimum_interval_seconds,
            monitored_jobs=monitored_jobs,
            job_selection=job_selection,
            monitor_all_code_locations=monitor_all_code_locations,
        ).sensors:
            each.evaluate_tick(context)
        return SkipReason("Trigger run status sensors if any new runs present...")

    return datahub_sensor


class DatahubSensors:
    def __init__(
        self,
        config: Optional[DatahubDagsterSourceConfig] = None,
        minimum_interval_seconds: Optional[int] = None,
        monitored_jobs: Optional[
            Sequence[
                Union[
                    JobDefinition,
                    GraphDefinition,
                    UnresolvedAssetJobDefinition,
                    "RepositorySelector",
                    "JobSelector",
                    "CodeLocationSelector",
                ]
            ]
        ] = None,
        job_selection: Optional[
            Sequence[
                Union[
                    JobDefinition,
                    GraphDefinition,
                    UnresolvedAssetJobDefinition,
                    "RepositorySelector",
                    "JobSelector",
                    "CodeLocationSelector",
                ]
            ]
        ] = None,
        monitor_all_code_locations: Optional[bool] = None,
    ):
        """
        Set dagster source configurations and initialize datahub emitter and dagster run status sensors
        """
        if config:
            self.config = config
        else:
            # This is a temporary warning for backwards compatibility. Eventually, we'll remove this
            # branch and make the config required.
            warnings.warn(
                "Using the default DataHub client config is deprecated. Pass in a config object explicitly.",
                stacklevel=2,
            )
            self.config = DatahubDagsterSourceConfig(
                datahub_client_config=DatahubClientConfig(
                    server=Constant.DEFAULT_DATAHUB_REST_URL,
                    client_mode=ClientMode.INGESTION,
                    datahub_component="dagster-plugin",
                )
            )
        self.graph = DataHubGraph(
            self.config.datahub_client_config,
        )

        self.graph.test_connection()

        self.sensors: List[SensorDefinition] = []
        self.sensors.append(
            run_status_sensor(
                name="datahub_success_sensor",
                run_status=DagsterRunStatus.SUCCESS,
                minimum_interval_seconds=minimum_interval_seconds,
                monitored_jobs=monitored_jobs,
                job_selection=job_selection,
                monitor_all_code_locations=monitor_all_code_locations,
            )(self._emit_metadata)
        )

        self.sensors.append(
            run_status_sensor(
                name="datahub_failure_sensor",
                run_status=DagsterRunStatus.FAILURE,
                minimum_interval_seconds=minimum_interval_seconds,
                monitored_jobs=monitored_jobs,
                job_selection=job_selection,
                monitor_all_code_locations=monitor_all_code_locations,
            )(self._emit_metadata)
        )

        self.sensors.append(
            run_status_sensor(
                name="datahub_canceled_sensor",
                run_status=DagsterRunStatus.CANCELED,
                minimum_interval_seconds=minimum_interval_seconds,
                monitored_jobs=monitored_jobs,
                job_selection=job_selection,
                monitor_all_code_locations=monitor_all_code_locations,
            )(self._emit_metadata)
        )

        self.sensors.append(
            multi_asset_sensor(
                name="datahub_multi_asset_sensor",
                monitored_assets=AssetSelection.all(),
            )(self._emit_asset_metadata)
        )

    def get_dagster_environment(
        self, context: Optional[RunStatusSensorContext] = None
    ) -> Optional[DagsterEnvironment]:
        module: Optional[str] = None
        repository: Optional[str] = None
        if (
            context
            and context.dagster_run.job_code_origin
            and context.dagster_run.job_code_origin.repository_origin
            and context.dagster_run.job_code_origin.repository_origin.code_pointer
        ):
            code_pointer = (
                context.dagster_run.job_code_origin.repository_origin.code_pointer
            )
            context.log.debug(f"code_pointer: {code_pointer}")

            if hasattr(code_pointer, "attribute"):
                repository = code_pointer.attribute
            else:
                repository = None

            if hasattr(code_pointer, "module"):
                module = code_pointer.module
            else:
                context.log.error("Unable to get Module")

        dagster_environment = DagsterEnvironment(
            is_cloud=os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", None) is not None,
            is_branch_deployment=(
                True
                if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", False) == 1
                else False
            ),
            branch=os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod"),
            module=module,
            repository=repository,
        )
        return dagster_environment

    def parse_sql(
        self,
        context: RunStatusSensorContext,
        sql_query: str,
        env: str,
        platform: str,
        default_db: Optional[str] = None,
        platform_instance: Optional[str] = None,
    ) -> Optional[Lineage]:
        """
        Parse SQL to get table name and columns
        """
        sql_parsing_result: SqlParsingResult = create_lineage_sql_parsed_result(
            query=sql_query,
            graph=self.graph,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            default_db=default_db,
        )

        if not sql_parsing_result.debug_info.table_error:
            context.log.info(
                f"SQL Parsing Result: {sql_parsing_result} for query: {sql_query}"
            )
            return Lineage(
                upstreams=sql_parsing_result.in_tables,
                downstreams=sql_parsing_result.out_tables,
            )
        else:
            context.log.error(
                f"Error in parsing sql: {sql_parsing_result.debug_info.table_error} for query: {sql_query}"
            )
        return None

    def process_asset_materialization(
        self,
        log: EventLogEntry,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        dataset_inputs: Dict[str, Set[DatasetUrn]],
        dataset_outputs: Dict[str, Set[DatasetUrn]],
    ) -> None:
        if not self._is_valid_asset_materialization(log):
            return

        asset_materialization = log.asset_materialization
        if asset_materialization is None:
            return

        asset_key = asset_materialization.asset_key.path
        asset_downstream_urn = self._get_asset_downstream_urn(
            log, context, dagster_generator, list(asset_key)
        )

        if not asset_downstream_urn:
            return

        properties = {
            key: str(value) for (key, value) in asset_materialization.metadata.items()
        }
        upstreams, downstreams = self._process_lineage(
            context=context,
            dagster_generator=dagster_generator,
            log=log,
            asset_downstream_urn=asset_downstream_urn,
        )

        self._emit_or_connect_asset(
            context,
            dagster_generator,
            log,
            list(asset_key),
            properties,
            upstreams,
            downstreams,
            dataset_inputs,
            dataset_outputs,
        )

    def _is_valid_asset_materialization(self, log: EventLogEntry) -> bool:
        return (
            log.dagster_event is not None
            and log.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION
            and log.step_key is not None
            and log.asset_materialization is not None
        )

    def _get_asset_downstream_urn(
        self,
        log: EventLogEntry,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        asset_key: List[str],
    ) -> Optional[DatasetUrn]:
        materialization = log.asset_materialization
        if materialization is None:
            return None

        asset_downstream_urn: Optional[DatasetUrn] = None

        if materialization.metadata.get("datahub_urn") and isinstance(
            materialization.metadata.get("datahub_urn"), TextMetadataValue
        ):
            try:
                asset_downstream_urn = DatasetUrn.from_string(
                    str(materialization.metadata["datahub_urn"].text)
                )
                context.log.info(
                    f"asset_downstream_urn from metadata datahub_urn: {asset_downstream_urn}"
                )
            except Exception as e:
                context.log.error(f"Error in parsing datahub_urn: {e}")

        if not asset_downstream_urn:
            asset_downstream_urn = (
                dagster_generator.asset_keys_to_dataset_urn_converter(asset_key)
            )
            context.log.info(
                f"asset_downstream_urn from asset keys: {asset_downstream_urn}"
            )

        return asset_downstream_urn

    def _process_lineage(
        self,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        log: EventLogEntry,
        asset_downstream_urn: DatasetUrn,
    ) -> Tuple[Set[str], Set[str]]:
        downstreams = {asset_downstream_urn.urn()}
        upstreams: Set[str] = set()

        if (
            log.asset_materialization
            and self.config.enable_asset_query_metadata_parsing
        ):
            try:
                query_metadata = log.asset_materialization.metadata.get("Query")
                if isinstance(query_metadata, TextMetadataValue):
                    lineage = self.parse_sql(
                        context=context,
                        sql_query=str(query_metadata.text),
                        env=asset_downstream_urn.env,
                        platform=asset_downstream_urn.platform.replace(
                            "urn:li:dataPlatform:", ""
                        ),
                    )
                    if lineage and lineage.downstreams:
                        if self.config.emit_queries:
                            dagster_generator.gen_query_aspect(
                                graph=self.graph,
                                platform=asset_downstream_urn.platform,
                                query_subject_urns=lineage.upstreams
                                + lineage.downstreams,
                                query=str(query_metadata.text),
                            )
                        downstreams = downstreams.union(set(lineage.downstreams))
                        upstreams = upstreams.union(set(lineage.upstreams))
                        context.log.info(
                            f"Upstreams: {upstreams} Downstreams: {downstreams}"
                        )
                    else:
                        context.log.info(f"Lineage not found for {query_metadata.text}")
                else:
                    context.log.info("Query not found in metadata")
            except Exception as e:
                context.log.exception(f"Error in processing asset logs: {e}")

        return upstreams, downstreams

    def _emit_or_connect_asset(
        self,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        log: EventLogEntry,
        asset_key: List[str],
        properties: Dict[str, str],
        upstreams: Set[str],
        downstreams: Set[str],
        dataset_inputs: Dict[str, Set[DatasetUrn]],
        dataset_outputs: Dict[str, Set[DatasetUrn]],
    ) -> None:
        if self.config.emit_assets:
            context.log.info("Emitting asset metadata...")
            dataset_urn = dagster_generator.emit_asset(
                self.graph,
                asset_key,
                log.asset_materialization.description
                if log.asset_materialization
                else None,
                properties,
                downstreams=downstreams,
                upstreams=upstreams,
                materialize_dependencies=self.config.materialize_dependencies,
            )
            if log.step_key:
                dataset_outputs[log.step_key].add(dataset_urn)
        else:
            context.log.info(
                "Not emitting assets but connecting materialized dataset to DataJobs"
            )
            if log.step_key:
                dataset_outputs[log.step_key] = dataset_outputs[log.step_key].union(
                    [DatasetUrn.from_string(d) for d in downstreams]
                )

                dataset_upstreams: List[DatasetUrn] = []

                for u in upstreams:
                    try:
                        dataset_upstreams.append(DatasetUrn.from_string(u))
                    except InvalidUrnError as e:
                        context.log.error(
                            f"Error in parsing upstream dataset urn: {e}", exc_info=True
                        )
                        continue

                dataset_inputs[log.step_key] = dataset_inputs[log.step_key].union(
                    dataset_upstreams
                )
                context.log.info(
                    f"Dataset Inputs: {dataset_inputs[log.step_key]} Dataset Outputs: {dataset_outputs[log.step_key]}"
                )

    def process_asset_observation(
        self,
        log: EventLogEntry,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        dataset_inputs: Dict[str, set],
        dataset_outputs: Dict[str, set],
    ) -> None:
        asset_observation = log.asset_observation
        if not asset_observation:
            return

        assert log.step_key

        properties = {
            key: str(value)
            for (key, value) in asset_observation.metadata.items()  # type: ignore
        }
        asset_key = asset_observation.asset_key.path  # type: ignore
        dataset_urn = dagster_generator.emit_asset(
            self.graph,
            asset_key,
            asset_observation.description,
            properties,  # type: ignore
        )
        dataset_inputs[log.step_key].add(dataset_urn)

    def process_asset_logs(
        self,
        context: RunStatusSensorContext,
        dagster_generator: DagsterGenerator,
        log: EventLogEntry,
        dataset_inputs: Dict[str, Set[DatasetUrn]],
        dataset_outputs: Dict[str, Set[DatasetUrn]],
    ) -> None:
        context.log.info("Processing Asset Logs")
        if not log.dagster_event or not log.step_key:
            return

        if log.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            self.process_asset_materialization(
                log=log,
                context=context,
                dagster_generator=dagster_generator,
                dataset_inputs=dataset_inputs,
                dataset_outputs=dataset_outputs,
            )
        elif log.dagster_event.event_type == DagsterEventType.ASSET_OBSERVATION:
            self.process_asset_observation(
                log=log,
                context=context,
                dagster_generator=dagster_generator,
                dataset_inputs=dataset_inputs,
                dataset_outputs=dataset_outputs,
            )

    def process_handle_input_output(
        self,
        context: RunStatusSensorContext,
        log: EventLogEntry,
        dagster_generator: DagsterGenerator,
        dataset_inputs: Dict[str, Set[DatasetUrn]],
        dataset_outputs: Dict[str, Set[DatasetUrn]],
    ) -> None:
        if not log.dagster_event or not log.step_key:
            return

        event_specific_data = log.dagster_event.event_specific_data
        if (
            self.config.capture_input_output
            and log.dagster_event.event_type == DagsterEventType.HANDLED_OUTPUT
        ):
            if isinstance(event_specific_data, HandledOutputData):
                context.log.debug(
                    f"Output Path: {event_specific_data.metadata.get('path')}"
                )
                metadata = event_specific_data.metadata.get("path")
                context.log.debug(f"Metadata: {metadata}")
                if not metadata:
                    return
                urn = dagster_generator.metadata_resolver(metadata)
                if urn:
                    context.log.debug(f"Output Urn: {urn}")
                    dataset_outputs[log.step_key].add(urn)
        elif (
            self.config.capture_input_output
            and log.dagster_event.event_type == DagsterEventType.LOADED_INPUT
        ):
            if isinstance(event_specific_data, LoadedInputData):
                context.log.debug(
                    f"Input Path: {event_specific_data.metadata.get('path')}"
                )
                metadata = event_specific_data.metadata.get("path")
                context.log.debug(f"Metadata: {metadata}")
                if not metadata:
                    return
                urn = dagster_generator.metadata_resolver(metadata)
                if urn:
                    context.log.debug(f"Input Urn: {urn}")
                    dataset_inputs[log.step_key].add(urn)

    def process_dagster_logs(
        self, context: RunStatusSensorContext, dagster_generator: DagsterGenerator
    ) -> Tuple[Dict[str, Set[DatasetUrn]], Dict[str, Set[DatasetUrn]]]:
        dataset_outputs: Dict[str, Set[DatasetUrn]] = defaultdict(lambda: set())
        dataset_inputs: Dict[str, Set[DatasetUrn]] = defaultdict(lambda: set())

        logs = context.instance.all_logs(
            context.dagster_run.run_id,
            {
                DagsterEventType.ASSET_MATERIALIZATION,
                DagsterEventType.ASSET_OBSERVATION,
                DagsterEventType.HANDLED_OUTPUT,
                DagsterEventType.LOADED_INPUT,
                DagsterEventType.RESOURCE_INIT_SUCCESS,
            },
        )

        for log in logs:
            if not log.dagster_event or not log.step_key:
                continue
            context.log.debug(f"Log: {log.step_key} - {log.dagster_event}")
            context.log.debug(f"Event Type: {log.dagster_event.event_type}")
            context.log.info(f"Log: {log.step_key} - {log.dagster_event}")
            context.log.info(f"Event Type: {log.dagster_event.event_type}")

            if self.config.capture_input_output:
                self.process_handle_input_output(
                    context=context,
                    log=log,
                    dagster_generator=dagster_generator,
                    dataset_inputs=dataset_inputs,
                    dataset_outputs=dataset_outputs,
                )

            if self.config.capture_asset_materialization:
                self.process_asset_logs(
                    context=context,
                    dagster_generator=dagster_generator,
                    log=log,
                    dataset_inputs=dataset_inputs,
                    dataset_outputs=dataset_outputs,
                )

        return dataset_inputs, dataset_outputs

    @staticmethod
    def merge_dicts(dict1: Dict[str, Set], dict2: Dict[str, Set]) -> Dict[str, Set]:
        """
        Merge two dictionaries
        """
        for key, value in dict2.items():
            if key in dict1:
                dict1[key] = dict1[key].union(value)
            else:
                dict1[key] = value
        return dict1

    def _emit_asset_metadata(
        self, context: MultiAssetSensorEvaluationContext
    ) -> AssetMaterializationFunctionReturn:
        dagster_environment = self.get_dagster_environment()
        context.log.debug(f"dagster enivronment: {dagster_environment}")
        if not dagster_environment:
            return SkipReason("Unable to get Dagster Environment from DataHub Sensor")

        dagster_generator = DagsterGenerator(
            logger=context.log,
            config=self.config,
            dagster_environment=dagster_environment,
        )

        context.log.info(
            f"Updating asset group name cache... {DATAHUB_ASSET_GROUP_NAME_CACHE}"
        )
        dagster_generator.update_asset_group_name_cache(context)

        return SkipReason("Asset metadata processed")

    def _emit_metadata(self, context: RunStatusSensorContext) -> SensorReturnTypesUnion:
        """
        Function to emit metadata for datahub rest.
        """
        try:
            context.log.info("Emitting metadata...")
            assets = load_assets_from_modules([ModuleType(name="dagster_jobs")])
            context.log.info(f"Assets: {assets}")
            context.log.info(f"Job Name: {context.dagster_run.job_name}")
            context.log.info(f"Code Origin: {context.dagster_run.job_code_origin}")
            context.log.info(f"Resource: {context.resources.original_resource_dict}")
            context.log.info(f"Asset Key: {context.dagster_event.asset_key}")

            context.log.info(f"Step Key: {context.dagster_event.step_key}")
            context.log.info(
                f"Event Specific Data: {context.dagster_event.event_specific_data}"
            )
            context.log.info(f"Tags: {context.dagster_run.tags}")
            context.log.info(
                f"Run Stats: {context.instance.get_run_stats(context.dagster_run.run_id)}"
            )
            context.log.info(
                f"Run Stats: {context.instance.get_run_record_by_id(context.dagster_run.run_id)}"
            )
            assert context.dagster_run.job_snapshot_id
            assert context.dagster_run.execution_plan_snapshot_id

            dagster_environment = self.get_dagster_environment(context)
            context.log.debug(f"dagster enivronment: {dagster_environment}")
            if not dagster_environment:
                return SkipReason(
                    "Unable to get Dagster Environment from DataHub Sensor"
                )

            context.log.debug(f"Dagster Environment: {dagster_environment}")

            dagster_generator = DagsterGenerator(
                logger=context.log,
                config=self.config,
                dagster_environment=dagster_environment,
            )

            job_snapshot = context.instance.get_job_snapshot(
                snapshot_id=context.dagster_run.job_snapshot_id
            )

            dataset_inputs: Dict[str, Set[DatasetUrn]] = {}
            dataset_outputs: Dict[str, Set[DatasetUrn]] = {}

            if self.config.asset_lineage_extractor:
                asset_lineages = self.config.asset_lineage_extractor(
                    context, dagster_generator, self.graph
                )
                for key, value in asset_lineages.items():
                    dataset_inputs[key] = dataset_inputs.get(key, set()).union(
                        value.inputs
                    )
                    dataset_outputs[key] = dataset_outputs.get(key, set()).union(
                        value.outputs
                    )

            (
                dataset_inputs_from_logs,
                dataset_outputs_from_logs,
            ) = self.process_dagster_logs(context, dagster_generator)

            dataset_inputs = DatahubSensors.merge_dicts(
                dataset_inputs, dataset_inputs_from_logs
            )
            dataset_outputs = DatahubSensors.merge_dicts(
                dataset_outputs, dataset_outputs_from_logs
            )

            context.log.debug(f"Outputs: {dataset_outputs}")
            # Emit dagster job entity which get mapped with datahub dataflow entity
            dataflow = dagster_generator.generate_dataflow(
                job_snapshot=job_snapshot,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            if (
                dataflow.name
                and dataflow.name.startswith("__ASSET_JOB")
                and dataflow.name.split("__")
            ):
                dagster_generator.generate_browse_path(
                    dataflow.name.split("__"), urn=dataflow.urn, graph=self.graph
                )
                dataflow.name = dataflow.name.split("__")[-1]

            dataflow.emit(self.graph)
            if self.config.debug_mode:
                for mcp in dataflow.generate_mcp():
                    if self.config.debug_mode:
                        context.log.debug(f"Emitted MCP: {mcp}")

            # Emit dagster job run which get mapped with datahub data process instance entity
            dagster_generator.emit_job_run(
                graph=self.graph,
                dataflow=dataflow,
                run=context.dagster_run,
                run_stats=context.instance.get_run_stats(context.dagster_run.run_id),
            )

            # Execution plan snapshot contains all steps(ops) dependency.
            execution_plan_snapshot = context.instance.get_execution_plan_snapshot(
                snapshot_id=context.dagster_run.execution_plan_snapshot_id
            )

            # Map step key with its run step stats
            run_step_stats: Dict[str, RunStepKeyStatsSnapshot] = {
                run_step_stat.step_key: run_step_stat
                for run_step_stat in context.instance.get_run_step_stats(
                    context.dagster_run.run_id
                )
            }

            # For all dagster ops present in job:
            # Emit op entity which get mapped with datahub datajob entity.
            # Emit op run which get mapped with datahub data process instance entity.
            for op_def_snap in job_snapshot.node_defs_snapshot.op_def_snaps:
                datajob = dagster_generator.generate_datajob(
                    job_snapshot=job_snapshot,
                    step_deps=execution_plan_snapshot.step_deps,
                    op_def_snap=op_def_snap,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                    output_datasets=dataset_outputs,
                    input_datasets=dataset_inputs,
                )

                if (
                    datajob.name
                    and datajob.name.startswith("__ASSET_JOB")
                    and datajob.name.split("__")
                ):
                    dagster_generator.generate_browse_path(
                        datajob.name.split("__"), urn=datajob.urn, graph=self.graph
                    )
                    datajob.name = datajob.name.split("__")[-1]

                datajob.emit(self.graph)

                if self.config.debug_mode:
                    for mcp in datajob.generate_mcp():
                        context.log.debug(f"Emitted MCP: {mcp}")

                self.graph.emit_mcp(
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=str(datajob.urn),
                        aspect=SubTypesClass(
                            typeNames=["Op"],
                        ),
                    )
                )

                dagster_generator.emit_op_run(
                    graph=self.graph,
                    datajob=datajob,
                    run_step_stats=run_step_stats[op_def_snap.name],
                )
            context.log.info("Metadata emitted to DataHub successfully")
            return SkipReason("Pipeline metadata is emitted to DataHub")
        except Exception as e:
            context.log.error(
                f"Error in emitting metadata to DataHub: {e}. Traceback: {traceback.format_exc()}"
            )
            return SkipReason("Error in emitting metadata to DataHub")
