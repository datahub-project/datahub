import os
import traceback
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dagster import (
    DagsterRunStatus,
    EventLogEntry,
    RunStatusSensorContext,
    SensorDefinition,
    SkipReason,
    run_status_sensor,
    sensor,
)
from dagster._core.definitions.asset_selection import CoercibleToAssetSelection
from dagster._core.definitions.sensor_definition import (
    DefaultSensorStatus,
    RawSensorEvaluationFunctionReturn,
)
from dagster._core.definitions.target import ExecutableDefinition
from dagster._core.events import DagsterEventType, HandledOutputData, LoadedInputData
from dagster._core.execution.stats import RunStepKeyStatsSnapshot
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SubTypesClass

from datahub_dagster_plugin.client.dagster_generator import (
    DagsterEnvironment,
    DagsterGenerator,
    DatahubDagsterSourceConfig,
)


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
        for each in DatahubSensors(config).sensors:
            each.evaluate_tick(context)
        return SkipReason("Trigger run status sensors if any new runs present...")

    return datahub_sensor


class DatahubSensors:
    def __init__(self, config: Optional[DatahubDagsterSourceConfig] = None):
        """
        Set dagster source configurations and initialize datahub emitter and dagster run status sensors
        """
        if config:
            self.config = config
        else:
            self.config = DatahubDagsterSourceConfig()
        self.graph = DataHubGraph(
            self.config.datahub_client_config,
        )

        self.graph.test_connection()
        self.sensors: List[SensorDefinition] = []
        self.sensors.append(
            run_status_sensor(
                name="datahub_success_sensor", run_status=DagsterRunStatus.SUCCESS
            )(self._emit_metadata)
        )

        self.sensors.append(
            run_status_sensor(
                name="datahub_failure_sensor", run_status=DagsterRunStatus.FAILURE
            )(self._emit_metadata)
        )

        self.sensors.append(
            run_status_sensor(
                name="datahub_canceled_sensor", run_status=DagsterRunStatus.CANCELED
            )(self._emit_metadata)
        )

    def get_dagster_environment(
        self, context: RunStatusSensorContext
    ) -> Optional[DagsterEnvironment]:
        if (
            context.dagster_run.job_code_origin
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
                return None

            dagster_environment = DagsterEnvironment(
                is_cloud=os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", None)
                is not None,
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
        else:
            context.log.error("Unable to get Dagster Environment...")
            return None

    def process_asset_logs(
        self,
        dagster_generator: DagsterGenerator,
        log: EventLogEntry,
        dataset_inputs: Dict[str, set],
        dataset_outputs: Dict[str, set],
    ) -> None:

        if not log.dagster_event or not log.step_key:
            return

        if log.dagster_event.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            if log.step_key not in dataset_outputs:
                dataset_outputs[log.step_key] = set()

            materialization = log.asset_materialization
            if not materialization:
                return

            properties = {
                key: str(value) for (key, value) in materialization.metadata.items()
            }
            asset_key = materialization.asset_key.path
            dataset_urn = dagster_generator.emit_asset(
                self.graph, asset_key, materialization.description, properties
            )
            dataset_outputs[log.step_key].add(dataset_urn)

        elif log.dagster_event.event_type == DagsterEventType.ASSET_OBSERVATION:
            if log.step_key not in dataset_inputs:
                dataset_inputs[log.step_key] = set()
            asset_observation = log.asset_observation
            if not asset_observation:
                return

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

    def process_handle_input_output(
        self,
        context: RunStatusSensorContext,
        log: EventLogEntry,
        dagster_generator: DagsterGenerator,
        dataset_inputs: Dict[str, set],
        dataset_outputs: Dict[str, set],
    ) -> None:
        if not log.dagster_event or not log.step_key:
            return

        if (
            self.config.capture_input_output
            and log.dagster_event.event_type == DagsterEventType.HANDLED_OUTPUT
        ):
            if log.step_key not in dataset_outputs:
                dataset_outputs[log.step_key] = set()

            event_specific_data = log.dagster_event.event_specific_data
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
            if log.step_key not in dataset_inputs:
                dataset_inputs[log.step_key] = set()
            event_specific_data = log.dagster_event.event_specific_data
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
    ) -> Tuple[Dict[str, set], Dict[str, set]]:
        dataset_outputs: Dict[str, set] = {}
        dataset_inputs: Dict[str, set] = {}

        logs = context.instance.all_logs(
            context.dagster_run.run_id,
            {
                DagsterEventType.ASSET_MATERIALIZATION,
                DagsterEventType.ASSET_OBSERVATION,
                DagsterEventType.HANDLED_OUTPUT,
                DagsterEventType.LOADED_INPUT,
            },
        )

        for log in logs:
            if not log.dagster_event or not log.step_key:
                continue
            context.log.debug(f"Log: {log.step_key} - {log.dagster_event}")
            context.log.debug(f"Event Type: {log.dagster_event.event_type}")
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

    def _emit_metadata(
        self, context: RunStatusSensorContext
    ) -> RawSensorEvaluationFunctionReturn:
        """
        Function to emit metadata for datahub rest.
        """
        try:
            context.log.info("Emitting metadata...")

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

            dataset_inputs: Dict[str, Set] = {}
            dataset_outputs: Dict[str, Set] = {}

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
            dataflow.emit(self.graph)

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
                context.log.info(f"Generated Datajob: {datajob}")
                datajob.emit(self.graph)

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

            return SkipReason("Pipeline metadata is emitted to DataHub")
        except Exception as e:
            context.log.error(
                f"Error in emitting metadata to DataHub: {e}. Traceback: {traceback.format_exc()}"
            )
            return SkipReason("Error in emitting metadata to DataHub")
