from typing import Dict, List

from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    SensorDefinition,
    SkipReason,
    run_status_sensor,
    sensor,
)
from dagster._core.definitions.sensor_definition import (
    RawSensorEvaluationFunctionReturn,
)
from dagster._core.execution.stats import RunStepKeyStatsSnapshot
from datahub.emitter.rest_emitter import DatahubRestEmitter

from datahub_dagster_plugin.client.dagster_generator import (
    DagsterGenerator,
    DagsterSourceConfig,
)


@sensor()
def datahub_sensor(context):
    """
    Sensor which instigate all run status sensors and trigger them based upon run status
    """
    for each in DatahubSensors().sensors:
        each.evaluate_tick(context)
    return SkipReason("Trigger run status sensors if any new runs present...")


class DatahubSensors:
    def __init__(self):
        """
        Set dagster source configurations and initialize datahub emitter and dagster run status sensors
        """
        self.config: DagsterSourceConfig = DagsterSourceConfig()

        self.emitter: DatahubRestEmitter = DatahubRestEmitter(
            gms_server=self.config.datahub_rest_url
        )
        self.emitter.test_connection()

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

    def _emit_metadata(
        self, context: RunStatusSensorContext
    ) -> RawSensorEvaluationFunctionReturn:
        """
        Function to emit metadata for datahub rest.
        """
        context.log.info("Emitting metadata...")

        assert context.dagster_run.job_snapshot_id
        assert context.dagster_run.execution_plan_snapshot_id

        job_snapshot = context.instance.get_job_snapshot(
            snapshot_id=context.dagster_run.job_snapshot_id
        )
        # Emit dagster job entity which get mapped with datahub dataflow entity
        dataflow = DagsterGenerator.generate_dataflow(
            job_snapshot=job_snapshot,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        dataflow.emit(self.emitter)

        # Emit dagster job run which get mapped with datahub data process instance entity
        DagsterGenerator.emit_job_run(
            emitter=self.emitter,
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
            datajob = DagsterGenerator.generate_datajob(
                job_snapshot=job_snapshot,
                step_deps=execution_plan_snapshot.step_deps,
                op_def_snap=op_def_snap,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
            datajob.emit(self.emitter)

            DagsterGenerator.emit_op_run(
                emitter=self.emitter,
                datajob=datajob,
                run_step_stats=run_step_stats[op_def_snap.name],
            )

        return SkipReason("Pipeline metadata is emitted to DataHub")
