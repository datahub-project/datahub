from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    SkipReason,
    run_status_sensor,
)

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub_provider.client.dagster_generator import DATAHUB_REST_URL, DagsterGenerator


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def emit_metadata_sensor(context: RunStatusSensorContext):
    emitter = DatahubRestEmitter(gms_server=DATAHUB_REST_URL)
    emitter.test_connection()

    assert context.dagster_run.job_snapshot_id
    assert context.dagster_run.execution_plan_snapshot_id

    job_snapshot = context.instance.get_job_snapshot(
        snapshot_id=context.dagster_run.job_snapshot_id
    )
    dataflow = DagsterGenerator.generate_dataflow(job_snapshot=job_snapshot)
    dataflow.emit(emitter)

    DagsterGenerator.emit_job_run(
        emitter=emitter,
        dataflow=dataflow,
        run=context.dagster_run,
        run_stats=context.instance.get_run_stats(context.dagster_run.run_id),
    )

    execution_plan_snapshot = context.instance.get_execution_plan_snapshot(
        snapshot_id=context.dagster_run.execution_plan_snapshot_id
    )

    run_step_stats = {
        run_step_stat.step_key: run_step_stat
        for run_step_stat in context.instance.get_run_step_stats(
            context.dagster_run.run_id
        )
    }

    for op_def_snap in job_snapshot.node_defs_snapshot.op_def_snaps:
        datajob = DagsterGenerator.generate_datajob(
            job_snapshot=job_snapshot,
            step_deps=execution_plan_snapshot.step_deps,
            op_def_snap=op_def_snap,
        )
        datajob.emit(emitter)

        DagsterGenerator.emit_op_run(
            emitter=emitter,
            datajob=datajob,
            run_step_stats=run_step_stats[op_def_snap.name],
        )

    yield SkipReason("Datahub emit metadata success")
