from datahub.emitter.mce_builder import make_data_flow_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.informatica.data_classes import WorkflowInfo
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataFlowSnapshotClass,
    MetadataChangeEventClass,
)


def make_dataflow_workunit(
    flow_id: str, workflow_info: WorkflowInfo, env: str
) -> MetadataWorkUnit:
    flow_urn = make_data_flow_urn("informatica", flow_id, env)
    props = {
        "server": workflow_info.server_name,
        "scheduler": workflow_info.scheduler_name,
        "last_update_time": str(workflow_info.last_update_time),
    }

    flow_info = DataFlowInfoClass(
        name=flow_id,
        description=workflow_info.workflow_desc,
        project=None,
        customProperties={k: str(v) for k, v in workflow_info.dict().items() if v},
        env=env,
    )

    snapshot = DataFlowSnapshotClass(urn=flow_urn, aspects=[flow_info])

    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)

    return MetadataWorkUnit(id=f"dataflow-{flow_id}", mce=mce)
