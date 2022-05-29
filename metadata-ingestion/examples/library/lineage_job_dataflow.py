import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.datajob import DataJobInfoClass
from datahub.metadata.schema_classes import ChangeTypeClass, DataFlowInfoClass

# Construct the DataJobInfo aspect with the job -> flow lineage.
dataflow_urn = builder.make_data_flow_urn(
    orchestrator="airflow", flow_id="flow_old_api", cluster="prod"
)

dataflow_info = DataFlowInfoClass(name="LowLevelApiFlow")

dataflow_info_mcp = MetadataChangeProposalWrapper(
    entityType="dataflow",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataflow_urn,
    aspectName="dataFlowInfo",
    aspect=dataflow_info,
)

datajob_info = DataJobInfoClass(name="My Job 1", type="AIRFLOW", flowUrn=dataflow_urn)

# Construct a MetadataChangeProposalWrapper object with the DataJobInfo aspect.
# NOTE: This will overwrite all of the existing dataJobInfo aspect information associated with this job.
datajob_info_mcp = MetadataChangeProposalWrapper(
    entityType="dataJob",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_data_job_urn(
        orchestrator="airflow", flow_id="flow_old_api", job_id="job1", cluster="prod"
    ),
    aspectName="dataJobInfo",
    aspect=datajob_info,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(dataflow_info_mcp)
emitter.emit_mcp(datajob_info_mcp)
