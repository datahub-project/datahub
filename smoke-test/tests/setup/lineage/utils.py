from typing import List

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EdgeClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamClass,
)
from tests.setup.lineage.constants import (
    DATA_FLOW_ENTITY_TYPE,
    DATA_FLOW_INFO_ASPECT_NAME,
    DATA_JOB_ENTITY_TYPE,
    DATA_JOB_INFO_ASPECT_NAME,
    DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
    DATASET_ENTITY_TYPE,
)
from tests.setup.lineage.helper_classes import Dataset, Pipeline


def create_node(dataset: Dataset) -> List[MetadataChangeProposalWrapper]:
    mcps: List[MetadataChangeProposalWrapper] = []
    dataset_urn = make_dataset_urn(platform=dataset.platform, name=dataset.id)
    data_platform_urn = make_data_platform_urn(dataset.platform)
    print(dataset)
    print(dataset_urn)

    dataset_properties = DatasetPropertiesClass(
        name=dataset.id.split(".")[-1],
    )
    mcps.append(
        MetadataChangeProposalWrapper(
            entityType=DATASET_ENTITY_TYPE,
            entityUrn=dataset_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProperties",
            aspect=dataset_properties,
        )
    )

    dataset_schema = SchemaMetadataClass(
        schemaName="schema",
        platform=data_platform_urn,
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=[
            SchemaFieldClass(fieldPath=f.name, type=f.type, nativeDataType=str(f.type))
            for f in dataset.schema_metadata
        ]
        if dataset.schema_metadata
        else [],
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityType=DATASET_ENTITY_TYPE,
            entityUrn=dataset_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="schemaMetadata",
            aspect=dataset_schema,
        )
    )
    return mcps


def create_edge(
    source_urn: str,
    destination_urn: str,
    created_timestamp_millis: int,
    updated_timestamp_millis: int,
) -> EdgeClass:
    created_audit_stamp: AuditStampClass = AuditStampClass(
        time=created_timestamp_millis, actor="urn:li:corpuser:unknown"
    )
    updated_audit_stamp: AuditStampClass = AuditStampClass(
        time=updated_timestamp_millis, actor="urn:li:corpuser:unknown"
    )
    return EdgeClass(
        sourceUrn=source_urn,
        destinationUrn=destination_urn,
        created=created_audit_stamp,
        lastModified=updated_audit_stamp,
    )


def create_nodes_and_edges(
    airflow_dag: Pipeline,
) -> List[MetadataChangeProposalWrapper]:
    mcps = []
    data_flow_urn = make_data_flow_urn(
        orchestrator=airflow_dag.platform, flow_id=airflow_dag.name
    )
    data_flow_info = DataFlowInfoClass(name=airflow_dag.name)
    mcps.append(
        MetadataChangeProposalWrapper(
            entityType=DATA_FLOW_ENTITY_TYPE,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=data_flow_urn,
            aspectName=DATA_FLOW_INFO_ASPECT_NAME,
            aspect=data_flow_info,
        )
    )

    for task in airflow_dag.tasks:
        data_job_urn = make_data_job_urn_with_flow(
            flow_urn=data_flow_urn, job_id=task.name
        )
        data_job_info = DataJobInfoClass(
            name=task.name,
            type="SnapshotETL",
            flowUrn=data_flow_urn,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityType=DATA_JOB_ENTITY_TYPE,
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=data_job_urn,
                aspectName=DATA_JOB_INFO_ASPECT_NAME,
                aspect=data_job_info,
            )
        )
        data_job_io = DataJobInputOutputClass(
            inputDatasets=[],
            outputDatasets=[],
            inputDatasetEdges=task.upstream_edges,
            outputDatasetEdges=task.downstream_edges,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityType=DATA_JOB_ENTITY_TYPE,
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=data_job_urn,
                aspectName=DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
                aspect=data_job_io,
            )
        )

    return mcps


def create_upstream_edge(
    upstream_entity_urn: str,
    created_timestamp_millis: int,
    updated_timestamp_millis: int,
):
    created_audit_stamp: AuditStampClass = AuditStampClass(
        time=created_timestamp_millis, actor="urn:li:corpuser:unknown"
    )
    updated_audit_stamp: AuditStampClass = AuditStampClass(
        time=updated_timestamp_millis, actor="urn:li:corpuser:unknown"
    )
    upstream: UpstreamClass = UpstreamClass(
        dataset=upstream_entity_urn,
        type=DatasetLineageTypeClass.TRANSFORMED,
        auditStamp=updated_audit_stamp,
        created=created_audit_stamp,
    )
    return upstream


def create_upstream_mcp(
    entity_type: str,
    entity_urn: str,
    upstreams: List[UpstreamClass],
    timestamp_millis: int,
    run_id: str = "",
) -> MetadataChangeProposalWrapper:
    print(f"Creating upstreamLineage aspect for {entity_urn}")
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        entityUrn=entity_urn,
        changeType=ChangeTypeClass.UPSERT,
        aspectName="upstreamLineage",
        aspect=UpstreamLineage(upstreams=upstreams),
        systemMetadata=SystemMetadata(
            lastObserved=timestamp_millis,
            runId=run_id,
        ),
    )
    return mcp


def emit_mcps(
    graph_client: DataHubGraph, mcps: List[MetadataChangeProposalWrapper]
) -> None:
    for mcp in mcps:
        graph_client.emit_mcp(mcp)
