from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    DataTransform,
    DataTransformLogic,
)
from datahub.metadata.com.linkedin.pegasus2avro.query import QueryStatement
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobSnapshotClass,
    MetadataChangeEventClass,
)


def make_datajob_workunit(
    job_urn: str, job_id: str, job_name: str, description: str = None
) -> MetadataWorkUnit:
    job_info = DataJobInfoClass(
        name=job_name,
        type="ETL",  # or Mapping/Session 등 구체화 가능
        description=description,
        customProperties={},
    )

    snapshot = DataJobSnapshotClass(urn=job_urn, aspects=[job_info])

    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)

    return MetadataWorkUnit(id=f"datajob-{job_id}", mce=mce)


def make_datajob_query_workunit(job_urn: str, query: str) -> MetadataWorkUnit:
    logic = DataTransformLogic(
        [DataTransform(queryStatement=QueryStatement(value=query, language="SQL"))]
    )
    mcp = MetadataChangeProposalWrapper(entityUrn=job_urn, aspect=logic)
    return MetadataWorkUnit(id=f"datajob-query-{job_urn}", mce=mcp)
