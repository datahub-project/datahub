from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
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
