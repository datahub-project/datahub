from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import make_data_job_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.mapper import secondary, shared_status
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.sdk import DataFlow, DataJob
from datahub.specific.datajob import DataJobPatchBuilder


def job_urn(
    *, platform: str, flow_id: str, job_id: str, env: str, instance: Optional[str]
) -> str:
    return make_data_job_urn(
        orchestrator=platform,
        flow_id=flow_id,
        job_id=job_id,
        cluster=env,
        platform_instance=instance,
    )


def shared_job_workunits(
    *,
    flow: DataFlow,
    job: DataJob,
    inputs: Iterable[str] = (),
    outputs: Iterable[str] = (),
    input_jobs: Iterable[str] = (),
    fine_grained: Optional[List[FineGrainedLineageClass]] = None,
) -> Iterable[MetadataWorkUnit]:
    """A Lambda function, rule or pipe that both the sqs and sns sources may emit.

    Each source writes only its own edges, as patches, so neither erases the
    other's. The entity is marked non-primary: stale-entity removal in one source
    must not delete a job the other source still describes."""
    patch = DataJobPatchBuilder(str(job.urn))
    for urn in sorted(set(inputs)):
        patch.add_input_dataset(urn)
    for urn in sorted(set(outputs)):
        patch.add_output_dataset(urn)
    for urn in sorted(set(input_jobs)):
        patch.add_input_datajob(urn)
    for lineage in fine_grained or []:
        patch.add_fine_grained_lineage(lineage)
    yield from secondary(flow.as_workunits())
    yield shared_status(str(flow.urn))
    yield from secondary(job.as_workunits())
    yield shared_status(str(job.urn))
    for mcp in patch.build():
        yield MetadataWorkUnit(
            id=f"{job.urn}-io-patch", mcp_raw=mcp, is_primary_source=False
        )
