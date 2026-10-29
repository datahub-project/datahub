from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_data_job_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.ingestion.source.aws_messaging.constants import (
    EVENTBRIDGE_PIPES_PLATFORM,
    EVENTBRIDGE_PLATFORM,
    LAMBDA_PLATFORM,
    JobEdgeKind,
)
from datahub.ingestion.source.aws_messaging.mapper import secondary, shared_status
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DataJobUrn
from datahub.sdk import DataFlow, DataJob
from datahub.specific.datajob import DataJobPatchBuilder

# Job platforms whose edges to queues and topics only these sources write. The
# kinesis Firehose job is left out: the kinesis source owns its inputs.
TRACKED_JOB_PLATFORMS: FrozenSet[str] = frozenset(
    {LAMBDA_PLATFORM, EVENTBRIDGE_PLATFORM, EVENTBRIDGE_PIPES_PLATFORM}
)


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


class JobEdgeTracker:
    """Removes job edges to this source's queues or topics that an earlier run
    wrote and AWS no longer has: a deleted event source mapping, rule target or
    pipe. Shared jobs are never stale-removed as entities, so without this a
    deleted function or rule would keep pointing at the queue."""

    def __init__(
        self, *, report: AwsMessagingReportBase, graph: Optional[DataHubGraph]
    ):
        self.report = report
        self.graph = graph
        self.seen: Set[Tuple[str, str, str]] = set()
        # (job platform, edge kind) whose AWS listing failed or was turned off
        # this run: those edges are unknown, not gone.
        self.unknown: Set[Tuple[str, JobEdgeKind]] = set()

    def record(
        self, job: str, *, inputs: Iterable[str] = (), outputs: Iterable[str] = ()
    ) -> None:
        self.seen.update((job, JobEdgeKind.CONSUMES, urn) for urn in inputs)
        self.seen.update((job, JobEdgeKind.PRODUCES, urn) for urn in outputs)

    def mark_unknown(self, platform: str, *kinds: JobEdgeKind) -> None:
        self.unknown.update((platform, kind) for kind in kinds)

    def stale_workunits(self, datasets: Iterable[str]) -> Iterable[MetadataWorkUnit]:
        if self.graph is None:
            return
        patches: Dict[str, DataJobPatchBuilder] = {}
        for dataset in sorted(datasets):
            try:
                for related in self.graph.get_related_entities(
                    dataset,
                    [JobEdgeKind.CONSUMES.value, JobEdgeKind.PRODUCES.value],
                    RelationshipDirection.INCOMING,
                ):
                    kind = JobEdgeKind(related.relationship_type)
                    if not self._removable(related.urn, kind, dataset):
                        continue
                    patch = patches.setdefault(
                        related.urn, DataJobPatchBuilder(related.urn)
                    )
                    if kind == JobEdgeKind.CONSUMES:
                        patch.remove_input_dataset(dataset)
                    else:
                        patch.remove_output_dataset(dataset)
                    self.report.job_edges_removed += 1
            except Exception as e:
                # Removal is best effort; leaving an edge in place is safe.
                self.report.warning(
                    title="Stale lineage not checked",
                    message=(
                        "Could not read existing job lineage from DataHub; stale "
                        "edges to this resource are kept this run."
                    ),
                    context=dataset,
                    exc=e,
                )
        for job in sorted(patches):
            for mcp in patches[job].build():
                yield MetadataWorkUnit(
                    id=f"{job}-io-stale-patch", mcp_raw=mcp, is_primary_source=False
                )

    def _removable(self, job: str, kind: JobEdgeKind, dataset: str) -> bool:
        if not job.startswith("urn:li:dataJob:"):
            return False
        platform = DataJobUrn.from_string(job).get_data_flow_urn().orchestrator
        return (
            platform in TRACKED_JOB_PLATFORMS
            and (platform, kind) not in self.unknown
            and (job, kind, dataset) not in self.seen
        )
