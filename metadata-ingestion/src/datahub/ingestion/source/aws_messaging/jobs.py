from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from requests.exceptions import RequestException

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import make_data_job_urn, make_data_platform_urn
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
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    EdgeClass,
    FineGrainedLineageClass,
)
from datahub.metadata.urns import DataJobUrn, DatasetUrn, SchemaFieldUrn
from datahub.sdk import DataFlow, DataJob
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.utilities.urns.error import InvalidUrnError

# Job platforms whose edges to queues and topics only these sources write.
TRACKED_JOB_PLATFORMS: FrozenSet[str] = frozenset(
    {LAMBDA_PLATFORM, EVENTBRIDGE_PLATFORM, EVENTBRIDGE_PIPES_PLATFORM}
)


def _is_entity(urn: str, entity_type: str) -> bool:
    # A prefix check, not guess_entity_type: other writers' URNs may be malformed.
    return urn.startswith(f"urn:li:{entity_type}:")


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
        self,
        *,
        report: AwsMessagingReportBase,
        graph: Optional[DataHubGraph],
        instance: Optional[str],
        region: str,
        platform: str,
    ):
        self.report = report
        self.graph = graph
        # The dataset platform this source writes job edges for (sqs or sns).
        self.platform = platform
        # Every job this recipe emits is in its own account and region, so its flow
        # id starts `<instance>.<region>.`. Another account's recipe may write a
        # job edge to one of these queues that this run can never see.
        self.own_flow_prefix = f"{instance}.{region}." if instance else f"{region}."
        self.seen: Set[Tuple[str, str, str]] = set()
        # (job platform, edge kind) whose AWS listing failed or was turned off
        # this run: those edges are unknown, not gone.
        self.unknown: Set[Tuple[str, JobEdgeKind]] = set()
        # Pipe job -> (downstream field, upstream field) emitted this run, for pipes
        # whose field lineage this run could work out. A pipe missing here keeps
        # its field edges while its output edge stays.
        self.fine_grained: Dict[str, Set[Tuple[str, str]]] = {}

    def record(
        self, job: str, *, inputs: Iterable[str] = (), outputs: Iterable[str] = ()
    ) -> None:
        self.seen.update((job, JobEdgeKind.CONSUMES, urn) for urn in inputs)
        self.seen.update((job, JobEdgeKind.PRODUCES, urn) for urn in outputs)

    def record_fine_grained(
        self, job: str, lineages: List[FineGrainedLineageClass]
    ) -> None:
        self.fine_grained.setdefault(job, set()).update(
            (downstream, upstream)
            for lineage in lineages
            for downstream in lineage.downstreams or []
            for upstream in lineage.upstreams or []
        )

    def mark_unknown(self, platform: str, *kinds: JobEdgeKind) -> None:
        self.unknown.update((platform, kind) for kind in kinds)

    def stale_workunits(self, datasets: Iterable[str]) -> Iterable[MetadataWorkUnit]:
        # Same guard as StaleEntityRemovalHandler: a failed run didn't see every
        # edge, so none can be called gone.
        if self.graph is None or self.report.failures:
            return
        patches: Dict[str, DataJobPatchBuilder] = {}
        for dataset in sorted(datasets):
            try:
                for related in self.graph.get_related_entities(
                    dataset,
                    [JobEdgeKind.CONSUMES.value, JobEdgeKind.PRODUCES.value],
                    RelationshipDirection.INCOMING,
                ):
                    job = related.urn
                    kind = JobEdgeKind(related.relationship_type)
                    platform = self._owned_platform(job, kind)
                    if platform is None:
                        continue
                    edge_gone = (job, kind, dataset) not in self.seen
                    stale_fields = (
                        self._stale_pipe_fields(job, dataset, edge_gone)
                        if platform == EVENTBRIDGE_PIPES_PLATFORM
                        and kind == JobEdgeKind.PRODUCES
                        else []
                    )
                    self._remove(patches, job, kind, dataset, edge_gone, stale_fields)
            except (GraphError, RequestException) as e:
                self._not_checked(dataset, e)
        self._stale_remote_edges(self.graph, patches)
        for job in sorted(patches):
            for mcp in patches[job].build():
                yield MetadataWorkUnit(
                    id=f"{job}-io-stale-patch", mcp_raw=mcp, is_primary_source=False
                )

    def _stale_remote_edges(
        self, graph: DataHubGraph, patches: Dict[str, DataJobPatchBuilder]
    ) -> None:
        """Edges from this run's jobs to another account's or region's queues or
        topics. No recipe reads those datasets in full, so the dataset pass above
        never reaches them; the job's own edge list does."""
        for job in sorted({job for job, _, _ in self.seen}):
            try:
                io = graph.get_aspect(job, DataJobInputOutputClass)
            except (GraphError, RequestException) as e:
                self._not_checked(job, e)
                continue
            if io is None:
                continue
            for kind, urns in (
                (
                    JobEdgeKind.CONSUMES,
                    _edge_urns(io.inputDatasets, io.inputDatasetEdges),
                ),
                (
                    JobEdgeKind.PRODUCES,
                    _edge_urns(io.outputDatasets, io.outputDatasetEdges),
                ),
            ):
                platform = self._owned_platform(job, kind)
                if platform is None:
                    continue
                for dataset in sorted(urns):
                    if not self._remote(dataset) or (job, kind, dataset) in self.seen:
                        continue
                    stale_fields = (
                        self._stale_pipe_fields(job, dataset, True)
                        if platform == EVENTBRIDGE_PIPES_PLATFORM
                        and kind == JobEdgeKind.PRODUCES
                        else []
                    )
                    self._remove(patches, job, kind, dataset, True, stale_fields)

    def _remote(self, dataset: str) -> bool:
        """A dataset of this source's platform outside the recipe's own account
        and region."""
        try:
            urn = DatasetUrn.from_string(dataset)
        except InvalidUrnError:
            return False
        return urn.platform == make_data_platform_urn(
            self.platform
        ) and not urn.name.startswith(self.own_flow_prefix)

    def _remove(
        self,
        patches: Dict[str, DataJobPatchBuilder],
        job: str,
        kind: JobEdgeKind,
        dataset: str,
        edge_gone: bool,
        stale_fields: List[FineGrainedLineageClass],
    ) -> None:
        if not edge_gone and not stale_fields:
            return
        patch = patches.setdefault(job, DataJobPatchBuilder(job))
        if edge_gone:
            if kind == JobEdgeKind.CONSUMES:
                patch.remove_input_dataset(dataset)
            else:
                patch.remove_output_dataset(dataset)
            self.report.job_edges_removed += 1
        for lineage in stale_fields:
            patch.remove_fine_grained_lineage(lineage)
            self.report.column_lineage_edges_removed += 1

    def _not_checked(self, context: str, exc: Exception) -> None:
        # Removal is best effort; leaving an edge in place is safe.
        self.report.warning(
            title="Stale lineage not checked",
            message=(
                "Could not read existing job lineage from DataHub; stale "
                "edges to this resource are kept this run."
            ),
            context=context,
            exc=exc,
        )

    def _owned_platform(self, job: str, kind: JobEdgeKind) -> Optional[str]:
        """The job's platform, if this run is the one that writes and reads in
        full that job's edges of this kind; None otherwise."""
        if not _is_entity(job, DataJobUrn.ENTITY_TYPE):
            return None
        flow = DataJobUrn.from_string(job).get_data_flow_urn()
        if (
            flow.orchestrator not in TRACKED_JOB_PLATFORMS
            or (flow.orchestrator, kind) in self.unknown
            or not flow.flow_id.startswith(self.own_flow_prefix)
        ):
            return None
        return flow.orchestrator

    def _stale_pipe_fields(
        self, job: str, dataset: str, edge_gone: bool
    ) -> List[FineGrainedLineageClass]:
        """Field edges into `dataset` on a pipe job that this run didn't emit: all
        of them once the pipe no longer writes to it, else those its current
        input template no longer maps."""
        emitted = set() if edge_gone else self.fine_grained.get(job)
        if emitted is None or self.graph is None:
            return []
        try:
            io = self.graph.get_aspect(job, DataJobInputOutputClass)
        except (GraphError, RequestException) as e:
            self._not_checked(job, e)
            return []
        stale: List[FineGrainedLineageClass] = []
        for existing in (io.fineGrainedLineages if io else None) or []:
            for downstream in existing.downstreams or []:
                if not _is_entity(downstream, SchemaFieldUrn.ENTITY_TYPE):
                    continue
                try:
                    parent = SchemaFieldUrn.from_string(downstream).parent
                except InvalidUrnError as e:
                    # Another writer's malformed field edge; leave it in place.
                    self.report.warning(
                        title="Stale field lineage not checked",
                        message="An existing field lineage edge has an unparseable downstream urn; it is kept.",
                        context=f"{job}: {downstream}",
                        exc=e,
                    )
                    continue
                if parent != dataset:
                    continue
                for upstream in existing.upstreams or []:
                    if (downstream, upstream) in emitted:
                        continue
                    stale.append(
                        FineGrainedLineageClass(
                            upstreamType=existing.upstreamType,
                            upstreams=[upstream],
                            downstreamType=existing.downstreamType,
                            downstreams=[downstream],
                            transformOperation=existing.transformOperation,
                            query=existing.query,
                        )
                    )
        return stale


def _edge_urns(urns: Optional[List[str]], edges: Optional[List[EdgeClass]]) -> Set[str]:
    return set(urns or []) | {edge.destinationUrn for edge in edges or []}
