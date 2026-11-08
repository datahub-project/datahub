from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    DATA_JOB_TYPE,
    FLINK_PLATFORM,
    MAX_QUERY_PROPERTY_CHARS,
    PROP_QUERY,
)
from datahub.ingestion.source.kafka.stream_processing.models import FlinkJob
from datahub.ingestion.source.kafka.stream_processing.sql import (
    column_lineage_fine_grained,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.dedup_list import deduplicate_list


def build_flink_workunits(
    jobs: List[FlinkJob],
    environment_id: str,
    kafka_cluster_id: str,
    topic_platform: str,
    platform_instance: Optional[str],
    env: str,
    report: KafkaSourceReport,
    topic_allowed: Callable[[str], bool],
    graph: Optional[DataHubGraph],
    include_column_lineage: bool,
) -> Iterable[MetadataWorkUnit]:
    """One `flink` DataFlow per environment and Kafka cluster, one DataJob per statement.

    A statement set can write to several clusters, and each cluster's recipe emits only
    its own topics; keying the flow on the cluster gives each recipe its own job for the
    statement instead of overwriting one shared job with a partial set of edges.
    """
    if not jobs:
        return
    flow_urn = make_data_flow_urn(
        orchestrator=FLINK_PLATFORM,
        flow_id=f"{environment_id}.{kafka_cluster_id}",
        cluster=env,
        platform_instance=platform_instance,
    )
    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=DataFlowInfoClass(
            name=f"Flink statements ({environment_id} / {kafka_cluster_id})",
            description="Confluent Cloud Flink SQL statements writing to Kafka cluster "
            f"{kafka_cluster_id}.",
        ),
    ).as_workunit()

    # One resolver for the run: it caches the topic schemas it fetches from DataHub.
    schema_resolver: Optional[SchemaResolver] = None
    if include_column_lineage:
        schema_resolver = (
            graph._make_schema_resolver(
                platform=topic_platform, platform_instance=platform_instance, env=env
            )
            if graph is not None
            else SchemaResolver(
                platform=topic_platform, platform_instance=platform_instance, env=env
            )
        )
    try:
        for job in jobs:
            try:
                workunits = _job_workunits(
                    job,
                    flow_urn,
                    topic_platform,
                    platform_instance,
                    env,
                    report,
                    topic_allowed,
                    schema_resolver,
                )
            except Exception as e:
                # Nothing of the job is emitted, and the failure keeps stale-entity
                # removal from deleting the job emitted by an earlier run.
                report.failure(
                    title="Flink job could not be emitted",
                    message="Unexpected error building a Flink statement's lineage; "
                    "other statements are still emitted",
                    context=f"statement={job.name}",
                    exc=e,
                )
                continue
            yield from workunits
            _count_emitted(workunits, report)
    finally:
        if schema_resolver is not None and graph is None:
            schema_resolver.close()


def _job_workunits(
    job: FlinkJob,
    flow_urn: str,
    topic_platform: str,
    platform_instance: Optional[str],
    env: str,
    report: KafkaSourceReport,
    topic_allowed: Callable[[str], bool],
    schema_resolver: Optional[SchemaResolver],
) -> List[MetadataWorkUnit]:
    topic_urns = {
        topic: make_dataset_urn_with_platform_instance(
            platform=topic_platform,
            name=topic,
            platform_instance=platform_instance,
            env=env,
        )
        for topic in deduplicate_list(job.input_topics + job.output_topics)
        if topic_allowed(topic)
    }
    inputs = [topic_urns[t] for t in job.input_topics if t in topic_urns]
    outputs = [topic_urns[t] for t in job.output_topics if t in topic_urns]
    if not outputs:
        return []

    fine_grained: List[FineGrainedLineageClass] = []
    # Skip column lineage when any of the job's topics is denied: the parser cannot
    # tell allowed from denied tables, so mixed jobs would leak edges.
    if schema_resolver is not None and len(topic_urns) == len(
        set(job.input_topics + job.output_topics)
    ):
        try:
            fine_grained = column_lineage_fine_grained(
                job, topic_urns, schema_resolver, platform_instance, report
            )
        except Exception as e:
            report.warning(
                title="Flink column-level lineage skipped",
                message="Could not derive column-level lineage from a Flink statement; "
                "its topic-level lineage is still emitted",
                context=f"statement={job.name}",
                exc=e,
            )

    job_urn = make_data_job_urn_with_flow(flow_urn, job.name)
    workunits = [
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=job.name,
                type=DATA_JOB_TYPE,
                customProperties=_job_properties(job),
            ),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=inputs,
                outputDatasets=outputs,
                fineGrainedLineages=fine_grained or None,
            ),
        ).as_workunit(),
    ]
    return workunits


def _count_emitted(
    workunits: List[MetadataWorkUnit], report: KafkaSourceReport
) -> None:
    for workunit in workunits:
        aspect = getattr(workunit.metadata, "aspect", None)
        if isinstance(aspect, DataJobInputOutputClass):
            report.flink_jobs_emitted += 1
            report.flink_topic_lineage_edges += len(aspect.inputDatasets) + len(
                aspect.outputDatasets
            )
            report.flink_column_lineage_edges += len(aspect.fineGrainedLineages or [])


def _job_properties(job: FlinkJob) -> Dict[str, str]:
    properties = dict(job.custom_properties)
    properties[PROP_QUERY] = job.query[:MAX_QUERY_PROPERTY_CHARS]
    return properties
