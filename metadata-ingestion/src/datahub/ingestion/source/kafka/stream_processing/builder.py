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
    topic_platform: str,
    platform_instance: Optional[str],
    env: str,
    report: KafkaSourceReport,
    topic_allowed: Callable[[str], bool],
    graph: Optional[DataHubGraph],
    include_column_lineage: bool,
) -> Iterable[MetadataWorkUnit]:
    """One `flink` DataFlow per Confluent environment and one DataJob per statement.

    The platform instance and env of this Kafka recipe scope both, so recipes for
    different clusters of one environment do not overwrite each other's jobs.
    """
    if not jobs:
        return
    flow_urn = make_data_flow_urn(
        orchestrator=FLINK_PLATFORM,
        flow_id=environment_id,
        cluster=env,
        platform_instance=platform_instance,
    )
    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=DataFlowInfoClass(
            name=f"Flink statements ({environment_id})",
            description="Confluent Cloud Flink SQL statements writing to this Kafka "
            "cluster.",
        ),
    ).as_workunit()

    # One resolver for the run: it caches the topic schemas it fetches from DataHub.
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
                continue

            job_urn = make_data_job_urn_with_flow(flow_urn, job.name)
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=job.name,
                    type=DATA_JOB_TYPE,
                    customProperties=_job_properties(job),
                ),
            ).as_workunit()

            fine_grained: List[FineGrainedLineageClass] = []
            # Skip column lineage when any of the job's topics is denied: the parser
            # cannot tell allowed from denied tables, so mixed jobs would leak edges.
            if include_column_lineage and len(topic_urns) == len(
                set(job.input_topics + job.output_topics)
            ):
                fine_grained = column_lineage_fine_grained(
                    job, topic_urns, schema_resolver, platform_instance, report
                )
                report.flink_column_lineage_edges += len(fine_grained)

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=inputs,
                    outputDatasets=outputs,
                    fineGrainedLineages=fine_grained or None,
                ),
            ).as_workunit()
            report.flink_jobs_emitted += 1
            report.flink_topic_lineage_edges += len(inputs) + len(outputs)
    finally:
        if graph is None:
            schema_resolver.close()


def _job_properties(job: FlinkJob) -> Dict[str, str]:
    properties = dict(job.custom_properties)
    properties[PROP_QUERY] = job.query[:MAX_QUERY_PROPERTY_CHARS]
    return properties
