import logging
from typing import Callable, Dict, List, Optional, Set

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
    ENGINE_FLOW_METADATA,
    MAX_QUERY_PROPERTY_CHARS,
    PROP_ENGINE,
    PROP_QUERY,
    StreamProcessingEngine,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob
from datahub.ingestion.source.kafka.stream_processing.sql import (
    column_lineage_fine_grained,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
)

logger = logging.getLogger(__name__)


def build_stream_processing_workunits(
    jobs: List[StreamProcessingJob],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    report: KafkaSourceReport,
    topic_allowed: Callable[[str], bool],
    graph: Optional[DataHubGraph],
    include_column_lineage: bool,
) -> List[MetadataWorkUnit]:
    workunits: List[MetadataWorkUnit] = []
    emitted_flows: Set[StreamProcessingEngine] = set()

    for job in jobs:
        input_datasets = _topic_urns(
            job.input_topics, platform, platform_instance, env, topic_allowed
        )
        output_datasets = _topic_urns(
            job.output_topics, platform, platform_instance, env, topic_allowed
        )
        if not input_datasets and not output_datasets:
            continue

        flow_urn = make_data_flow_urn(
            orchestrator=platform,
            flow_id=ENGINE_FLOW_METADATA[job.engine][0],
            cluster=env,
            platform_instance=platform_instance,
        )
        if job.engine not in emitted_flows:
            _, flow_name, flow_description = ENGINE_FLOW_METADATA[job.engine]
            workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=flow_urn,
                    aspect=DataFlowInfoClass(
                        name=flow_name, description=flow_description
                    ),
                ).as_workunit()
            )
            emitted_flows.add(job.engine)

        job_urn = make_data_job_urn_with_flow(flow_urn, job.job_id)
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=job.name,
                    type=DATA_JOB_TYPE,
                    customProperties=_job_properties(job),
                ),
            ).as_workunit()
        )

        fine_grained: List[FineGrainedLineageClass] = []
        # Skip CLL when any of the job's topics are denied — the parser cannot
        # tell allowed from denied identifiers, so mixed jobs would leak edges.
        job_topics_allowed = all(
            topic_allowed(topic) for topic in job.input_topics + job.output_topics
        )
        if include_column_lineage and job.parse_query and job_topics_allowed:
            fine_grained = column_lineage_fine_grained(
                query=job.parse_query,
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                graph=graph,
                dialect=job.sql_dialect,
            )
            report.stream_processing_column_lineage_edges += len(fine_grained)

        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                    fineGrainedLineages=fine_grained or None,
                ),
            ).as_workunit()
        )
        report.stream_processing_jobs_with_lineage += 1
        report.stream_processing_lineage_edges += len(input_datasets) + len(
            output_datasets
        )

    return workunits


def _topic_urns(
    topics: List[str],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    topic_allowed: Callable[[str], bool],
) -> List[str]:
    urns: List[str] = []
    seen: Set[str] = set()
    for topic in topics:
        if not topic_allowed(topic) or topic in seen:
            continue
        seen.add(topic)
        urns.append(
            make_dataset_urn_with_platform_instance(
                platform=platform,
                name=topic,
                platform_instance=platform_instance,
                env=env,
            )
        )
    return urns


def _job_properties(job: StreamProcessingJob) -> Dict[str, str]:
    properties: Dict[str, str] = dict(job.custom_properties)
    properties[PROP_ENGINE] = job.engine.value
    if job.query:
        properties[PROP_QUERY] = job.query[:MAX_QUERY_PROPERTY_CHARS]
    return properties
