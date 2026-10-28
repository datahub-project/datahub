from typing import Dict, Iterable, Optional, Set

from datahub.emitter.mce_builder import make_data_job_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder
from datahub.ingestion.source.aws_messaging.constants import (
    FIREHOSE_DELIVERY_JOB_ID,
    FIREHOSE_PLATFORM,
)
from datahub.ingestion.source.aws_messaging.models import Arn
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.specific.datajob import DataJobPatchBuilder


class FirehoseLineage:
    """SNS topics feeding Firehose delivery streams.

    The delivery job belongs to the kinesis source, so the topic is patched onto
    its inputs, and only when the job is already in DataHub: otherwise the patch
    would create a job nobody else describes."""

    def __init__(
        self,
        *,
        env: str,
        urns: UrnBuilder,
        kinesis_platform_instance: Optional[str],
        graph: Optional[DataHubGraph],
        report: AwsMessagingReportBase,
    ):
        self.env = env
        self.urns = urns
        self.kinesis_platform_instance = kinesis_platform_instance
        self.graph = graph
        self.report = report
        self._inputs: Dict[str, Set[str]] = {}

    def job_urn(self, stream: Arn) -> str:
        # Mirrors the kinesis source: flow id `<region>.<stream>`, account id as
        # the default platform instance.
        return make_data_job_urn(
            orchestrator=FIREHOSE_PLATFORM,
            flow_id=f"{stream.region}.{stream.resource_name}",
            job_id=FIREHOSE_DELIVERY_JOB_ID,
            cluster=self.env,
            platform_instance=self.kinesis_platform_instance or stream.account or None,
        )

    def add(self, stream: Arn, topic_urn: str) -> None:
        self._inputs.setdefault(self.job_urn(stream), set()).add(topic_urn)

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        for job_urn in sorted(self._inputs):
            if self.graph is None or not self.graph.exists(job_urn):
                for topic_urn in sorted(self._inputs[job_urn]):
                    self.report.report_edge_skipped(
                        topic_urn,
                        job_urn,
                        "Firehose delivery job not found in DataHub"
                        if self.graph
                        else "no DataHub graph connection to check the Firehose job",
                    )
                continue
            patch = DataJobPatchBuilder(job_urn)
            for topic_urn in sorted(self._inputs[job_urn]):
                patch.add_input_dataset(topic_urn)
                self.report.lineage_edges_emitted += 1
            for mcp in patch.build():
                yield MetadataWorkUnit(
                    id=f"{job_urn}-inputs-patch", mcp_raw=mcp, is_primary_source=False
                )
