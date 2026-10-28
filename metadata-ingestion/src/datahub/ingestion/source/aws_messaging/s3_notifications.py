from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws_messaging.arn import parse_arn
from datahub.ingestion.source.aws_messaging.clients import S3NotificationClient
from datahub.ingestion.source.aws_messaging.constants import (
    S3_PLATFORM,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.models import BucketNotification
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver


class S3NotificationLineage:
    """S3 buckets whose event notifications go to a queue or topic.

    S3 datasets are named by bucket and path, and a notification filter only
    gives a key prefix, so the edge is added only when `bucket/prefix` or the
    bare bucket already exists in DataHub."""

    def __init__(
        self,
        *,
        client: S3NotificationClient,
        resolver: ResourceResolver,
        graph: Optional[DataHubGraph],
        env: str,
        s3_platform_instance: Optional[str],
        s3_convert_urns_to_lowercase: bool = False,
        report: AwsMessagingReportBase,
    ):
        self.client = client
        self.resolver = resolver
        self.graph = graph
        self.env = env
        self.s3_platform_instance = s3_platform_instance
        self.s3_convert_urns_to_lowercase = s3_convert_urns_to_lowercase
        self.report = report
        self._exists: Dict[str, bool] = {}

    def _candidates(self, notification: BucketNotification) -> List[str]:
        names = [notification.bucket]
        prefix = (notification.prefix or "").strip("/")
        if prefix:
            names.insert(0, f"{notification.bucket}/{prefix}")
        if self.s3_convert_urns_to_lowercase:
            names = [name.lower() for name in names]
        return [
            make_dataset_urn_with_platform_instance(
                platform=S3_PLATFORM,
                name=name,
                platform_instance=self.s3_platform_instance,
                env=self.env,
            )
            for name in names
        ]

    def _existing(self, urn: str) -> bool:
        if self.graph is None:
            return False
        if urn not in self._exists:
            self._exists[urn] = self.graph.exists(urn)
        return self._exists[urn]

    def collect(self, lineage: DatasetLineageCollector) -> None:
        for bucket in self.client.list_bucket_names(self.resolver.region):
            for notification in self.client.get_notifications(bucket):
                target_urn = self.resolver.urn(parse_arn(notification.target_arn))
                if target_urn is None:
                    continue
                upstream = next(
                    (
                        urn
                        for urn in self._candidates(notification)
                        if self._existing(urn)
                    ),
                    None,
                )
                if upstream is None:
                    self.report.report_edge_skipped(
                        f"s3://{bucket}",
                        target_urn,
                        "no matching S3 dataset in DataHub"
                        if self.graph
                        else "no DataHub graph connection to look up the S3 dataset",
                    )
                    continue
                lineage.add(
                    upstream=upstream,
                    downstream=target_urn,
                    relationship=LineageRelationship.S3_NOTIFICATION,
                )
