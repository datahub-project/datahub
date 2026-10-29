import itertools
from typing import List, Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws_messaging.arn import parse_arn
from datahub.ingestion.source.aws_messaging.clients import S3NotificationClient
from datahub.ingestion.source.aws_messaging.constants import (
    MAX_S3_DATASETS_PER_NOTIFICATION,
    S3_PLATFORM,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.models import BucketNotification
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver


class S3NotificationLineage:
    """S3 buckets whose event notifications go to a queue or topic.

    A notification filter only gives a key prefix, while the s3 source names
    datasets after a path_spec's table folder (`bucket/incoming/orders` for a
    notification on `incoming/`). So every S3 dataset already in DataHub under
    `bucket/prefix` becomes an upstream; none are created."""

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
        self.complete = True

    def _urn_prefix(self, notification: BucketNotification) -> str:
        # The trailing slash after the bucket keeps `landing` from matching
        # `landing-archive`; a key prefix itself matches like S3 does, as a prefix.
        name = f"{notification.bucket}/{(notification.prefix or '').lstrip('/')}"
        if self.s3_convert_urns_to_lowercase:
            name = name.lower()
        urn = make_dataset_urn_with_platform_instance(
            platform=S3_PLATFORM,
            name=name,
            platform_instance=self.s3_platform_instance,
            env=self.env,
        )
        return urn[: -len(f",{self.env})")]

    def _datasets(self, notification: BucketNotification) -> List[str]:
        """Live (not soft-deleted) S3 datasets under the notification's prefix."""
        if self.graph is None:
            return []
        matches = self.graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=S3_PLATFORM,
            platform_instance=self.s3_platform_instance,
            env=self.env,
            extraFilters=[
                {
                    "field": "urn",
                    "condition": "START_WITH",
                    "values": [self._urn_prefix(notification)],
                }
            ],
        )
        found = sorted(itertools.islice(matches, MAX_S3_DATASETS_PER_NOTIFICATION + 1))
        if len(found) > MAX_S3_DATASETS_PER_NOTIFICATION:
            self.report.warning(
                title="S3 notification matches too many datasets",
                message=(
                    f"Only the first {MAX_S3_DATASETS_PER_NOTIFICATION} S3 datasets "
                    "under the notification's prefix are linked."
                ),
                context=f"s3://{notification.bucket}/{notification.prefix or ''}",
            )
            found = found[:MAX_S3_DATASETS_PER_NOTIFICATION]
        return found

    def _listing_failed(self, _error: Union[ClientError, BotoCoreError]) -> None:
        self.complete = False

    def collect(self, lineage: DatasetLineageCollector) -> bool:
        """Adds the edges; True when every bucket's notifications were read, so
        edges missing this run are really gone."""
        for bucket in self.client.list_bucket_names(
            self.resolver.region, on_error=self._listing_failed
        ):
            notifications = self.client.get_notifications(bucket)
            if notifications is None:
                self.complete = False
                continue
            for notification in notifications:
                target_urn = self.resolver.urn(parse_arn(notification.target_arn))
                if target_urn is None:
                    continue
                upstreams = self._datasets(notification)
                if not upstreams:
                    self.report.report_edge_skipped(
                        f"s3://{bucket}",
                        target_urn,
                        "no matching S3 dataset in DataHub"
                        if self.graph
                        else "no DataHub graph connection to look up the S3 dataset",
                    )
                    continue
                for upstream in upstreams:
                    lineage.add(
                        upstream=upstream,
                        downstream=target_urn,
                        relationship=LineageRelationship.S3_NOTIFICATION,
                    )
        return self.complete
