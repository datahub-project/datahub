import itertools
from typing import Callable, List, Optional, Tuple, Union

from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws_messaging.arn import parse_arn
from datahub.ingestion.source.aws_messaging.clients import S3NotificationClient
from datahub.ingestion.source.aws_messaging.config import LinkedPlatformDetail
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
        s3: Optional[LinkedPlatformDetail],
        report: AwsMessagingReportBase,
    ):
        s3 = s3 or LinkedPlatformDetail()
        self.client = client
        self.resolver = resolver
        self.graph = graph
        self.env = s3.env or env
        self.platform_instance = s3.platform_instance
        self.convert_urns_to_lowercase = s3.convert_urns_to_lowercase
        self.report = report
        self.complete = True

    def _urn(self, name: str) -> str:
        if self.convert_urns_to_lowercase:
            name = name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=S3_PLATFORM,
            name=name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def _matcher(
        self, notification: BucketNotification
    ) -> Tuple[str, Callable[[str], bool]]:
        """The URN prefix to search on, and the check a hit must pass.

        A key prefix ending in `/` names a folder, and the s3 source names a
        folder dataset without the trailing slash (`bucket/orders`), so that
        folder and anything under it match, but not `bucket/orders2`. Any other
        key prefix matches like S3 does, as a plain prefix. No prefix is the
        whole bucket, which keeps `landing` from matching `landing-archive`."""
        name = f"{notification.bucket}/{(notification.prefix or '').lstrip('/')}"
        if not name.endswith("/"):
            plain = self._urn(name)[: -len(f",{self.env})")]
            return plain, lambda urn: True
        folder_urn = self._urn(name.rstrip("/"))
        folder_prefix = folder_urn[: -len(f",{self.env})")]
        return (
            folder_prefix,
            lambda urn: urn == folder_urn or urn.startswith(f"{folder_prefix}/"),
        )

    def _datasets(self, notification: BucketNotification) -> List[str]:
        """Live (not soft-deleted) S3 datasets under the notification's prefix."""
        if self.graph is None:
            return []
        urn_prefix, matches = self._matcher(notification)
        try:
            hits = self.graph.get_urns_by_filter(
                entity_types=["dataset"],
                platform=S3_PLATFORM,
                platform_instance=self.platform_instance,
                env=self.env,
                extraFilters=[
                    {"field": "urn", "condition": "START_WITH", "values": [urn_prefix]}
                ],
            )
            found = sorted(
                itertools.islice(
                    (urn for urn in hits if matches(urn)),
                    MAX_S3_DATASETS_PER_NOTIFICATION + 1,
                )
            )
        except (GraphError, RequestException) as e:
            self.report.warning(
                title="S3 dataset search failed",
                message=(
                    "Could not search DataHub for the S3 datasets under the "
                    "notification's prefix; earlier S3 notification lineage is kept."
                ),
                context=f"s3://{notification.bucket}/{notification.prefix or ''}",
                exc=e,
            )
            self.complete = False
            return []
        if len(found) > MAX_S3_DATASETS_PER_NOTIFICATION:
            self.report.warning(
                title="S3 notification matches too many datasets",
                message=(
                    f"Only the first {MAX_S3_DATASETS_PER_NOTIFICATION} S3 datasets "
                    "under the notification's prefix are linked, and no S3 "
                    "notification lineage is removed this run."
                ),
                context=f"s3://{notification.bucket}/{notification.prefix or ''}",
            )
            # Search order isn't stable, so edges to datasets past the cap
            # can't be told apart from gone ones.
            self.complete = False
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
                    # Index lag or a platform_map mismatch also look like this,
                    # so earlier edges into the target are kept.
                    self.complete = False
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
