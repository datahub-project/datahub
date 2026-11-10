from typing import Callable, List, Optional, Set, Tuple, Union

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
    S3_SOURCE_COMPRESSIONS,
    S3_SOURCE_FILE_TYPES,
    LineageRelationship,
)
from datahub.ingestion.source.aws_messaging.lineage import DatasetLineageCollector
from datahub.ingestion.source.aws_messaging.models import BucketNotification
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.metadata.urns import DatasetUrn


def _is_file_name(name: str) -> bool:
    """Whether an s3 dataset name's last segment is a file, e.g. `a.csv` or
    `a.json.gz`, rather than a folder."""
    parts = name.lower().split(".")
    if len(parts) > 2 and parts[-1] in S3_SOURCE_COMPRESSIONS:
        parts = parts[:-1]
    return len(parts) > 1 and parts[-1] in S3_SOURCE_FILE_TYPES


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
        target_allowed: Callable[[str], bool],
    ):
        s3 = s3 or LinkedPlatformDetail()
        self.client = client
        self.resolver = resolver
        self.graph = graph
        self.env = s3.env or env
        self.platform_instance = s3.platform_instance
        self.convert_urns_to_lowercase = s3.convert_urns_to_lowercase
        self.report = report
        self.target_allowed = target_allowed
        self.complete = True
        # Buckets whose notifications were read, and queues or topics whose S3
        # upstreams this run couldn't work out.
        self._read_buckets: List[str] = []
        self._unknown_targets: Set[str] = set()

    def _urn(self, name: str) -> str:
        if self.convert_urns_to_lowercase:
            name = name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=S3_PLATFORM,
            name=name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def can_retract(self, downstream: str, upstream: str) -> bool:
        """Whether a missing S3 edge is really gone: its bucket's notifications
        were read (a bucket in another account can notify these queues and topics,
        but can't be read from here), and the target's S3 upstreams were all
        worked out this run."""
        return downstream not in self._unknown_targets and self._from_read_bucket(
            upstream
        )

    def _from_read_bucket(self, urn: str) -> bool:
        for bucket in self._read_buckets:
            bucket_urn = self._urn(bucket)
            if urn == bucket_urn or urn.startswith(
                f"{bucket_urn[: -len(f',{self.env})')]}/"
            ):
                return True
        return False

    def _matcher(
        self, notification: BucketNotification
    ) -> Tuple[str, Callable[[str], bool]]:
        """The URN prefix to search on, and the check a hit must pass.

        A key prefix ending in `/` names a folder, and the s3 source names a
        folder dataset without the trailing slash (`bucket/orders`), so that
        folder and anything under it match, but not `bucket/orders2`. Any other
        key prefix matches like S3 does, as a plain prefix, so `landing` also
        matches `landing-archive`. No prefix is the whole bucket, so bucket
        `landing` doesn't match bucket `landing-archive`.

        A suffix filter can only be checked against a file dataset (one named
        after a file the s3 source reads, such as `bucket/orders/2024.csv`); a
        folder dataset, dotted ones like `orders.v2` included, is kept, since
        the files in it aren't known here."""
        name = f"{notification.bucket}/{notification.prefix or ''}"
        suffix = notification.suffix
        if suffix and self.convert_urns_to_lowercase:
            suffix = suffix.lower()

        def suffix_matches(urn: str) -> bool:
            if not suffix:
                return True
            leaf = DatasetUrn.from_string(urn).name.rsplit("/", 1)[-1]
            return not _is_file_name(leaf) or leaf.endswith(suffix)

        if not name.endswith("/"):
            plain = self._urn(name)[: -len(f",{self.env})")]
            return plain, suffix_matches
        folder_urn = self._urn(name.rstrip("/"))
        folder_prefix = folder_urn[: -len(f",{self.env})")]
        return (
            folder_prefix,
            lambda urn: (
                (urn == folder_urn or urn.startswith(f"{folder_prefix}/"))
                and suffix_matches(urn)
            ),
        )

    def _datasets(self, notification: BucketNotification) -> Optional[List[str]]:
        """Live (not soft-deleted) S3 datasets under the notification's prefix, or
        None when DataHub couldn't be searched."""
        if self.graph is None:
            return None
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
            # Search order isn't stable, so every match is read and the cap
            # applied by name; otherwise each run would link a different subset.
            found = sorted(urn for urn in hits if matches(urn))
        except (GraphError, RequestException) as e:
            self.report.warning(
                title="S3 dataset search failed",
                message=(
                    "Could not search DataHub for the S3 datasets under the "
                    "notification's prefix; earlier S3 notification lineage into its "
                    "target is kept."
                ),
                context=f"s3://{notification.bucket}/{notification.prefix or ''}",
                exc=e,
            )
            return None
        if len(found) > MAX_S3_DATASETS_PER_NOTIFICATION:
            self.report.warning(
                title="S3 notification matches too many datasets",
                message=(
                    f"Only the first {MAX_S3_DATASETS_PER_NOTIFICATION} S3 datasets "
                    "by name under the notification's prefix are linked."
                ),
                context=f"s3://{notification.bucket}/{notification.prefix or ''}",
            )
            found = found[:MAX_S3_DATASETS_PER_NOTIFICATION]
        return found

    def _listing_failed(self, _error: Union[ClientError, BotoCoreError]) -> None:
        self.complete = False

    def collect(self, lineage: DatasetLineageCollector) -> bool:
        """Adds the edges; True when the buckets could be listed, so
        `can_retract` decides per edge."""
        for bucket in self.client.list_bucket_names(
            self.resolver.region, on_error=self._listing_failed
        ):
            notifications = self.client.get_notifications(bucket)
            if notifications is None:
                # Not read, so edges from this bucket are kept.
                continue
            self._read_buckets.append(bucket)
            for notification in notifications:
                target = parse_arn(notification.target_arn)
                target_urn = self.resolver.urn(target)
                if target_urn is None:
                    if (
                        target is not None
                        and target.service == self.resolver.service
                        and self.resolver.is_local(target)
                        and self.target_allowed(target.resource_name)
                    ):
                        # One of this source's own queues or topics that this run
                        # didn't read, so its earlier S3 edges are kept. One the
                        # pattern filters out is never this source's.
                        self._unknown_targets.add(self.resolver.urn_of(target))
                    continue
                upstreams = self._datasets(notification)
                if not upstreams:
                    # A failed search, index lag or a platform_map mismatch, so
                    # earlier S3 edges into this target are kept.
                    self._unknown_targets.add(target_urn)
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
