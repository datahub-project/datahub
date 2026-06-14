import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.graph.filters import RawSearchFilterRule

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataLakeUrnLookup:
    matched_urns: Tuple[str, ...] = ()
    transient_error: Optional[Exception] = None

    def __post_init__(self) -> None:
        if self.transient_error is not None and self.matched_urns:
            raise ValueError(
                "DataLakeUrnLookup is either a transient-failure result or a "
                "(possibly empty) match — never both."
            )


@dataclass
class DataLakePathResolver:
    """Resolve a data-lake storage path to existing dataset URNs rooted at it.

    Bulk-fetches every dataset URN under a bucket once per
    ``(platform, platform_instance, bucket)`` and matches client-side, so N paths
    under a bucket cost a single graph call rather than one wildcard query each.
    Transient failures are surfaced via ``DataLakeUrnLookup.transient_error`` so
    callers can log and count without this module owning a report type.
    """

    graph: "DataHubGraph"
    env: str
    _bucket_index: Dict[Tuple[str, Optional[str], str], Tuple[str, ...]] = field(
        default_factory=dict
    )

    def resolve_datasets_under_path(
        self,
        *,
        platform: str,
        bucket: str,
        path: str,
        platform_instance: Optional[str] = None,
    ) -> DataLakeUrnLookup:
        """Return existing dataset URNs whose path equals or sits under ``path``."""
        key = (platform, platform_instance, bucket)
        bucket_urns = self._bucket_index.get(key)
        if bucket_urns is None:
            try:
                bucket_urns = self._bulk_fetch_bucket(
                    platform=platform,
                    platform_instance=platform_instance,
                    bucket=bucket,
                )
            except Exception as e:
                logger.warning(
                    f"Transient failure fetching {platform} dataset URNs for bucket "
                    f"{bucket!r}; lookup will not be cached.",
                    exc_info=True,
                )
                return DataLakeUrnLookup(transient_error=e)
            self._bucket_index[key] = bucket_urns

        path_prefix = self._urn_prefix(platform, platform_instance, path)
        matches = tuple(
            u for u in bucket_urns if dataset_path_is_rooted_at(u, path_prefix)
        )
        return DataLakeUrnLookup(matched_urns=matches)

    def _bulk_fetch_bucket(
        self, *, platform: str, platform_instance: Optional[str], bucket: str
    ) -> Tuple[str, ...]:
        bucket_prefix = self._urn_prefix(platform, platform_instance, bucket)
        extra_filters: List["RawSearchFilterRule"] = [
            {"field": "urn", "condition": "START_WITH", "values": [bucket_prefix]}
        ]
        candidate_urns = self.graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=platform,
            platform_instance=platform_instance,
            env=self.env,
            extraFilters=extra_filters,
        )
        # The START_WITH wildcard is case-insensitive and prefix-only, so it can
        # over-match sibling buckets (e.g. `bucket` vs `bucket-other`). Re-check the
        # bucket boundary case-sensitively before caching.
        return tuple(
            urn
            for urn in candidate_urns
            if dataset_path_is_rooted_at(urn, bucket_prefix)
        )

    @staticmethod
    def _urn_prefix(platform: str, platform_instance: Optional[str], path: str) -> str:
        name = f"{platform_instance}.{path}" if platform_instance else path
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name}"


def dataset_path_is_rooted_at(dataset_urn: str, urn_prefix: str) -> bool:
    """Whether ``dataset_urn``'s path equals or sits strictly under ``urn_prefix``.

    Rejects false positives where a plain prefix match would treat a sibling path as
    a child (e.g. ``foo`` matching ``foobar``): the character immediately after the
    prefix must be ``/`` (a child path) or ``,`` (the URN's env separator, i.e. an
    exact match).
    """
    if not dataset_urn.startswith(urn_prefix):
        return False
    return dataset_urn[len(urn_prefix) : len(urn_prefix) + 1] in ("/", ",")
