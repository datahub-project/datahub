import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from google.api_core.exceptions import GoogleAPIError, PermissionDenied
from google.cloud import bigquery, bigquery_analyticshub_v1, resourcemanager_v3

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.common.gcp_errors import (
    is_iam_permission_denied,
    is_service_disabled,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublisherRef:
    """Publisher-side identity for a linked dataset.

    `project_id` is `None` when the publisher project number could not be
    resolved to an ID (e.g. `resourcemanager.projects.get` denied).
    """

    project_number: str
    dataset: str
    project_id: Optional[str]


@dataclass(frozen=True)
class LinkedDatasetInfo:
    """Resolved BigQuery Sharing metadata for a single linked dataset.

    `publisher` is `None` when the dataset exposed no source; its `project_id`
    is `None` when the source was found but could not be resolved. `has_publisher`
    is the single gate for whether lineage can be emitted.
    """

    consumer_project_id: str
    consumer_dataset: str

    publisher: Optional[PublisherRef] = None

    subscription_state: Optional[bigquery_analyticshub_v1.Subscription.State] = None
    link_state: Optional[str] = None  # Dataset.linkedDatasetMetadata.linkState
    listing: Optional[str] = None
    data_exchange: Optional[str] = None
    publisher_organization: Optional[str] = None
    creation_time: Optional[str] = None
    last_modify_time: Optional[str] = None

    @property
    def has_publisher(self) -> bool:
        return self.publisher is not None and self.publisher.project_id is not None

    def to_extra_properties(self) -> Dict[str, str]:
        """Render as `linked_dataset.*` / `analytics_hub.*` custom properties.

        Empty values are dropped; `linked_dataset.link_type` is always present.
        """
        props: Dict[str, str] = {}

        publisher = self.publisher
        if publisher is not None and publisher.project_id is not None:
            props["linked_dataset.source"] = (
                f"{publisher.project_id}.{publisher.dataset}"
            )
        props["linked_dataset.link_type"] = "LINKED"

        if self.link_state:
            props["linked_dataset.link_state"] = self.link_state
        listing_or_exchange = self.listing or self.data_exchange
        if listing_or_exchange:
            props["analytics_hub.listing"] = listing_or_exchange
        if self.subscription_state is not None:
            props["analytics_hub.subscription_state"] = self.subscription_state.name
        if self.publisher_organization:
            props["analytics_hub.publisher_organization"] = self.publisher_organization
        if self.creation_time:
            props["analytics_hub.link_creation_time"] = self.creation_time
        if self.last_modify_time:
            props["analytics_hub.last_modify_time"] = self.last_modify_time

        return props


def _last_segment(resource_name: Optional[str]) -> Optional[str]:
    """Return the last `/`-segment of a resource name, or None for empty input."""
    if not resource_name:
        return None
    return resource_name.rsplit("/", 1)[-1] or None


class BigQueryLinkedDatasetsHandler:
    """Detect and enrich BigQuery Sharing linked datasets."""

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        identifiers: BigQueryIdentifierBuilder,
        filters: BigQueryFilter,
    ) -> None:
        self.config = config
        self.report = report
        self.identifiers = identifiers
        self.filters = filters

        self._lookup: Dict[Tuple[str, str], LinkedDatasetInfo] = {}

        # Caching None avoids retrying a failed publisher resolution within a run.
        self._publisher_project_id_cache: Dict[str, Optional[str]] = {}

        self._ah_client: Optional[
            bigquery_analyticshub_v1.AnalyticsHubServiceClient
        ] = None
        self._rm_client: Optional[resourcemanager_v3.ProjectsClient] = None
        self._bq_client: Optional[bigquery.Client] = None

    # ---- Client accessors -------------------------------------------------

    def _get_ah_client(self) -> bigquery_analyticshub_v1.AnalyticsHubServiceClient:
        if self._ah_client is None:
            self._ah_client = bigquery_analyticshub_v1.AnalyticsHubServiceClient()
        return self._ah_client

    def _get_rm_client(self) -> resourcemanager_v3.ProjectsClient:
        if self._rm_client is None:
            self._rm_client = resourcemanager_v3.ProjectsClient()
        return self._rm_client

    def _get_bq_client(self) -> bigquery.Client:
        if self._bq_client is None:
            self._bq_client = self.config.get_bigquery_client()
        return self._bq_client

    # ---- Public API -------------------------------------------------------

    def populate_for_project(
        self, project_id: str, datasets: List[BigqueryDataset]
    ) -> None:
        """Detect linked datasets in a project and populate the lookup.

        A disabled Analytics Hub API is warned and a missing `subscriptions.list`
        grant is failed; both continue. Any other error propagates to the caller.
        """
        if not datasets:
            return

        # The API is location-scoped, so group datasets and query once per location.
        locations: Dict[str, List[str]] = {}
        for ds in datasets:
            location = (ds.location or "US").lower()
            locations.setdefault(location, []).append(ds.name)

        ah_client = self._get_ah_client()

        for location in locations:
            parent = f"projects/{project_id}/locations/{location}"
            try:
                subscriptions = list(ah_client.list_subscriptions(parent=parent))
            except PermissionDenied as e:
                if is_service_disabled(e):
                    self.report.warning(
                        title="BigQuery Sharing (Analytics Hub) API not enabled",
                        message=(
                            "The Analytics Hub API is not enabled on this project, "
                            "so linked dataset detection is skipped. Enable it to "
                            "ingest BigQuery Sharing metadata, or unset "
                            "`include_linked_datasets`."
                        ),
                        context=f"project={project_id}, location={location}",
                        exc=e,
                    )
                    continue
                if is_iam_permission_denied(e):
                    self.report.failure(
                        title="Missing permission to list BigQuery Sharing subscriptions",
                        message=(
                            "`include_linked_datasets` is enabled but the service "
                            "account lacks `analyticshub.subscriptions.list`. Grant "
                            "it (e.g. `roles/analyticshub.subscriptionOwner`) or "
                            "unset `include_linked_datasets`."
                        ),
                        context=f"project={project_id}, location={location}",
                        exc=e,
                    )
                    continue
                raise

            for sub in subscriptions:
                # Skip non-BigQuery shared resources (e.g. Pub/Sub topics).
                if (
                    sub.resource_type
                    != bigquery_analyticshub_v1.SharedResourceType.BIGQUERY_DATASET
                ):
                    continue

                consumer_dataset = self._consumer_dataset_name(sub)
                if consumer_dataset is None:
                    continue
                if not self.filters.is_dataset_allowed(
                    dataset_name=consumer_dataset, project_id=project_id
                ):
                    continue

                info = self._resolve_subscription(project_id, consumer_dataset, sub)
                if info is None:
                    continue

                self._lookup[(project_id, consumer_dataset)] = info
                self.report.num_linked_datasets_scanned += 1
                self._track_state_counters(info)

    def get_info(
        self, project_id: str, dataset_name: str
    ) -> Optional[LinkedDatasetInfo]:
        return self._lookup.get((project_id, dataset_name))

    def emits_copy_lineage(self, project_id: str, dataset_name: str) -> bool:
        """Whether this (project, dataset) will emit COPY lineage."""
        if not self.config.include_linked_dataset_lineage:
            return False
        info = self._lookup.get((project_id, dataset_name))
        return info is not None and info.has_publisher

    def gen_lineage_workunits(
        self,
        consumer_project_id: str,
        consumer_dataset: str,
        entity_name: str,
        columns: List[BigqueryColumn],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Siblings + UpstreamLineage for one table/view in a linked dataset."""
        info = self._lookup.get((consumer_project_id, consumer_dataset))
        if info is None or info.publisher is None:
            return
        publisher = info.publisher
        if publisher.project_id is None:
            return

        consumer_urn = self.identifiers.gen_dataset_urn(
            consumer_project_id, consumer_dataset, entity_name
        )
        publisher_urn = self.identifiers.gen_dataset_urn(
            publisher.project_id,
            publisher.dataset,
            entity_name,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=Siblings(primary=False, siblings=[publisher_urn]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=publisher_urn,
            aspect=Siblings(primary=True, siblings=[consumer_urn]),
        ).as_workunit()

        fine_grained = self._build_fine_grained_lineages(
            publisher_urn=publisher_urn,
            consumer_urn=consumer_urn,
            columns=columns,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=UpstreamLineage(
                upstreams=[
                    Upstream(dataset=publisher_urn, type=DatasetLineageType.COPY)
                ],
                fineGrainedLineages=fine_grained or None,
            ),
        ).as_workunit()

        self.report.num_linked_dataset_lineage_emitted += 1

    def _consumer_dataset_name(
        self, sub: bigquery_analyticshub_v1.Subscription
    ) -> Optional[str]:
        """Extract the consumer-side dataset name from a Subscription.

        The destination message and its dataset reference are always present on
        a proto Subscription; only the dataset ID may be empty.
        """
        return sub.destination_dataset.dataset_reference.dataset_id or None

    def _resolve_subscription(
        self,
        project_id: str,
        consumer_dataset: str,
        sub: bigquery_analyticshub_v1.Subscription,
    ) -> Optional[LinkedDatasetInfo]:
        """Resolve publisher refs and build a LinkedDatasetInfo.

        Returns None when the consumer dataset cannot be read; the caller then
        ingests it as a plain dataset.
        """
        bq = self._get_bq_client()
        try:
            ds = bq.get_dataset(f"{project_id}.{consumer_dataset}")
        except GoogleAPIError as e:
            self.report.num_linked_dataset_get_dataset_errors += 1
            self.report.warning(
                title="Cannot read linked dataset metadata",
                message=(
                    "`get_dataset` failed on a linked dataset, so it is ingested "
                    "without BigQuery Sharing enrichment. The dataset may have been "
                    "deleted after listing, or `bigquery.datasets.get` may be missing."
                ),
                context=f"{project_id}.{consumer_dataset}",
                exc=e,
            )
            return None

        # google-cloud-bigquery exposes raw API properties via _properties.
        properties = getattr(ds, "_properties", None) or {}
        link_state = (properties.get("linkedDatasetMetadata") or {}).get(
            "linkState"
        ) or None
        source = (properties.get("linkedDatasetSource") or {}).get(
            "sourceDataset"
        ) or {}
        publisher_project_number = source.get("projectId")
        publisher_dataset = source.get("datasetId")

        publisher: Optional[PublisherRef] = None
        if publisher_project_number and publisher_dataset:
            publisher = PublisherRef(
                project_number=publisher_project_number,
                dataset=publisher_dataset,
                project_id=self._resolve_publisher_project_id(publisher_project_number),
            )
        else:
            self.report.num_linked_dataset_source_unresolved += 1
            self.report.warning(
                title="Linked dataset source not resolved",
                message=(
                    "A dataset recognised as linked did not expose its source "
                    "dataset, so no lineage or siblings are emitted. This usually "
                    "means the subscriber cannot see the publisher project, or the "
                    "link is still pending."
                ),
                context=f"{project_id}.{consumer_dataset}",
            )

        creation_time = sub.creation_time
        last_modify_time = sub.last_modify_time

        return LinkedDatasetInfo(
            consumer_project_id=project_id,
            consumer_dataset=consumer_dataset,
            publisher=publisher,
            subscription_state=sub.state,
            link_state=link_state,
            listing=_last_segment(sub.listing),
            data_exchange=_last_segment(sub.data_exchange),
            publisher_organization=sub.organization_display_name or None,
            creation_time=creation_time.isoformat() if creation_time else None,
            last_modify_time=last_modify_time.isoformat() if last_modify_time else None,
        )

    def _resolve_publisher_project_id(self, project_number: str) -> Optional[str]:
        """Resolve a publisher project number to its project ID via Cloud RM.

        Cached (failures as None) so repeat references resolve with one RM call.
        """
        if project_number in self._publisher_project_id_cache:
            return self._publisher_project_id_cache[project_number]

        rm_client = self._get_rm_client()
        try:
            project = rm_client.get_project(name=f"projects/{project_number}")
            resolved: Optional[str] = project.project_id
        except GoogleAPIError as e:
            self.report.num_linked_dataset_project_resolve_errors += 1
            self.report.warning(
                title="Cannot resolve publisher project ID",
                message=(
                    "Lineage will be skipped for subscriptions whose publisher "
                    "project number cannot be resolved. Grant "
                    "`resourcemanager.projects.get` on the publisher project."
                ),
                context=f"publisher_project_number={project_number}",
                exc=e,
            )
            resolved = None

        self._publisher_project_id_cache[project_number] = resolved
        return resolved

    def _track_state_counters(self, info: LinkedDatasetInfo) -> None:
        State = bigquery_analyticshub_v1.Subscription.State
        if info.subscription_state == State.STATE_STALE:
            self.report.num_linked_dataset_state_stale += 1
        elif info.subscription_state == State.STATE_INACTIVE:
            self.report.num_linked_dataset_state_inactive += 1

    def _build_fine_grained_lineages(
        self,
        publisher_urn: str,
        consumer_urn: str,
        columns: List[BigqueryColumn],
    ) -> List[FineGrainedLineageClass]:
        """One FineGrainedLineage per column, 1:1 by name.

        Linked datasets mirror the publisher byte-identically, so column names
        always match. Honours `convert_column_urns_to_lowercase` for URN casing.
        """
        if not columns:
            return []

        lineages: List[FineGrainedLineageClass] = []
        seen: Set[str] = set()
        lowercase = self.config.convert_column_urns_to_lowercase

        for column in columns:
            column_name = column.name
            if not column_name:
                continue
            if lowercase:
                column_name = column_name.lower()
            if column_name in seen:
                continue
            seen.add(column_name)

            lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[make_schema_field_urn(publisher_urn, column_name)],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[make_schema_field_urn(consumer_urn, column_name)],
                )
            )

        return lineages


__all__ = [
    "BigQueryLinkedDatasetsHandler",
    "LinkedDatasetInfo",
    "PublisherRef",
]
