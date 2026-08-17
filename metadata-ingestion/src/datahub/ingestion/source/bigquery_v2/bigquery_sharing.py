import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Protocol, Set, Tuple

from google.api_core.exceptions import GoogleAPIError, PermissionDenied
from google.cloud import bigquery, resourcemanager_v3

if TYPE_CHECKING:
    # google-cloud-bigquery-analyticshub ships only with the `bigquery` extra. This
    # module is reached from bigquery.py, which bigquery-slim installs also load, so
    # importing it at runtime here would break that install. The one runtime use is a
    # local import inside _apply_subscription.
    from google.cloud.bigquery_analyticshub_v1 import AnalyticsHubServiceClient

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    LINK_STATE_LINKED,
    LINKED_DATASET_TYPE,
    BigqueryColumn,
    BigqueryDataset,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SiblingsClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger: logging.Logger = logging.getLogger(__name__)

# Split rather than one `project.dataset` string: the publisher is in a different
# project from the `project_id` already on this container, and each half is filterable
# on its own. Mirrors the container's existing project_id / dataset_id pair.
PROP_SOURCE_PROJECT_ID: str = "source_project_id"
PROP_SOURCE_DATASET_ID: str = "source_dataset_id"
PROP_LINK_STATE: str = "link_state"
# Only reachable through the BigQuery Sharing API.
PROP_LISTING_ID: str = "listing_id"
PROP_SUBSCRIPTION_STATE: str = "subscription_state"

# Subscription.State members are named STATE_ACTIVE, STATE_STALE, STATE_INACTIVE.
# The prefix is a protobuf artefact, so it is stripped before the value is emitted.
_STATE_PREFIX: str = "STATE_"

REASON_SERVICE_DISABLED: str = "SERVICE_DISABLED"
# The generated client wraps list_subscriptions with default_timeout=None, so nothing
# bounds a hung connection. 600s matches google-cloud-bigquery's own deadline.
_LIST_SUBSCRIPTIONS_TIMEOUT: float = 600.0


def _last_segment(resource_name: Optional[str]) -> Optional[str]:
    """Return the final path segment of an API resource name."""
    if not resource_name:
        return None
    return resource_name.rsplit("/", 1)[-1] or None


class BigQuerySharingConfigProtocol(Protocol):
    """The narrow config slice this handler needs."""

    include_linked_datasets: bool
    extract_subscriptions_from_analytics_hub: bool
    include_table_lineage: Optional[bool]
    convert_column_urns_to_lowercase: bool

    def get_sharing_client(
        self,
    ) -> "AnalyticsHubServiceClient": ...


@dataclass(frozen=True)
class PublisherRef:
    """The publisher side of a share. Holds a project ID, never a project number,
    because a URN built from a number matches nothing."""

    dataset: str
    project_id: str


@dataclass(frozen=True)
class LinkedDatasetInfo:
    consumer_project_id: str
    consumer_dataset: str
    publisher: Optional[PublisherRef] = None
    link_state: Optional[str] = None

    listing: Optional[str] = None
    subscription_state: Optional[str] = None

    @property
    def is_live_link(self) -> bool:
        """Whether to emit relationship metadata for this link.

        Suppress on evidence, not absence: `type == LINKED` already established this
        is a link, so a missing `linkState` is not grounds to treat it as dead.
        """
        return self.publisher is not None and self.link_state in (
            None,
            LINK_STATE_LINKED,
        )

    def to_extra_properties(self) -> Dict[str, str]:
        props: Dict[str, str] = {}
        if self.publisher is not None:
            props[PROP_SOURCE_PROJECT_ID] = self.publisher.project_id
            props[PROP_SOURCE_DATASET_ID] = self.publisher.dataset
        if self.link_state:
            props[PROP_LINK_STATE] = self.link_state
        if self.listing:
            props[PROP_LISTING_ID] = self.listing
        if self.subscription_state:
            props[PROP_SUBSCRIPTION_STATE] = self.subscription_state
        return props


class BigQuerySharingHandler:
    """Detects BigQuery Sharing linked datasets and emits their lineage.

    A share is created by a click rather than a query, so query-log lineage never
    sees it. Detection comes from `Dataset.type` on the existing datasets.list call.
    """

    def __init__(
        self,
        config: BigQuerySharingConfigProtocol,
        report: BigQueryV2Report,
        identifiers: BigQueryIdentifierBuilder,
        client: bigquery.Client,
        projects_client: resourcemanager_v3.ProjectsClient,
    ) -> None:
        self.config = config
        self.report = report
        self.identifiers = identifiers
        self.client = client
        self.projects_client = projects_client

        self._lookup: Dict[Tuple[str, str], LinkedDatasetInfo] = {}
        self._entities: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
        # `siblings` is versioned, so one write per consumer would leave only the last.
        self._publisher_siblings: Dict[str, Set[str]] = {}
        self._publisher_project_ids: Dict[str, Optional[str]] = {}
        self._project_number_map: Optional[Dict[str, str]] = None
        self._sharing_client: Optional["AnalyticsHubServiceClient"] = None

    # ---- population -------------------------------------------------------

    def populate_for_project(
        self, project_id: str, datasets: List[BigqueryDataset]
    ) -> None:
        """Resolve every linked dataset in a project.

        Must run before the per-dataset thread pool fans out: this writes the shared
        lookup that the workers only read.
        """
        linked = [ds for ds in datasets if ds.type == LINKED_DATASET_TYPE]
        if not linked:
            return

        self.report.num_linked_datasets_detected[project_id] = len(linked)
        for dataset in linked:
            info = self._resolve_from_dataset(project_id, dataset.name)
            # Counted per dataset so resolved + unresolved == detected. Resolution is
            # cached, so counting inside it would tally publishers, since several share one.
            if info is None or info.publisher is None:
                self.report.num_linked_datasets_unresolved += 1
            if info is None:
                continue
            self._lookup[(project_id, dataset.name)] = info
            if info.publisher is not None:
                self.report.num_linked_datasets_resolved += 1
            if info.link_state is None:
                self.report.num_linked_datasets_missing_link_state += 1
            elif info.link_state != LINK_STATE_LINKED:
                self.report.num_linked_datasets_not_linked += 1

        if self.config.extract_subscriptions_from_analytics_hub:
            self._enrich_from_sharing(project_id, linked)

    def _resolve_from_dataset(
        self, project_id: str, dataset_name: str
    ) -> Optional[LinkedDatasetInfo]:
        try:
            dataset = self.client.get_dataset(f"{project_id}.{dataset_name}")
        except Exception as e:
            # The client raises more than GoogleAPIError here. Catching broadly keeps
            # one unreadable dataset from ending the project or the run.
            self.report.warning(
                title="Cannot read linked dataset metadata",
                message=(
                    "Reading this dataset's full metadata failed, so it is ingested "
                    "without its source reference or lineage."
                ),
                context=f"{project_id}.{dataset_name}",
                exc=e,
            )
            return None

        # linkedDatasetSource is absent from the datasets.list field subset, so it is
        # only reachable through the full resource fetched above.
        properties = getattr(dataset, "_properties", None) or {}
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
            publisher_project_id = self._resolve_publisher_project_id(
                str(publisher_project_number)
            )
            if publisher_project_id is not None:
                publisher = PublisherRef(
                    dataset=publisher_dataset, project_id=publisher_project_id
                )
        else:
            self.report.warning(
                title="Linked dataset source not resolved",
                message=(
                    "A dataset reported itself as linked but exposed no source "
                    "dataset, so no lineage or siblings are emitted for it."
                ),
                context=f"{project_id}.{dataset_name}",
            )

        return LinkedDatasetInfo(
            consumer_project_id=project_id,
            consumer_dataset=dataset_name,
            publisher=publisher,
            link_state=link_state,
        )

    # ---- optional enrichment from the BigQuery Sharing API ----------------

    def _get_sharing_client(self) -> "AnalyticsHubServiceClient":
        if self._sharing_client is None:
            self._sharing_client = self.config.get_sharing_client()
        return self._sharing_client

    def _enrich_from_sharing(
        self, project_id: str, linked: List[BigqueryDataset]
    ) -> None:
        """Attach the listing and subscription state.

        Optional throughout. Lineage is complete without it, so a missing permission
        warns rather than fails. Matches and replaces existing entries, never creates.
        """
        try:
            sharing_client = self._get_sharing_client()
        except ImportError as e:
            # google-cloud-bigquery-analyticshub ships with the `bigquery` extra but
            # not with bigquery-slim, so this flag can be set on an install that has
            # no client to build. Reported like every other enrichment failure here:
            # the linked datasets and their lineage are already emitted.
            self.report.warning(
                title="BigQuery Sharing client unavailable",
                message=(
                    "`extract_subscriptions_from_analytics_hub` is on but "
                    "google-cloud-bigquery-analyticshub is not installed, so the "
                    "listing and subscription state are omitted. Lineage is "
                    "unaffected. Install `acryl-datahub[bigquery]` or unset the flag."
                ),
                context=project_id,
                exc=e,
            )
            return

        # The API is location-scoped; only locations holding a linked dataset can
        # contain a subscription.
        locations = {(ds.location or "US").lower() for ds in linked}

        for location in sorted(locations):
            parent = f"projects/{project_id}/locations/{location}"
            try:
                subscriptions = list(
                    sharing_client.list_subscriptions(
                        parent=parent, timeout=_LIST_SUBSCRIPTIONS_TIMEOUT
                    )
                )
            except PermissionDenied as e:
                self._report_sharing_denied(project_id, location, e)
                continue
            except GoogleAPIError as e:
                self.report.warning(
                    title="Could not read BigQuery Sharing subscriptions",
                    message=(
                        "Linked datasets are still detected and their lineage is "
                        "still emitted; only the sharing properties are missing."
                    ),
                    context=f"{project_id}, location {location}",
                    exc=e,
                )
                continue

            for subscription in subscriptions:
                self._apply_subscription(project_id, subscription)

    def _report_sharing_denied(
        self, project_id: str, location: str, exc: PermissionDenied
    ) -> None:
        # `reason` carries the response's ErrorInfo and is the precise signal, but it
        # is only populated when grpcio-status is importable, a google-api-core extra
        # this package does not require. The message match covers that case.
        reason = getattr(exc, "reason", None)
        detail = str(exc)
        if reason == REASON_SERVICE_DISABLED or (
            reason is None
            and (
                "SERVICE_DISABLED" in detail or "has not been used in project" in detail
            )
        ):
            self.report.warning(
                title="BigQuery Sharing API not enabled",
                message=(
                    "`extract_subscriptions_from_analytics_hub` is on but the Analytics "
                    "Hub API is not enabled on this project, so the listing and "
                    "subscription state are omitted. Lineage is unaffected. Enable "
                    "`analyticshub.googleapis.com` or unset the flag."
                ),
                context=f"{project_id}, location {location}",
                exc=exc,
            )
        else:
            self.report.warning(
                title="Missing permission to list BigQuery Sharing subscriptions",
                message=(
                    "`extract_subscriptions_from_analytics_hub` is on but the ingestion account "
                    "lacks `analyticshub.subscriptions.list`, so the sharing "
                    "properties are omitted. Lineage is unaffected. Grant the "
                    "permission or unset the flag."
                ),
                context=f"{project_id}, location {location}",
                exc=exc,
            )

    def _apply_subscription(self, project_id: str, subscription: object) -> None:
        from google.cloud.bigquery_analyticshub_v1 import SharedResourceType

        if getattr(subscription, "resource_type", None) != (
            SharedResourceType.BIGQUERY_DATASET
        ):
            # A project's Pub/Sub subscriptions come back from the same call.
            return

        # Read from `destination_dataset`, which reports IDs.
        # `linked_resources[].linked_dataset` looks equally usable but reports project
        # NUMBERS, and matching on it parses cleanly and silently finds nothing.
        reference = getattr(
            getattr(subscription, "destination_dataset", None),
            "dataset_reference",
            None,
        )
        consumer_dataset = getattr(reference, "dataset_id", None)
        if not consumer_dataset:
            return

        self.report.num_sharing_subscriptions_scanned += 1
        existing = self._lookup.get((project_id, consumer_dataset))
        if existing is None:
            # Detection and the sharing API disagree about this dataset: one reports
            # it as linked, the other does not. Counted rather than dropped silently.
            self.report.num_sharing_subscriptions_unmatched += 1
            return

        state = getattr(subscription, "state", None)
        state_name = getattr(state, "name", None)
        self._lookup[(project_id, consumer_dataset)] = replace(
            existing,
            listing=_last_segment(getattr(subscription, "listing", None)),
            subscription_state=(
                state_name[len(_STATE_PREFIX) :]
                if state_name and state_name.startswith(_STATE_PREFIX)
                else state_name
            ),
        )

    # ---- publisher project resolution, two tiers --------------------------

    def _resolve_publisher_project_id(self, project_number: str) -> Optional[str]:
        """Turn a project number into a project ID.

        The share reports its source as a project *number*; DataHub URNs use project
        IDs, and a URN built from a number matches nothing.
        """
        if project_number in self._publisher_project_ids:
            return self._publisher_project_ids[project_number]

        resolved = self._from_project_list(project_number)
        if resolved is None:
            resolved = self._from_resource_manager(project_number)

        self._publisher_project_ids[project_number] = resolved
        return resolved

    def _from_project_list(self, project_number: str) -> Optional[str]:
        """Tier 1, needing no additional permission. list_projects() carries both forms for every
        project the account holds a BigQuery role on."""
        if self._project_number_map is None:
            self._project_number_map = {}
            try:
                for project in self.client.list_projects():
                    numeric_id = getattr(project, "numeric_id", None)
                    if numeric_id is not None:
                        self._project_number_map[str(numeric_id)] = project.project_id
            except Exception as e:
                # Broad on purpose, matching _resolve_from_dataset: the client raises
                # more than GoogleAPIError, and tier 1 failing must fall through to
                # Resource Manager rather than end the project.
                logger.debug(f"Could not list projects for number resolution: {e}")

        resolved = self._project_number_map.get(project_number)
        if resolved is not None:
            self.report.num_publisher_lookups_from_project_list += 1
        return resolved

    def _from_resource_manager(self, project_number: str) -> Optional[str]:
        """Tier 2, for a publisher this account cannot see in BigQuery. Needs
        `resourcemanager.projects.get` on the publisher project."""
        try:
            project = self.projects_client.get_project(
                name=f"projects/{project_number}"
            )
            self.report.num_publisher_lookups_from_resource_manager += 1
            return project.project_id
        except GoogleAPIError as e:
            self.report.warning(
                title="Cannot resolve publisher project",
                message=(
                    "The share reports its source project as a number, and this "
                    "account can resolve it neither through BigQuery nor through "
                    "Resource Manager, so no lineage is emitted for it. Grant the "
                    "ingestion account a BigQuery role on the publisher project, or "
                    "`resourcemanager.projects.get` (roles/browser)."
                ),
                context=f"publisher project number {project_number}",
                exc=e,
            )
            return None

    # ---- lookup -----------------------------------------------------------

    def get_info(
        self, project_id: str, dataset_name: str
    ) -> Optional[LinkedDatasetInfo]:
        return self._lookup.get((project_id, dataset_name))

    def suppresses_view_definition(self, project_id: str, dataset_name: str) -> bool:
        """Whether the view path should skip registering a definition for SQL parsing.

        Registration writes the same upstreamLineage slot this handler writes, so only
        one of the two may do it. This covers every linked dataset, not only live ones:
        a dead link emits no COPY edge, and parsed lineage for a stale mirror would be
        no better than none.
        """
        if not self.config.include_table_lineage:
            return False
        return (project_id, dataset_name) in self._lookup

    # ---- deferred emission ------------------------------------------------

    def record_entities(
        self,
        project_id: str,
        dataset_name: str,
        columns_by_entity: Dict[str, List[BigqueryColumn]],
    ) -> None:
        """Note the tables and views of a linked dataset, for emission at the end.

        Called from the schema workers. `_entities` needs no lock only because each
        (project, dataset) key has exactly one writer; sharding the fan-out below
        dataset level breaks that.
        """
        info = self._lookup.get((project_id, dataset_name))
        if info is None or not info.is_live_link:
            return
        recorded = self._entities.setdefault((project_id, dataset_name), {})
        for entity_name, columns in columns_by_entity.items():
            names: List[str] = []
            seen: Set[str] = set()
            for column in columns or []:
                if column.name not in seen:
                    seen.add(column.name)
                    names.append(column.name)
            recorded[entity_name] = names

    def gen_all_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Emit every linked dataset's lineage, last in the run.

        Four code paths write `upstreamLineage` destructively and nothing reports the
        loss, so position is load-bearing.
        """
        for (project_id, dataset_name), entities in self._entities.items():
            for entity_name, column_names in entities.items():
                yield from self._gen_lineage_for_entity(
                    project_id, dataset_name, entity_name, column_names
                )

        for publisher_urn, consumer_urns in self._publisher_siblings.items():
            # is_primary_source=False: this run may not ingest the publisher's project,
            # and primary would have the framework synthesise `status` for an empty entity.
            yield MetadataChangeProposalWrapper(
                entityUrn=publisher_urn,
                aspect=SiblingsClass(primary=True, siblings=sorted(consumer_urns)),
            ).as_workunit(is_primary_source=False)

    # ---- emission ---------------------------------------------------------

    def _gen_lineage_workunits(
        self,
        project_id: str,
        dataset_name: str,
        entity_name: str,
        columns: List[BigqueryColumn],
    ) -> Iterable[MetadataWorkUnit]:
        # Exercised only by tests: production reaches _gen_lineage_for_entity via
        # gen_all_lineage_workunits, which caches names rather than column objects.
        yield from self._gen_lineage_for_entity(
            project_id, dataset_name, entity_name, [c.name for c in columns]
        )

    def _gen_lineage_for_entity(
        self,
        project_id: str,
        dataset_name: str,
        entity_name: str,
        column_names: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        info = self._lookup.get((project_id, dataset_name))
        if info is None or not info.is_live_link:
            return
        publisher = info.publisher
        assert publisher is not None  # is_live_link implies it

        consumer_urn = self.identifiers.gen_dataset_urn(
            project_id, dataset_name, entity_name
        )
        publisher_urn = self.identifiers.gen_dataset_urn(
            publisher.project_id, publisher.dataset, entity_name
        )

        # Both URNs describe the same underlying storage, so the pair is recorded as
        # siblings alongside the lineage edge.
        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=SiblingsClass(primary=False, siblings=[publisher_urn]),
        ).as_workunit()
        self._publisher_siblings.setdefault(publisher_urn, set()).add(consumer_urn)

        if not self.config.include_table_lineage:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=publisher_urn, type=DatasetLineageTypeClass.COPY
                    )
                ],
                fineGrainedLineages=self._build_fine_grained_lineages(
                    publisher_urn=publisher_urn,
                    consumer_urn=consumer_urn,
                    column_names=column_names,
                )
                or None,
            ),
        ).as_workunit()
        self.report.num_linked_dataset_lineage_emitted += 1

    def _build_fine_grained_lineages(
        self, publisher_urn: str, consumer_urn: str, column_names: List[str]
    ) -> List[FineGrainedLineageClass]:
        """One edge per column. A share copies nothing, so the mapping is identity.

        A STRUCT arrives as one entry per leaf path, all sharing a name, hence the
        dedup. Lowercasing must happen before the field URN is built, not after.
        """
        lineages: List[FineGrainedLineageClass] = []
        seen: Set[str] = set()
        lowercase = self.config.convert_column_urns_to_lowercase

        for raw_name in column_names:
            column_name = raw_name.lower() if lowercase else raw_name
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
