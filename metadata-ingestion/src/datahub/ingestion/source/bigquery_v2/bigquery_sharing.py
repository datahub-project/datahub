from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from google.api_core.exceptions import InvalidArgument, NotFound, PermissionDenied
from google.cloud import bigquery, resourcemanager_v3

if TYPE_CHECKING:
    # analyticshub ships only with the `bigquery` extra, not bigquery-slim, so it is
    # imported locally in _apply_subscription rather than at runtime module scope.
    from google.cloud.bigquery_analyticshub_v1 import (
        AnalyticsHubServiceClient,
        Subscription,
    )

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigQueryTableRef
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    LINK_STATE_LINKED,
    BigqueryDataset,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

# Two fields: the publisher's project differs from the container's own project_id.
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
# Per-request bound for list_projects, which the client leaves at None. Sits inside the
# client's own 600s retry budget so a slow request cannot exhaust it in one attempt.
_LIST_PROJECTS_TIMEOUT: float = 120.0


def _last_segment(resource_name: Optional[str]) -> Optional[str]:
    """Return the final path segment of an API resource name."""
    if not resource_name:
        return None
    return resource_name.rsplit("/", 1)[-1] or None


@dataclass(frozen=True)
class PublisherRef:
    """The publisher side of a share. Holds a project ID, never a project number,
    because a URN built from a number matches nothing."""

    dataset: str
    project_id: str


@dataclass(frozen=True)
class LinkedDatasetInfo:
    publisher: Optional[PublisherRef] = None
    link_state: Optional[str] = None

    listing: Optional[str] = None
    subscription_state: Optional[str] = None

    @property
    def live_publisher(self) -> Optional[PublisherRef]:
        """The publisher to emit relationship metadata for, or None if the link is not live.

        Suppress on evidence, not absence: `type == LINKED` already established this
        is a link, so a missing `linkState` is not grounds to treat it as dead.
        """
        if self.link_state not in (None, LINK_STATE_LINKED):
            return None
        return self.publisher

    @property
    def is_live_link(self) -> bool:
        return self.live_publisher is not None

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
        config: BigQueryV2Config,
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
        # Positive resolutions only, from either tier; number -> project ID.
        self._resolved_publisher_ids: Dict[str, str] = {}
        # Numbers whose tier-2 lookup failed this run, terminal or transient (the two differ
        # only in the warning). Not re-hit this run; a rerun starts clean and retries.
        self._unresolvable_project_numbers: Set[str] = set()
        self._project_number_map: Optional[Dict[str, str]] = None
        self._sharing_client: Optional["AnalyticsHubServiceClient"] = None
        # Warn once per run, not once per project, when Dataset.type is wholesale absent.
        self._warned_all_dataset_types_missing: bool = False
        self._warned_list_projects_failed: bool = False

    # ---- population -------------------------------------------------------

    def populate_for_project(
        self, project_id: str, datasets: List[BigqueryDataset]
    ) -> None:
        """Resolve every linked dataset in a project.

        Must run before the per-dataset thread pool fans out: this writes the shared
        lookup that the workers only read.
        """
        linked = [ds for ds in datasets if ds.is_linked_dataset()]
        if not linked:
            # If Dataset.type stops being returned, every dataset reads as non-linked
            # and the feature silently no-ops; warn once so that is visible.
            if (
                not self._warned_all_dataset_types_missing
                and datasets
                and all(ds.type is None for ds in datasets)
            ):
                self._warned_all_dataset_types_missing = True
                self.report.warning(
                    title="Linked datasets enabled but no dataset types returned",
                    message=(
                        "`include_linked_dataset_lineage` is enabled but no dataset "
                        "reported a type, so linked datasets cannot be detected. This "
                        "usually means the BigQuery datasets.list response omitted the "
                        "type field."
                    ),
                    context=project_id,
                )
            return

        self.report.num_linked_datasets_detected[project_id] = len(linked)
        self._ensure_project_number_map()
        for dataset in linked:
            info = self._resolve_from_dataset(project_id, dataset.name)
            # Counted per dataset so resolved + unresolved == detected. Resolution is
            # cached, so counting inside it would tally publishers, since several share one.
            if info is None or info.publisher is None:
                self.report.num_linked_datasets_unresolved += 1
            if info is None:
                continue
            # A dataset sealed unresolved during a transient outage is not backfilled
            # mid-run (it would desync the resolved/unresolved counters); it heals next run.
            self._lookup[(project_id, dataset.name)] = info
            if info.publisher is not None:
                self.report.num_linked_datasets_resolved += 1
            if info.link_state is None:
                self.report.num_linked_datasets_missing_link_state += 1
            elif info.link_state != LINK_STATE_LINKED:
                self.report.num_linked_datasets_not_linked += 1

        if self.config.extract_subscriptions_from_analytics_hub:
            try:
                self._enrich_from_sharing(project_id, linked)
            except Exception as e:
                # Enrichment is optional and must never abort the project; backstops
                # anything _enrich_from_sharing's handlers miss (e.g. a bad subscription).
                self.report.warning(
                    title="BigQuery Sharing enrichment failed",
                    message=(
                        "Listing and subscription state are omitted for this project; "
                        "lineage is unaffected."
                    ),
                    context=project_id,
                    exc=e,
                )

    def _resolve_from_dataset(
        self, project_id: str, dataset_name: str
    ) -> Optional[LinkedDatasetInfo]:
        try:
            dataset = self.client.get_dataset(f"{project_id}.{dataset_name}")

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
                        "dataset, so no lineage is emitted for it."
                    ),
                    context=f"{project_id}.{dataset_name}",
                )

            return LinkedDatasetInfo(
                publisher=publisher,
                link_state=link_state,
            )
        except Exception as e:
            # Broad on purpose: the client raises more than GoogleAPIError, and a malformed
            # _properties payload raises AttributeError here. This parse runs before the
            # fan-out, so an uncaught error aborts every remaining project, not just this one.
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
            # analyticshub is absent on bigquery-slim, so the flag can be set with no
            # client to build. Non-fatal; lineage is already emitted.
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
        except Exception as e:
            # Client construction resolves credentials, so it fails beyond ImportError.
            self.report.warning(
                title="BigQuery Sharing client could not be created",
                message=(
                    "`extract_subscriptions_from_analytics_hub` is on but the Analytics "
                    "Hub client could not be constructed, so the listing and "
                    "subscription state are omitted. Lineage is unaffected. Check the "
                    "ingestion account's credentials and the installed client version."
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
            except Exception as e:
                # Broader than GoogleAPIError: a credential refresh raises GoogleAuthError.
                # One location failing must not stop the others or the project.
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
        # `reason` (ErrorInfo) is precise but only set when grpcio-status is importable,
        # an extra this package does not require, so we match on the message too.
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

    def _apply_subscription(
        self, project_id: str, subscription: "Subscription"
    ) -> None:
        from google.cloud.bigquery_analyticshub_v1 import SharedResourceType

        if subscription.resource_type != SharedResourceType.BIGQUERY_DATASET:
            # A project's Pub/Sub subscriptions come back from the same call.
            return

        # Counted before the dataset_id check so a subscription naming no dataset still
        # leaves a trace.
        self.report.num_sharing_subscriptions_scanned += 1

        # Use `destination_dataset` (reports IDs); `linked_resources[].linked_dataset`
        # reports project NUMBERS, so matching on it silently finds nothing.
        reference = subscription.destination_dataset.dataset_reference
        consumer_dataset = reference.dataset_id
        if not consumer_dataset:
            self.report.num_sharing_subscriptions_unmatched += 1
            return

        # The destination dataset can live in a different project than the subscription, so
        # key on its own project_id, not the loop's, or a same-named dataset gets mis-stamped.
        key = (reference.project_id or project_id, consumer_dataset)

        existing = self._lookup.get(key)
        if existing is None:
            # Detection and the sharing API disagree about this dataset: one reports
            # it as linked, the other does not. Counted rather than dropped silently.
            self.report.num_sharing_subscriptions_unmatched += 1
            return

        self._lookup[key] = replace(
            existing,
            listing=_last_segment(subscription.listing),
            subscription_state=subscription.state.name.removeprefix(_STATE_PREFIX),
        )

    # ---- publisher project resolution, two tiers --------------------------

    def _resolve_publisher_project_id(self, project_number: str) -> Optional[str]:
        """Turn a project number into a project ID.

        The share reports its source as a project *number*; DataHub URNs use project
        IDs, and a URN built from a number matches nothing.
        """
        cached = self._resolved_publisher_ids.get(project_number)
        if cached is not None:
            return cached

        # Tier 1 is read first every time, so a late-built map beats an earlier tier-2
        # failure without a reconciliation pass. Tier 2 is attempted once per number.
        resolved = self._from_project_list(project_number)
        if (
            resolved is None
            and project_number not in self._unresolvable_project_numbers
        ):
            resolved = self._from_resource_manager(project_number)

        if resolved is not None:
            self._resolved_publisher_ids[project_number] = resolved
        return resolved

    def _ensure_project_number_map(self) -> None:
        """Build the number->id map once per run, up front, so no dataset resolves before
        tier 1 is ready. A failed build leaves the map None (never a partial ``{}``) and
        retries on the next project."""
        if self._project_number_map is not None:
            return
        mapping: Dict[str, str] = {}
        try:
            # DEFAULT_RETRY already covers projects.list's rate-limit errors at its
            # 2 req/s quota, so only the timeout is set here.
            for project in self.client.list_projects(timeout=_LIST_PROJECTS_TIMEOUT):
                numeric_id = getattr(project, "numeric_id", None)
                if numeric_id is not None:
                    mapping[str(numeric_id)] = project.project_id
        except Exception as e:
            # Broad on purpose: tier 1 failing must fall through to Resource Manager, not
            # end the project, and the client raises more than GoogleAPIError.
            if not self._warned_list_projects_failed:
                self._warned_list_projects_failed = True
                self.report.warning(
                    title="Could not list projects to resolve publisher project numbers",
                    message=(
                        "Publisher resolution falls back to Resource Manager, which "
                        "needs `resourcemanager.projects.get` on each publisher "
                        "project. This is the ingestion account's own project list "
                        "failing, not a permission on the publisher."
                    ),
                    exc=e,
                )
            return
        self._project_number_map = mapping

    def _from_project_list(self, project_number: str) -> Optional[str]:
        """Tier 1: read the number->id map built by _ensure_project_number_map."""
        if self._project_number_map is None:
            return None
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
        except (PermissionDenied, NotFound, InvalidArgument) as e:
            # Terminal: this account cannot resolve the number and a rerun will not help.
            self._unresolvable_project_numbers.add(project_number)
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
        except Exception as e:
            # Not a permission denial (a credential refresh raises GoogleAuthError, a slow
            # call DeadlineExceeded), so possibly transient. Not re-hit this run; a rerun retries.
            self._unresolvable_project_numbers.add(project_number)
            self.report.warning(
                title="Publisher project resolution failed, possibly transiently",
                message=(
                    "Resolving this publisher failed with an error that is not a "
                    "permission denial, so no lineage is emitted for datasets shared "
                    "from it in this run. Rerun to retry; if it recurs, check "
                    "`resourcemanager.projects.get` on the publisher."
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

    def needs_schema_for_copy_lineage(self, project_id: str, dataset_name: str) -> bool:
        """Whether this dataset's COPY edge will need its resolved schema.

        The edge builds identity column lineage from the consumer schema, so the schema
        must be registered even with the SQL parser off, but only when a COPY edge is
        actually emitted: table lineage on, and a live link with a resolved publisher.
        """
        if not self.config.include_table_lineage:
            return False
        info = self.get_info(project_id, dataset_name)
        return info is not None and info.live_publisher is not None

    # ---- lineage registration ---------------------------------------------

    def register_known_lineage(
        self, aggregator: SqlParsingAggregator, table_refs: Set[str]
    ) -> None:
        """Register a COPY mapping from each linked table, view, and snapshot to its publisher.

        A share exposes the publisher's objects read-only, so each linked object is a
        verbatim copy of the publisher's same-named object. The mapping goes through the
        aggregator's add_known_lineage_mapping, which handles emission and builds the
        identity column lineage from the resolved consumer schema.

        `table_refs` is already filtered by each object's *_pattern, so it is used as-is.
        """
        for ref in table_refs:
            entity = BigQueryTableRef.from_string_name(ref).table_identifier
            info = self._lookup.get((entity.project_id, entity.dataset))
            publisher = info.live_publisher if info is not None else None
            if publisher is None:
                continue
            consumer_urn = self.identifiers.gen_dataset_urn(
                entity.project_id, entity.dataset, entity.table
            )
            publisher_urn = self.identifiers.gen_dataset_urn(
                publisher.project_id, publisher.dataset, entity.table
            )
            aggregator.add_known_lineage_mapping(
                upstream_urn=publisher_urn, downstream_urn=consumer_urn
            )
            self.report.num_linked_dataset_lineage_emitted += 1
