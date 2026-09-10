import dataclasses

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList

# Constant, with no interpolation: `title` and `message` are the aggregation keys
# for structured-log grouping, so a dynamic value in either creates a separate
# bucket per object type. code_style.md classes that as a BLOCKER. The object
# type goes in `context` instead.
LARGE_HISTORY_MESSAGE = (
    "Reading this history view took more pages than expected. The whole view is "
    "still read, so metadata is complete, but the run will be slower than "
    "expected and this is worth checking."
)

EMPTY_INVENTORY_MESSAGE = (
    "No Openflow objects of this type were returned. This can mean the account has "
    "none, but it can equally mean the ingestion role lacks MONITOR on them: "
    "SHOW OPENFLOW ... is privilege-filtered per object and returns zero rows "
    "without an error. Verify with SHOW GRANTS TO ROLE <role> before concluding "
    "the account is empty."
)


@dataclasses.dataclass
class SnowflakeOpenflowReport(StaleEntityRemovalSourceReport):
    num_deployments: int = 0
    # No version_location_uri, so the configuration -- and therefore the
    # lineage derived from it -- could not be read.
    num_connectors_without_config_uri: int = 0
    # Not addressable by DESCRIBE, so no external link. Counted, not warned:
    # for a connector visible only in history this is the steady state.
    num_connectors_without_fqn: int = 0
    num_connectors_without_destination_database: int = 0
    # DESCRIBE answered but not with a usable canvas URL.
    num_connector_urls_failed: int = 0
    # Connectors left without a link because the account crossed the
    # distinct-runtime threshold. A count of connectors, not of runtimes:
    # it reports the impact, while the gate keys on the cause.
    num_connector_urls_skipped_for_scale: int = 0
    # Two runtimes share a name, so the connector's parent is a guess. Left
    # un-nested rather than nested wrongly.
    num_connectors_with_ambiguous_runtime: int = 0
    num_keys_with_mixed_lifecycle_rows: int = 0
    # Rows whose identity column was absent, so from_row could not build a model.
    # Non-zero means objects are missing from this run's inventory -- and with
    # stateful ingestion that reads as a deletion.
    num_rows_missing_identity: int = 0
    # SHOW calls that came back at exactly the row cap, so the inventory they
    # produced may be short. See _warn_if_show_truncated.
    num_show_results_at_row_cap: int = 0
    # Distinct CREATED_ON renderings that were present but unparseable. Non-zero
    # means ordering is degraded, and unordered rows resolve as deleted.
    num_unparseable_timestamps: int = 0
    # Objects filtered out because a lifecycle row said DELETED_ON. Worth
    # surfacing because a history-only key is EITHER genuinely deleted OR
    # invisible to SHOW for privilege reasons, and the two look identical here.
    num_objects_treated_as_deleted: int = 0
    # Runtimes skipped because their deployment was not visible. Skipped, not
    # un-nested: the runtime container URN embeds the deployment key.
    num_runtimes_without_deployment_parent: int = 0
    # The history views paged. Expected on a churning account; the signal is
    # that a single page is NOT the norm there.
    num_history_pages_beyond_first: int = 0
    num_runtimes: int = 0
    num_connectors: int = 0

    # Ownership ASPECTS, not owners: one per container, flow and per-table job
    # that carries the connector's OWNER.
    num_owners_emitted: int = 0

    num_lineage_edges: int = 0
    # One DataJob per replicated table. These are the only DataJobs emitted --
    # the connector itself is the DataFlow, not a task inside its own pipeline.
    num_table_jobs: int = 0
    num_lineage_edges_skipped: int = 0
    # Edges emitted with a destination but no upstream, because the
    # connector's source URL did not yield the database its platform needs.
    num_upstream_inlets_skipped: int = 0
    # Connectors whose configuration named neither table names nor a
    # pattern -- the shape a renamed source property would take.
    num_connectors_without_table_configuration: int = 0
    # Connectors configured with a table PATTERN rather than explicit names.
    # Their tables cannot be enumerated from config, so they get connector-level
    # lineage only.
    num_connectors_without_enumerable_tables: int = 0
    num_config_reads_failed: int = 0
    # Connectors whose runtime container was not emitted this run, so their
    # DataFlow is un-nested. Counted even when the cause is a deliberate
    # runtime_pattern filter (which warns nothing), so the total is visible
    # rather than only its anomalous half.
    num_connectors_without_runtime_parent: int = 0

    filtered_deployments: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_runtimes: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_connectors: LossyList[str] = dataclasses.field(default_factory=LossyList)

    def report_dropped_deployment(self, name: str) -> None:
        self.filtered_deployments.append(name)

    def report_dropped_runtime(self, name: str) -> None:
        self.filtered_runtimes.append(name)

    def report_dropped_connector(self, name: str) -> None:
        self.filtered_connectors.append(name)

    def report_empty_inventory(self, object_type: str) -> None:
        self.warning(
            title="No Openflow objects found",
            message=EMPTY_INVENTORY_MESSAGE,
            context=object_type,
        )
