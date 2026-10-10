"""Ingestion report for the Qualytics source.

Counters exist so an operator reading the end-of-run report can tell *why* something
did not show up in DataHub without reading logs. Prefer adding a specific counter over
reusing a generic one -- "12 checks skipped" is a support ticket, "12 checks skipped:
unmapped rule type" is an answer.
"""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList, LossySet


@dataclass
class QualyticsSourceReport(StaleEntityRemovalSourceReport):
    # Which Qualytics build this run talked to. Every deployment runs its own version,
    # so this is the first thing worth knowing when a mapping behaves unexpectedly.
    qualytics_version: str | None = None

    # What we read from Qualytics.
    datastores_scanned: int = 0
    containers_scanned: int = 0
    quality_checks_scanned: int = 0
    anomalies_scanned: int = 0

    # What we emitted to DataHub.
    assertions_emitted: int = 0
    # Assertions whose list-valued properties (expectedValues and the like) were trimmed
    # to MAX_LIST_PARAMETER_ITEMS. Each carries a `<key>_total_count` with the real size.
    assertion_parameters_truncated: int = 0
    assertion_results_emitted: int = 0
    # Results Qualytics reported without a usable timestamp. They cannot be placed on
    # a timeline, and stamping them with the run time would misdate the history.
    assertion_results_undated: int = 0
    # Checks Qualytics has never evaluated, so there is no verdict to emit. Normal,
    # but counted so it is distinguishable from a mapping bug.
    assertion_results_unevaluated: int = 0
    # Anomalies naming a check that has no assertion in this run: the check has been
    # archived since, or could not be parsed (see items_unparseable).
    anomalies_without_assertion: int = 0
    profiles_emitted: int = 0
    profiles_failed: int = 0
    # Containers Qualytics has never profiled. Normal -- a counter, not a warning.
    containers_unprofiled: int = 0
    # Profiles withheld because their dataset is not in DataHub. Writing one would
    # create the dataset -- a stub holding nothing but our profile, which is the shadow
    # catalog this connector must never build. Usually a table the warehouse source has
    # not ingested yet; the profile follows on the first run after it has.
    profiles_skipped_dataset_missing: int = 0
    datasets_missing: LossyList[str] = field(default_factory=LossyList)
    # Profiles emitted without that check, because the run had no DataHub connection
    # (a file sink, for instance). Non-zero means the output can create stub datasets
    # if it is later loaded into a DataHub that lacks them.
    profiles_existence_unchecked: int = 0
    # Profiles skipped because the existence check itself failed (DataHub unreachable
    # or refusing the token). After the first failure the check is not retried.
    profiles_existence_check_failed: int = 0
    # Containers whose anomaly listing failed. Their assertions and current verdicts
    # were emitted; only that run's failure history is missing.
    anomaly_listings_failed: int = 0
    # Fields whose histogram exceeded MAX_DISTINCT_VALUE_FREQUENCIES and was trimmed
    # to its most frequent buckets. Non-zero means the UI is showing a partial
    # distribution for those fields.
    profile_histograms_truncated: int = 0

    # Dropped on purpose by an allow/deny pattern. Kept separate from failures so a
    # deliberately narrow scope does not look like a broken run.
    datastores_dropped: int = 0
    containers_dropped: int = 0
    # Skipped because processing them raised, as opposed to being filtered out.
    containers_failed: int = 0
    # Objects whose payload did not match the model -- a required field missing, not
    # merely an unrecognised enum. Skipped individually rather than aborting the run.
    items_unparseable: int = 0
    # Datastores and containers whose type this build does not recognise at all.
    datastores_unrecognised: int = 0
    containers_unrecognised: int = 0

    # URN resolution -- the highest-consequence failure mode. An assertion attached to
    # a wrongly reconstructed URN is worse than one that was never emitted, so the
    # resolver is expected to skip and count rather than guess.
    urns_resolved: int = 0
    # Containers on a resolved datastore whose dataset name could not be built.
    # Counted per container, apart from datastores_unresolved, which is per datastore.
    containers_unnamed: int = 0
    datastores_unresolved: int = 0
    unresolved_datastores: LossyList[str] = field(default_factory=LossyList)

    # Object-store URNs whose dataset name was reconstructed from the bucket and path
    # rather than derived from a deterministic convention. The customer's path_spec
    # decides where the "table" boundary sits, so these are the ones most likely to
    # miss their target -- surfacing the count tells an operator whether to reach for
    # datastore_to_platform_map.
    urns_resolved_by_path_reconstruction: int = 0

    # Rule types with no explicit mapping, emitted as custom assertions. Non-empty
    # here after a Qualytics upgrade is the signal to extend the assertion mapper.
    unmapped_rule_types: LossySet[str] = field(default_factory=LossySet)

    def report_unresolved_datastore(self, datastore: str) -> None:
        """Record a datastore that could not be mapped to a DataHub platform."""
        self.datastores_unresolved += 1
        self.unresolved_datastores.append(datastore)

    def report_unmapped_rule_type(self, rule_type: str) -> None:
        self.unmapped_rule_types.add(rule_type)
