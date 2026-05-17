from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from queue import Queue
from typing import Any, Callable, Literal


@dataclass
class SeededEntity:
    urn: str
    aspect_name: str
    tc_number: int
    seeded_data: dict[str, Any]
    expected_schema_version: int
    validator: Callable[[dict[str, Any]], bool]


@dataclass
class IOObservation:
    worker: str
    urn: str
    aspect_name: str
    observed_version: int
    expected_version: int
    timestamp: datetime


@dataclass
class IOWriteResult:
    worker: str
    urn: str
    observed_version: int
    expected_version: int
    passed: bool
    timestamp: datetime
    error: str | None = None


@dataclass
class SnapshotT0:
    """Pre-upgrade ES + MySQL state captured before SystemUpdateBlocking runs.

    Written by ``SnapshotT0Phase``. Read by future ``UpgradeBlockingPhase``
    (to compute alias-swap deltas) and ``ValidationPhase`` (to verify
    post-upgrade invariants).
    """

    epoch_ms: int
    indices: dict[str, list[str]] = field(default_factory=dict)
    """alias name -> list of physical index names it points at (empty if none)."""

    doc_counts: dict[str, int] = field(default_factory=dict)
    """physical index name -> doc count at T0."""

    aspects_by_version: dict[str, dict[int | None, int]] = field(default_factory=dict)
    """aspect name -> {schemaVersion or None: row count} at T0."""

    upgrade_result_present: bool = False
    """True if a ``DataHubUpgradeResult`` exists for the configured upgrade
    version, indicating a prior partial/complete run that may need cleanup."""


@dataclass
class IndexState:
    """One row of ``DataHubUpgradeResult.indicesState`` after Phase 1 runs.

    Field names match the design-doc spec for required keys; values map
    directly from the parsed JSON. Optional fields are ``None`` when the
    upgrade step skipped persisting them (e.g., a no-reindex pass-through).
    """

    alias: str
    next_index_name: str | None = None
    old_backing_index_name: str | None = None
    reindex_start_time: int | None = None
    source_doc_count: int = 0
    task_id: str | None = None
    requires_data_backfill: bool = False
    status: str = "UNKNOWN"


@dataclass
class UpgradeBlockingResult:
    """Captured by ``UpgradeBlockingPhase`` after ``system-update -u SystemUpdateBlocking``.

    ``indices`` is the structured view derived from ``DataHubUpgradeResult.indicesState``.
    ``alias_swaps_observed`` is the list of ``(alias, next_index)`` pairs the framework
    saw in real time from the upgrade-job stdout — primary evidence the swap happened.
    ``raw`` is the full parsed ``DataHubUpgradeResult`` aspect for failure-bundle dumps.
    """

    indices: list[IndexState] = field(default_factory=list)
    alias_swaps_observed: list[tuple[str, str]] = field(default_factory=list)
    # Indices that hit the in-place mapping update path (production
    # ``ESIndexBuilder.updateMappingsInPlace`` — fires when
    # ReindexConfig.isPureMappingsAddition is true). Distinct from
    # ``alias_swaps_observed`` — these indices were NOT reindexed; their
    # mapping was patched in place. Used by TC-103.
    indices_updated_in_place: list[str] = field(default_factory=list)
    raw: dict | None = None
    duration_s: float = 0.0
    upgrade_id: str | None = None


@dataclass
class UpgradeBlockingReRunResult:
    """Captured by ``UpgradeBlockingReRunPhase`` — the second invocation of
    ``SystemUpdateBlocking``.

    On a re-run, every index already-completed by Phase 6 must either:
      - emit ``Index <name> already COMPLETED in previous run, skipping``
        (captured in ``skip_already_done_aliases``), OR
      - not appear in the rerun log stream at all (production opt-out path,
        e.g., ``ReindexConfig`` no longer flags the index as needing reindex).

    Any entry in ``rerun_alias_swaps_observed`` with a non-empty
    ``next_index_name`` is a regression — the upgrade re-did work it should
    have skipped per ``BuildIndicesIncrementalStep.java:103-112``.
    """

    skip_already_done_aliases: list[str] = field(default_factory=list)
    rerun_alias_swaps_observed: list[tuple[str, str]] = field(default_factory=list)
    duration_s: float = 0.0
    upgrade_id: str | None = None
    rerun_exit_code: int = -1


@dataclass
class Tc112FaultInjectionResult:
    """Captures the TC-112 fault-injection upgrade run.

    Engineered fault: pre-stage MySQL state to point ``alias`` at a manually
    created ``bad_target_index`` whose doc count differs from the alias's
    source. Production resume path runs ``validateAndSwapAlias``, detects the
    mismatch, logs ``Doc count mismatch for alias swap X -> Y: ...``, and
    exits FAILED. The framework captures each mismatch event in
    ``mismatch_events_observed`` as ``(alias, bad_target)`` tuples.
    """

    target_alias: str = ""
    bad_target_index: str = ""
    source_doc_count: int = 0
    target_doc_count: int = 0
    mismatch_events_observed: list[tuple[str, str]] = field(default_factory=list)
    upgrade_exit_code: int = -1
    duration_s: float = 0.0


@dataclass
class DataIntegritySnapshot:
    """Post-Phase-10 ES presence check for gap and dual URNs.

    Written by ``DataIntegritySnapshotPhase``. Read by the Suite D
    data-integrity validators (TC-201, TC-204) to assert no gap/dual URN
    was lost across the rolling-restart + catch-up window.

    ``entity_index_presence[urn]`` is True iff the URN is searchable via
    the entity index alias (``dashboardindex_v2``).
    ``systemmetadata_counts[urn]`` is the number of aspect entries in
    ``system_metadata_service_v1`` keyed by that URN (typically 4 per
    URN: corresponding to the ZDU framework's seed aspects).
    """

    entity_index_alias: str = "dashboardindex_v2"
    entity_index_presence: dict[str, bool] = field(default_factory=dict)
    systemmetadata_counts: dict[str, int] = field(default_factory=dict)
    # Suite F TC-401 — every gap/dual URN's embed aspect must be
    # at the target schemaVersion in MySQL after the sweep. Values are the
    # schemaVersion read from systemMetadata; ``None`` means the row didn't
    # exist or the systemMetadata.schemaVersion key wasn't set.
    embed_schema_versions: dict[str, int | None] = field(default_factory=dict)


@dataclass
class UpgradeNonBlockingResult:
    """Captured by ``UpgradeNonBlockingPhase`` after ``system-update -u SystemUpdateNonBlocking``.

    ``indices`` is the structured view derived from the post-sweep
    ``DataHubUpgradeResult.indicesState`` — symmetric to ``UpgradeBlockingResult.indices``
    but reflects the dual-write-disable / catch-up state, not the alias swap state.
    ``dual_write_disabled_indices`` is the list of physical index names the framework
    observed transition to ``DUAL_WRITE_DISABLED`` via the log line
    ``Marked index {name} as DUAL_WRITE_DISABLED``. ``catch_up_windows`` maps physical
    index name → ``(T0_ms, T1_ms)`` parsed from
    ``Catch-up for entity index {name}: window [{T0}, {T1}]``.
    ``raw`` is the full parsed ``DataHubUpgradeResult`` aspect for failure-bundle dumps.
    """

    indices: list[IndexState] = field(default_factory=list)
    dual_write_disabled_indices: list[str] = field(default_factory=list)
    catch_up_windows: dict[str, tuple[int, int]] = field(default_factory=dict)
    raw: dict | None = None
    duration_s: float = 0.0
    upgrade_id: str | None = None


@dataclass
class RuntimeMigrationProbe:
    """One read/write probe captured by ``RuntimeMigrationPhase``.

    ``mode`` distinguishes a read-path mutator probe (``"read"``) from a
    write-path mutator probe (``"write"``). ``observed_version`` is the
    ``schemaVersion`` returned by GMS; for write probes it's the version
    persisted on the post-write read-back. ``expected_version`` is the
    target schema version the mutator chain should produce.
    """

    urn: str
    aspect_name: str
    mode: Literal["read", "write"]
    observed_version: int
    expected_version: int
    timestamp: datetime
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and self.observed_version == self.expected_version


@dataclass
class RuntimeMigrationResult:
    """Captured by ``RuntimeMigrationPhase``.

    ``read_probes`` is one entry per seeded URN re-read after the upgrade
    completes. ``write_probes`` is one entry per fresh write issued to
    a disjoint URN namespace (``zdu-rt-{i}``) and verified by read-back.
    Phase 10 consumes ``passed_read_count`` / ``passed_write_count`` to
    decide if the runtime mutator chain is operating correctly.
    """

    read_probes: list[RuntimeMigrationProbe] = field(default_factory=list)
    write_probes: list[RuntimeMigrationProbe] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def passed_read_count(self) -> int:
        return sum(1 for p in self.read_probes if p.passed)

    @property
    def passed_write_count(self) -> int:
        return sum(1 for p in self.write_probes if p.passed)


@dataclass
class PrepareOldStackResult:
    """Captured by ``PrepareOldStackPhase`` when ``ZDU_SKIP_PREPARE_OLD_STACK`` is unset.

    ``current_images`` is the {service: image_string} snapshot taken
    BEFORE any restart. ``recreated_services`` is the list of services
    that were actually restarted (subset of ``services_inspected``).
    """

    old_image_tag: str = ""
    current_images: dict[str, str] = field(default_factory=dict)
    services_inspected: list[str] = field(default_factory=list)
    recreated_services: list[str] = field(default_factory=list)
    health_check_passed: bool = False
    duration_s: float = 0.0


@dataclass
class ImageBuildResult:
    """Captured by ``BuildImagesPhase`` when ``ZDU_BUILD_IMAGES=1``.

    ``old_ref`` / ``new_ref`` are the resolved git refs (typically a branch
    name like ``master`` or ``HEAD``). ``old_sha`` / ``new_sha`` are the
    short SHAs the framework derives from those refs. ``old_image_tag`` /
    ``new_image_tag`` are the actual Docker tag strings produced
    (``zdu-old-{sha8}`` / ``zdu-new-{sha8}``).

    ``cache_hit`` is True when ALL required images already existed locally
    with the computed tags — no build was performed. ``services_built`` is
    the list of Gradle service paths the phase invoked (or would have, if
    not for the cache).
    """

    old_ref: str = "master"
    new_ref: str = "HEAD"
    old_sha: str = ""
    new_sha: str = ""
    old_image_tag: str = ""
    new_image_tag: str = ""
    cache_hit: bool = False
    services_built: list[str] = field(default_factory=list)
    duration_s: float = 0.0


@dataclass
class RollingRestartResult:
    """Captured by ``RollingRestartPhase`` after sequenced GMS → MAE → MCE swap.

    ``services_restarted`` is the ordered list of compose services that were
    successfully recreated and reported healthy. ``dual_write_start_times``
    maps physical-index name → T1 epoch ms, parsed from the MAE log line
    ``Recorded dual-write start time for index '{x}' (entity '{y}'): {ts}``.
    Used by Phase 7 (InjectTrafficDual) and Phase 10 (Validation) to assert
    dual-write fan-out behaviour and the ``[T0, T1]`` window.
    """

    services_restarted: list[str] = field(default_factory=list)
    dual_write_start_times: dict[str, int] = field(default_factory=dict)
    duration_s: float = 0.0


@dataclass
class ValidationResult:
    tc_number: int
    name: str
    status: Literal["PASS", "FAIL", "XFAIL", "XPASS", "SKIP"]
    expected_to_fail: bool
    actual_result: str
    failure_reason: str | None = None


@dataclass
class TestContext:
    gms_url: str = "http://localhost:8080"

    # BuildImagesPhase writes (Phase 0 — opt-in via ZDU_BUILD_IMAGES=1)
    image_build: ImageBuildResult | None = None

    # PrepareOldStackPhase writes (Phase 0.5)
    prepare_old_stack: PrepareOldStackResult | None = None

    # DiscoveryPhase writes
    version_snapshot: dict[str, str] = field(default_factory=dict)

    # SeedPhase writes
    seeded_entities: list[SeededEntity] = field(default_factory=list)

    # SnapshotT0Phase writes
    snapshot_t0: SnapshotT0 | None = None

    # UpgradeBlockingPhase writes
    upgrade_blocking: UpgradeBlockingResult | None = None

    # UpgradeBlockingReRunPhase writes — captures second SystemUpdateBlocking
    # invocation; consumed by TC-108 validator.
    upgrade_blocking_rerun: UpgradeBlockingReRunResult | None = None

    # SnapshotT1Phase writes — re-queries each ``ctx.snapshot_t0`` index via
    # its alias after upgrade_blocking_rerun completes. Consumed by TC-109
    # validator to assert no doc loss across the upgrade.
    snapshot_t1: SnapshotT0 | None = None

    # DataIntegritySnapshotPhase writes — post-Phase-10 ES presence check
    # for gap_urns + dual_write_urns. Consumed by Suite D TC-201 and TC-204
    # validators to assert no URN was lost across the rolling-restart +
    # catch-up window.
    data_integrity_snapshot: DataIntegritySnapshot | None = None

    # Tc112FaultInjectionPhase writes — engineered doc-count mismatch run.
    # Phase is currently NOT wired into the runner (see plan 19 — superseded
    # by the simpler t0/t1 doc-count preservation design for TC-109). Field
    # remains for the future fault-injection plan.
    tc112_fault_injection: Tc112FaultInjectionResult | None = None

    # InjectTrafficPrePhase writes
    gap_urns: list[str] = field(default_factory=list)

    # RollingRestartPhase writes
    rolling_restart: RollingRestartResult | None = None

    # InjectTrafficDualPhase writes
    dual_write_urns: list[str] = field(default_factory=list)

    # UpgradeNonBlockingPhase writes
    upgrade_nonblocking: UpgradeNonBlockingResult | None = None
    sweep_events: Queue = field(default_factory=Queue)
    io_observations: list[IOObservation] = field(default_factory=list)
    io_write_results: list[IOWriteResult] = field(default_factory=list)
    sweep_total_migrated: int = 0
    # Dedicated entities for concurrent-write testing (separate from scenario entities)
    io_pool_entities: list[SeededEntity] = field(default_factory=list)

    # RuntimeMigrationPhase writes
    runtime_migration: RuntimeMigrationResult | None = None

    # ValidationPhase writes
    validation_results: list[ValidationResult] = field(default_factory=list)

    # Runner caches the active scenario list here so Suite B's TC-102 can
    # look up TC-101's expected_reindex_indices at validation time without
    # adding a back-reference from the scenario loader. Typed as Any to
    # avoid a circular import (scenario_loader → context → scenario_loader).
    all_scenarios: list[Any] = field(default_factory=list)
