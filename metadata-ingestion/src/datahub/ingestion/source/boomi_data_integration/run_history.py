import heapq
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.ingestion.source.boomi_data_integration.constants import (
    MAX_ERROR_DESCRIPTION_LENGTH,
    PROP_ERROR_DESCRIPTION,
    PROP_RPU,
    PROP_RUN_GROUP_ID,
    PROP_RUN_STATUS,
    PROP_RUN_TARGET_NAME,
    PROP_SUB_RIVER_ID,
    PROP_TABLE_COUNT,
    PROP_TRIGGER_NAME,
    PROP_UNITS,
    RUN_GROUP_PRUNE_FACTOR,
    RUN_STATUS_RESULTS,
    RUN_STATUSES_IN_PROGRESS,
    RUN_STATUSES_SKIPPED,
    SCHEDULED_TRIGGERS,
    VALUE_SEPARATOR,
    RiverType,
    RunStatus,
)
from datahub.ingestion.source.boomi_data_integration.models import RiverRun
from datahub.metadata.schema_classes import DataProcessTypeClass

KNOWN_RUN_STATUSES: Set[str] = set(RUN_STATUS_RESULTS) | set(RUN_STATUSES_IN_PROGRESS)


# One river run. A multi-table river returns a row per table, all sharing a
# run_group_id, and those rows are folded into one run.
@dataclass
class RunGroup:
    group_id: str
    run_group_id: Optional[str]
    start_millis: int
    end_millis: Optional[int]
    statuses: Set[str] = field(default_factory=set)
    run_ids: Set[str] = field(default_factory=set)
    targets: Set[str] = field(default_factory=set)
    sub_river_ids: Set[str] = field(default_factory=set)
    units: Optional[float] = None
    rpu: Optional[float] = None
    trigger_name: Optional[str] = None
    root_trigger_name: Optional[str] = None
    triggered_by_run_id: Optional[str] = None
    error_description: Optional[str] = None

    @classmethod
    def from_run(cls, run: RiverRun) -> "RunGroup":
        chain = run.trigger_chain or []
        group = cls(
            group_id=run.group_id,
            run_group_id=run.run_group_id,
            start_millis=run.start_date_in_milliseconds,
            end_millis=run.end_date_in_milliseconds,
            trigger_name=run.trigger_name,
            # The chain lists triggers oldest first, so its head is the act
            # that started the whole run tree.
            root_trigger_name=chain[0].trigger_name if chain else None,
            triggered_by_run_id=run.triggered_by_run_id
            or (chain[-1].run_id if chain else None),
        )
        group.add(run)
        return group

    def add(self, run: RiverRun) -> None:
        self.start_millis = min(self.start_millis, run.start_date_in_milliseconds)
        if run.end_date_in_milliseconds is not None:
            self.end_millis = max(
                self.end_millis or run.end_date_in_milliseconds,
                run.end_date_in_milliseconds,
            )
        status = run.status.lower()
        self.statuses.add(status)
        self.run_ids.add(run.run_id)
        # Logic runs name the target "Logic" rather than a table.
        if run.target_name and run.datasource_id != RiverType.LOGIC:
            self.targets.add(run.target_name)
        if run.is_sub_river_run and run.sub_river_id:
            self.sub_river_ids.add(run.sub_river_id)
        if run.units is not None:
            self.units = (self.units or 0.0) + run.units
        if run.rpu is not None:
            self.rpu = (self.rpu or 0.0) + run.rpu
        # A failed table's error explains the run better than any other.
        if run.error_description and (
            self.error_description is None or status == RunStatus.FAILED
        ):
            self.error_description = run.error_description

    @property
    def unknown_statuses(self) -> Set[str]:
        return self.statuses - KNOWN_RUN_STATUSES

    @property
    def result(self) -> Optional[InstanceRunResult]:
        # None means the run gets a start event only: a table is still pending
        # or running, or a status is not one the connector knows.
        if self.statuses & RUN_STATUSES_IN_PROGRESS:
            return None
        if RunStatus.FAILED in self.statuses:
            return InstanceRunResult.FAILURE
        if self.unknown_statuses:
            return None
        if self.statuses <= RUN_STATUSES_SKIPPED:
            return InstanceRunResult.SKIPPED
        return InstanceRunResult.SUCCESS

    @property
    def status(self) -> str:
        return VALUE_SEPARATOR.join(sorted(self.statuses))

    @property
    def run_type(self) -> str:
        # A run started by a logic river inherits how that logic run started.
        trigger = (self.root_trigger_name or self.trigger_name or "").lower()
        if trigger in SCHEDULED_TRIGGERS:
            return DataProcessTypeClass.BATCH_SCHEDULED
        return DataProcessTypeClass.BATCH_AD_HOC

    def properties(self) -> Dict[str, str]:
        candidates: Dict[str, Optional[str]] = {
            PROP_RUN_STATUS: self.status,
            PROP_TRIGGER_NAME: self.trigger_name,
            PROP_UNITS: _number(self.units),
            PROP_RPU: _number(self.rpu),
            PROP_ERROR_DESCRIPTION: (
                self.error_description[:MAX_ERROR_DESCRIPTION_LENGTH]
                if self.error_description
                else None
            ),
            PROP_RUN_TARGET_NAME: (
                next(iter(self.targets)) if len(self.targets) == 1 else None
            ),
            PROP_TABLE_COUNT: (
                str(len(self.targets)) if len(self.targets) > 1 else None
            ),
            PROP_RUN_GROUP_ID: self.run_group_id,
            PROP_SUB_RIVER_ID: VALUE_SEPARATOR.join(sorted(self.sub_river_ids)),
        }
        return {key: value for key, value in candidates.items() if value}


def _number(value: Optional[float]) -> Optional[str]:
    return None if value is None else f"{value:.12g}"


@dataclass
class RunSelection:
    # Most recent first.
    groups: List[RunGroup] = field(default_factory=list)
    # Both count runs, so a multi-table run counts once.
    scanned: int = 0
    over_cap: int = 0


def _group_rank(group: RunGroup) -> Tuple[int, str]:
    return group.start_millis, group.group_id


def select_recent_runs(runs: Iterable[RiverRun], max_runs: int) -> RunSelection:
    # Rows are folded into their group before capping, because the rows of one
    # multi-table run can be split across pages. Only the most recent groups
    # are kept while folding; a dropped group is remembered by ID so its later
    # rows are ignored instead of starting a partial run.
    kept: Dict[str, RunGroup] = {}
    dropped: Set[str] = set()
    prune_at = max_runs * RUN_GROUP_PRUNE_FACTOR
    for run in runs:
        group_id = run.group_id
        if group_id in dropped:
            continue
        group = kept.get(group_id)
        if group is not None:
            group.add(run)
            continue
        kept[group_id] = RunGroup.from_run(run)
        if len(kept) > prune_at:
            survivors = heapq.nlargest(max_runs, kept.values(), key=_group_rank)
            survivor_ids = {g.group_id for g in survivors}
            dropped.update(gid for gid in kept if gid not in survivor_ids)
            kept = {g.group_id: g for g in survivors}
    groups = heapq.nlargest(max_runs, kept.values(), key=_group_rank)
    scanned = len(kept) + len(dropped)
    return RunSelection(groups=groups, scanned=scanned, over_cap=scanned - len(groups))
