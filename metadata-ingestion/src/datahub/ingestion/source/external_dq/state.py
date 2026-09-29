from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

import pydantic

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)

# ASCII unit separator: cannot appear in normal identifiers, so keys never collide.
_KEY_SEPARATOR = "\x1f"

EXTERNAL_DQ_JOB_ID = JobId("external_dq_results")


def run_key(rule_id: str, run_id: str) -> str:
    return f"{rule_id}{_KEY_SEPARATOR}{run_id}"


class ExternalDQCheckpointState(CheckpointStateBase):
    """Per results table: the highest executed_at emitted (epoch millis), the
    run keys emitted from the next window start on, so re-reading that window
    never re-emits a run event (each re-emit re-fires notifications), where the
    next read starts, and [next window start, expected row count below it] to
    detect rows that landed too late to ever be read."""

    watermarks: Dict[str, int] = pydantic.Field(default_factory=dict)
    recent_keys: Dict[str, Dict[str, int]] = pydantic.Field(default_factory=dict)
    late_baselines: Dict[str, List[int]] = pydantic.Field(default_factory=dict)
    next_starts: Dict[str, int] = pydantic.Field(default_factory=dict)


@dataclass(frozen=True)
class LoadedState:
    watermark: Optional[int]
    recent: Dict[str, int]
    late_baseline: Optional[List[int]]
    next_start: Optional[int] = None


@dataclass(frozen=True)
class ReadWindow:
    start_millis: int
    seen: FrozenSet[str]


def plan_window(
    *,
    last_watermark: Optional[int],
    last_next_start: Optional[int],
    last_recent: Mapping[str, int],
    now_millis: int,
    initial_lookback_ms: int,
    overlap_ms: int,
) -> ReadWindow:
    if last_watermark is None:
        return ReadWindow(
            start_millis=now_millis - initial_lookback_ms, seen=frozenset()
        )
    start = last_watermark - overlap_ms
    if last_next_start is not None:
        # `recent` only covers rows from the previous next start on. Reading
        # earlier (e.g. after raising late_arrival_minutes) would re-emit results
        # that were already published.
        start = max(start, last_next_start)
    return ReadWindow(start_millis=start, seen=frozenset(last_recent))


def advance(
    *,
    last_watermark: Optional[int],
    last_recent: Mapping[str, int],
    observed: Mapping[str, int],
    overlap_ms: int,
    now_millis: int,
    hold_millis: Optional[int] = None,
) -> Tuple[Optional[int], Dict[str, int]]:
    timestamps = list(observed.values())
    if last_watermark is not None:
        timestamps.append(last_watermark)
    if not timestamps:
        return None, {}
    # Capped at now: a producer whose clock runs ahead must not shrink the late
    # window for every other result.
    watermark = min(max(timestamps), now_millis)
    if hold_millis is not None:
        # The next window starts at watermark - overlap, so this keeps the oldest
        # unresolved result inside it.
        watermark = min(watermark, hold_millis + overlap_ms)
    cutoff = watermark - overlap_ms
    merged = {**last_recent, **observed}
    return watermark, {key: ts for key, ts in merged.items() if ts >= cutoff}


class ExternalDQStateHandler(
    StatefulIngestionUsecaseHandlerBase[ExternalDQCheckpointState]
):
    def __init__(
        self,
        *,
        state_provider: StateProviderWrapper,
        pipeline_name: Optional[str],
        run_id: str,
        ignore_new_state: bool = False,
    ) -> None:
        self.state_provider = state_provider
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.ignore_new_state = ignore_new_state
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @property
    def job_id(self) -> JobId:
        return EXTERNAL_DQ_JOB_ID

    def is_checkpointing_enabled(self) -> bool:
        return (
            self.state_provider.is_stateful_ingestion_configured()
            and not self.ignore_new_state
        )

    def create_checkpoint(self) -> Optional[Checkpoint[ExternalDQCheckpointState]]:
        if not self.is_checkpointing_enabled():
            return None
        assert self.pipeline_name is not None
        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=ExternalDQCheckpointState(),
        )

    def load(self, table: str) -> LoadedState:
        last = self.state_provider.get_last_checkpoint(
            self.job_id, ExternalDQCheckpointState
        )
        if not last or not last.state:
            return LoadedState(None, {}, None)
        assert isinstance(last.state, ExternalDQCheckpointState)
        state = last.state
        loaded = LoadedState(
            watermark=state.watermarks.get(table),
            recent=dict(state.recent_keys.get(table, {})),
            late_baseline=state.late_baselines.get(table),
            next_start=state.next_starts.get(table),
        )
        if loaded.watermark is not None:
            # Carry forward now, so a run that fails before save() keeps the
            # previous watermark instead of re-reading the initial lookback.
            self.save(
                table,
                loaded.watermark,
                loaded.recent,
                loaded.late_baseline,
                next_start=loaded.next_start,
            )
        return loaded

    def save(
        self,
        table: str,
        watermark: int,
        recent: Mapping[str, int],
        late_baseline: Optional[Sequence[int]],
        *,
        next_start: Optional[int],
    ) -> None:
        current = self.state_provider.get_current_checkpoint(self.job_id)
        if current is None:
            return
        assert isinstance(current.state, ExternalDQCheckpointState)
        state = current.state
        state.watermarks[table] = watermark
        state.recent_keys[table] = dict(recent)
        # None keeps the carried-forward baseline: it stays valid while the
        # window start is unchanged, since the table is append-only.
        if late_baseline is not None:
            state.late_baselines[table] = list(late_baseline)
        if next_start is not None:
            state.next_starts[table] = next_start
