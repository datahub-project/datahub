from dataclasses import dataclass
from typing import Dict, FrozenSet, Mapping, Optional, Tuple

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
    """Per results table: the highest executed_at emitted (epoch millis) and the
    run keys emitted inside the late-arrival overlap window, so re-reading that
    window never re-emits a run event (each re-emit re-fires notifications)."""

    watermarks: Dict[str, int] = pydantic.Field(default_factory=dict)
    recent_keys: Dict[str, Dict[str, int]] = pydantic.Field(default_factory=dict)


@dataclass(frozen=True)
class ReadWindow:
    start_millis: int
    seen: FrozenSet[str]


def plan_window(
    *,
    last_watermark: Optional[int],
    last_recent: Mapping[str, int],
    now_millis: int,
    initial_lookback_ms: int,
    overlap_ms: int,
) -> ReadWindow:
    if last_watermark is None:
        return ReadWindow(
            start_millis=now_millis - initial_lookback_ms, seen=frozenset()
        )
    return ReadWindow(
        start_millis=last_watermark - overlap_ms, seen=frozenset(last_recent)
    )


def advance(
    *,
    last_watermark: Optional[int],
    last_recent: Mapping[str, int],
    observed: Mapping[str, int],
    overlap_ms: int,
) -> Tuple[Optional[int], Dict[str, int]]:
    timestamps = list(observed.values())
    if last_watermark is not None:
        timestamps.append(last_watermark)
    if not timestamps:
        return None, {}
    watermark = max(timestamps)
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

    def load(self, table: str) -> Tuple[Optional[int], Dict[str, int]]:
        last = self.state_provider.get_last_checkpoint(
            self.job_id, ExternalDQCheckpointState
        )
        if not last or not last.state:
            return None, {}
        assert isinstance(last.state, ExternalDQCheckpointState)
        state = last.state
        watermark = state.watermarks.get(table)
        recent = dict(state.recent_keys.get(table, {}))
        if watermark is not None:
            # Carry forward now, so a run that fails before save() keeps the
            # previous watermark instead of re-reading the initial lookback.
            self.save(table, watermark, recent)
        return watermark, recent

    def save(self, table: str, watermark: int, recent: Mapping[str, int]) -> None:
        current = self.state_provider.get_current_checkpoint(self.job_id)
        if current is None:
            return
        assert isinstance(current.state, ExternalDQCheckpointState)
        state = current.state
        state.watermarks[table] = watermark
        state.recent_keys[table] = dict(recent)
