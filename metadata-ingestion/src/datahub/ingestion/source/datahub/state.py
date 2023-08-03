from datetime import datetime, timezone
from functools import cache
from typing import TYPE_CHECKING, Optional, cast

from pydantic.types import NonNegativeInt

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.use_case_handler import (
    StatefulIngestionUsecaseHandlerBase,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.datahub.datahub_source import DataHubSource


class DataHubIngestionState(CheckpointStateBase):
    mysql_createdon_ts: NonNegativeInt = 0
    kafka_offset: NonNegativeInt = 0

    @property
    def mysql_createdon_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.mysql_createdon_ts / 1000, tz=timezone.utc)


class StatefulDataHubIngestionHandler(
    StatefulIngestionUsecaseHandlerBase[DataHubIngestionState]
):
    def __init__(self, source: "DataHubSource"):
        self.state_provider = source.state_provider
        self.config = source.config.stateful_ingestion
        self.run_id = source.ctx.run_id
        self.pipeline_name = source.ctx.pipeline_name
        self.state_provider.register_stateful_ingestion_usecase_handler(self)

    @cache
    def is_checkpointing_enabled(self) -> bool:
        return self.state_provider.is_stateful_ingestion_configured()

    def get_last_run_state(self) -> DataHubIngestionState:
        if self.is_checkpointing_enabled() and not self.config.ignore_old_state:
            last_checkpoint = self.state_provider.get_last_checkpoint(
                self.job_id, DataHubIngestionState
            )
            if last_checkpoint and last_checkpoint.state:
                return last_checkpoint.state

        return DataHubIngestionState()

    def create_checkpoint(self) -> Optional[Checkpoint[DataHubIngestionState]]:
        if not self.is_checkpointing_enabled() or self.config.ignore_new_state:
            return None

        if self.pipeline_name is None:
            raise ValueError(
                "Pipeline name must be set to use stateful datahub ingestion"
            )

        return Checkpoint(
            job_name=self.job_id,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state=DataHubIngestionState(),
        )

    def update_checkpoint(
        self,
        *,
        last_createdon: Optional[datetime] = None,
        last_offset: Optional[int] = None,
    ) -> None:
        cur_checkpoint = self.state_provider.get_current_checkpoint(self.job_id)
        if cur_checkpoint:
            cur_state = cast(DataHubIngestionState, cur_checkpoint.state)
            if last_createdon:
                cur_state.mysql_createdon_ts = int(last_createdon.timestamp() * 1000)
            if last_offset:
                cur_state.kafka_offset = last_offset

    def commit_checkpoint(self) -> None:
        if self.state_provider.ingestion_checkpointing_state_provider:
            self.state_provider.prepare_for_commit()
            self.state_provider.ingestion_checkpointing_state_provider.commit()

    @property
    def job_id(self) -> JobId:
        return JobId("datahub_ingestion")
