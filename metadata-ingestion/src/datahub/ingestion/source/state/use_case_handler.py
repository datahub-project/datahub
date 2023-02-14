from abc import ABC, abstractmethod
from typing import Generic, Optional

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.state.checkpoint import Checkpoint, StateType


class StatefulIngestionUsecaseHandlerBase(ABC, Generic[StateType]):
    """
    Common base-class for all stateful ingestion use-case handlers.

    This forces all sources to follow a strict use-case driven stateful ingestion support,
    promoting more reuse of the existing handlers, and hence a lot less code per source
    to enable stateful ingestion use-cases.
    """

    @abstractmethod
    def create_checkpoint(self) -> Optional[Checkpoint[StateType]]:
        raise NotImplementedError("Sub-classes must override this method.")

    @abstractmethod
    def is_checkpointing_enabled(self) -> bool:
        raise NotImplementedError("Sub-classes must override this method.")

    @property
    @abstractmethod
    def job_id(self) -> JobId:
        raise NotImplementedError("Sub-classes must override this method.")

    # TODO: Consider generalizing other state management actions such as (1) state update
    # and (2) other state based actions such as stale-entity removal & redundant run skip etc
    # across use-cases.
