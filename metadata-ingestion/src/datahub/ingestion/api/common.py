from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

T = TypeVar("T")


@dataclass
class RecordEnvelope(Generic[T]):
    record: T
    metadata: dict


@dataclass
class _WorkUnitId(metaclass=ABCMeta):
    id: str


# For information on why the WorkUnit class is structured this way
# and is separating the dataclass portion from the abstract methods, see
# https://github.com/python/mypy/issues/5374#issuecomment-568335302.
class WorkUnit(_WorkUnitId, metaclass=ABCMeta):
    @abstractmethod
    def get_metadata(self) -> dict:
        pass


class PipelineContext:
    def __init__(
        self,
        run_id: str,
        datahub_api: Optional[DatahubClientConfig] = None,
        pipeline_name: Optional[str] = None,
        dry_run: bool = False,
        preview_mode: bool = False,
    ) -> None:
        self.run_id = run_id
        self.graph = DataHubGraph(datahub_api) if datahub_api is not None else None
        self.pipeline_name = pipeline_name
        self.dry_run_mode = dry_run
        self.preview_mode = preview_mode
