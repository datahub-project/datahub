from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Generic, Iterable, Optional, Tuple, TypeVar

import requests

from datahub.emitter.mce_builder import set_dataset_urn_to_lower
from datahub.ingestion.api.committable import Committable
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

if TYPE_CHECKING:
    from datahub.ingestion.run.pipeline import PipelineConfig

T = TypeVar("T")


@dataclass
class RecordEnvelope(Generic[T]):
    record: T
    metadata: dict


class ControlRecord:
    """A marker class to indicate records that are control signals from the framework"""

    pass


class EndOfStream(ControlRecord):
    """A marker class to indicate an end of stream"""

    pass


@dataclass
class WorkUnit(metaclass=ABCMeta):
    id: str

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
        pipeline_config: Optional["PipelineConfig"] = None,
    ) -> None:
        self.pipeline_config = pipeline_config
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.dry_run_mode = dry_run
        self.preview_mode = preview_mode
        self.reporters: Dict[str, Committable] = {}
        self.checkpointers: Dict[str, Committable] = {}
        try:
            self.graph = DataHubGraph(datahub_api) if datahub_api is not None else None
        except requests.exceptions.ConnectionError as e:
            raise Exception("Failed to connect to DataHub") from e
        except Exception as e:
            raise Exception(
                "Failed to instantiate a valid DataHub Graph instance"
            ) from e

        self._set_dataset_urn_to_lower_if_needed()

    def _set_dataset_urn_to_lower_if_needed(self) -> None:
        # TODO: Get rid of this function once lower-casing is the standard.
        if self.graph:
            server_config = self.graph.get_config()
            if server_config and server_config.get("datasetUrnNameCasing"):
                set_dataset_urn_to_lower(True)

    def register_checkpointer(self, committable: Committable) -> None:
        if committable.name in self.checkpointers:
            raise IndexError(
                f"Checkpointing provider {committable.name} already registered."
            )
        self.checkpointers[committable.name] = committable

    def register_reporter(self, committable: Committable) -> None:
        if committable.name in self.reporters:
            raise IndexError(
                f"Reporting provider {committable.name} already registered."
            )
        self.reporters[committable.name] = committable

    def get_reporters(self) -> Iterable[Committable]:
        yield from self.reporters.values()

    def get_committables(self) -> Iterable[Tuple[str, Committable]]:
        yield from self.reporters.items()
        yield from self.checkpointers.items()
