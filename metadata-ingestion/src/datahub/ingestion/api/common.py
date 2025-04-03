from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Generic, Iterable, Optional, Tuple, TypeVar

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import set_dataset_urn_to_lower
from datahub.ingestion.api.committable import Committable
from datahub.ingestion.graph.client import DataHubGraph

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
        graph: Optional[DataHubGraph] = None,
        pipeline_name: Optional[str] = None,
        dry_run: bool = False,
        preview_mode: bool = False,
        pipeline_config: Optional["PipelineConfig"] = None,
    ) -> None:
        self.pipeline_config = pipeline_config
        self.graph = graph
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.dry_run_mode = dry_run
        self.preview_mode = preview_mode
        self.checkpointers: Dict[str, Committable] = {}

        self._set_dataset_urn_to_lower_if_needed()

    @property
    def flags(self) -> "FlagsConfig":
        return self.pipeline_config.flags if self.pipeline_config else FlagsConfig()

    def _set_dataset_urn_to_lower_if_needed(self) -> None:
        # TODO: Get rid of this function once lower-casing is the standard.
        if self.graph:
            server_config = self.graph.get_config()
            if server_config and server_config.get("datasetUrnNameCasing") is True:
                set_dataset_urn_to_lower(True)

    def register_checkpointer(self, committable: Committable) -> None:
        if committable.name in self.checkpointers:
            raise IndexError(
                f"Checkpointing provider {committable.name} already registered."
            )
        self.checkpointers[committable.name] = committable

    def get_committables(self) -> Iterable[Tuple[str, Committable]]:
        yield from self.checkpointers.items()

    def require_graph(self, operation: Optional[str] = None) -> DataHubGraph:
        if not self.graph:
            raise ConfigurationError(
                f"{operation or 'This operation'} requires a graph, but none was provided. "
                "To provide one, either use the datahub-rest sink or set the top-level datahub_api config in the recipe."
            )
        return self.graph


class FlagsConfig(ConfigModel):
    """Experimental flags for the ingestion pipeline.

    As ingestion flags an experimental feature, we do not guarantee backwards compatibility.
    Use at your own risk!
    """

    generate_browse_path_v2: bool = Field(
        default=True,
        description="Generate BrowsePathsV2 aspects from container hierarchy and existing BrowsePaths aspects.",
    )

    generate_browse_path_v2_dry_run: bool = Field(
        default=False,
        description=(
            "Run through browse paths v2 generation but do not actually write the aspects to DataHub. "
            "Requires `generate_browse_path_v2` to also be enabled."
        ),
    )

    generate_memory_profiles: Optional[str] = Field(
        default=None,
        description=(
            "Generate memray memory dumps for ingestion process by providing a path to write the dump file in."
        ),
    )

    set_system_metadata: bool = Field(
        True, description="Set system metadata on entities."
    )
    set_system_metadata_pipeline_name: bool = Field(
        True,
        description="Set system metadata pipeline name. Requires `set_system_metadata` to be enabled.",
    )
