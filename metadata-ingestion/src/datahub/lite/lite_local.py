from abc import ABCMeta, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Generic, Iterable, List, Optional, Type, TypeVar, Union, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    SystemMetadataClass,
    _Aspect,
)
from datahub.utilities.type_annotations import get_class_from_annotation

LiteConfig = TypeVar("LiteConfig", bound=ConfigModel)


class AutoComplete(ConfigModel):
    success_path: str
    failed_token: str
    suggested_path: str


class Browseable(ConfigModel):
    id: str
    name: str
    leaf: bool = False
    parents: Optional[List[str]] = None
    auto_complete: Optional[AutoComplete] = None


class Searchable(ConfigModel):
    id: str
    aspect: Optional[str]
    snippet: Optional[str]


class SearchFlavor(Enum):
    FREE_TEXT = "free-text"
    EXACT = "exact"


class PathNotFoundException(Exception):
    pass


@dataclass(frozen=True)
class AspectVersion:
    """A detached copy of a numbered aspect row and its original metadata.

    The typed value and system metadata are mutable; changing them does not
    modify the persisted history or other reads.
    """

    value: _Aspect
    version: int
    system_metadata: SystemMetadataClass
    createdon: int


class LiteVersionConflict(ValueError):
    """A conditional write failed; -1 denotes an absent aspect."""

    def __init__(self, expected_version: int, current_version: int) -> None:
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(
            f"Expected aspect version {expected_version}, current version {current_version}"
        )


class LiteTransactionError(RuntimeError):
    """A transaction cannot commit because an earlier write failed."""


class DataHubLiteLocal(Generic[LiteConfig], Closeable, metaclass=ABCMeta):
    """Embedded DataHub Lite. Instances are single-threaded, including transactions."""

    def __init__(self, config: LiteConfig):
        pass

    @classmethod
    def get_config_class(cls) -> Type[LiteConfig]:
        config_class = get_class_from_annotation(cls, DataHubLiteLocal, ConfigModel)
        assert config_class, "DataHubLiteLocal subclasses must define a config class"
        return cast(Type[LiteConfig], config_class)

    @abstractmethod
    def location(self) -> str:
        pass

    @abstractmethod
    def destroy(self) -> None:
        pass

    @abstractmethod
    def write(
        self,
        record: Union[
            MetadataChangeEventClass,
            MetadataChangeProposalWrapper,
        ],
        *,
        if_version_match: Optional[int] = None,
    ) -> Dict[str, int]:
        """Atomically write a record and return its aspect versions.

        Only MCPs accept a version condition; -1 requires an absent aspect.
        Versions returned inside a transaction are provisional until it commits.
        """
        pass

    @abstractmethod
    def transaction(self) -> AbstractContextManager[None]:
        """Group writes atomically. Nesting and forwarding are unsupported.

        A failed write or database statement makes the block rollback-only, even
        if its exception is caught. Reindexing, closing and destroying the store
        are forbidden inside the block. Reads see the block's writes.
        """
        pass

    @abstractmethod
    def get_aspect(
        self, urn: str, aspect_name: str, version: Optional[int] = None
    ) -> Optional[AspectVersion]:
        """Read an immutable numbered snapshot; None selects the latest.

        Explicit versions must be positive. Missing versions return None. For
        live observation metadata, use get(details=True) instead.
        """
        pass

    @abstractmethod
    def list_versions(self, urn: str, aspect_name: str) -> List[AspectVersion]:
        """Return numbered snapshots in ascending version order."""
        pass

    @abstractmethod
    def list_ids(self) -> Iterable[str]:
        pass

    @abstractmethod
    def get(
        self,
        id: str,
        aspects: Optional[List[str]],
        typed: bool = False,
        as_of: Optional[int] = None,
        details: Optional[bool] = False,
    ) -> Optional[Dict[str, Union[str, dict, _Aspect]]]:
        pass

    @abstractmethod
    def search(
        self,
        query: str,
        flavor: SearchFlavor,
        aspects: Optional[List[str]] = None,
        snippet: bool = True,
    ) -> Iterable[Searchable]:
        pass

    @abstractmethod
    def ls(self, path: str) -> List[Browseable]:
        pass

    @abstractmethod
    def get_all_entities(
        self, typed: bool = False
    ) -> Iterable[Dict[str, Union[dict, _Aspect]]]:
        pass

    @abstractmethod
    def get_all_aspects(self) -> Iterable[MetadataChangeProposalWrapper]:
        pass

    @abstractmethod
    def reindex(self) -> None:
        pass
