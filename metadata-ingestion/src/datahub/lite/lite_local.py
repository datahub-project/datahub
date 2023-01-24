from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Dict, Generic, Iterable, List, Optional, Type, TypeVar, Union, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import MetadataChangeEventClass, _Aspect
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


class DataHubLiteLocal(Generic[LiteConfig], Closeable, metaclass=ABCMeta):
    """The embedded version of DataHub Lite. All DataHub Lite implementations need to extend this class"""

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
    ) -> None:
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
        aspects: List[str] = [],
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
