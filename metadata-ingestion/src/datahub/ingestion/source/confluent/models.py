from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Generic, List, Optional, Sequence, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

# GraphQL's JsonPrimitive scalar.
BusinessMetadataValue = Union[str, bool, int, float]


def empty_if_null(value: object) -> object:
    # The catalog returns `null` rather than `[]` for unset collections.
    return value or []


class CatalogModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class CatalogBusinessMetadataAttribute(CatalogModel):
    name: str
    value: Optional[BusinessMetadataValue] = None


class CatalogEntity(CatalogModel):
    name: str
    qualified_name: Optional[str] = Field(default=None, alias="qualifiedName")
    tags: List[str] = Field(default_factory=list)
    business_metadata: List[CatalogBusinessMetadataAttribute] = Field(
        default_factory=list
    )

    @field_validator("tags", "business_metadata", mode="before")
    @classmethod
    def default_empty_collection(cls, value: object) -> object:
        return empty_if_null(value)

    def properties_from_business_metadata(self) -> Dict[str, str]:
        return {
            attribute.name: str(attribute.value)
            for attribute in self.business_metadata
            if attribute.name and attribute.value is not None
        }


CatalogEntityType = TypeVar("CatalogEntityType", bound=CatalogEntity)


# A plain dataclass rather than a pydantic model: a generic BaseModel would re-validate
# the entities against the TypeVar's bound and strip subclass fields.
@dataclass
class NameIndex(Generic[CatalogEntityType]):
    """
    Catalog names are not guaranteed unique, so a name the catalog reports more than
    once is held back in `ambiguous` instead of resolving last-write-wins to whichever
    entity happened to be paged in last.
    """

    by_name: Dict[str, CatalogEntityType]
    ambiguous: Dict[str, List[CatalogEntityType]]
    _by_lowered_name: Dict[str, CatalogEntityType] = field(init=False)

    def __post_init__(self) -> None:
        # The catalog's copy of a name can differ in case from the source's. Index once
        # rather than scanning every entity on each miss.
        self._by_lowered_name = {
            name.lower(): entity for name, entity in self.by_name.items()
        }

    def get(self, name: str) -> Optional[CatalogEntityType]:
        entity = self.by_name.get(name)
        if entity is not None:
            return entity
        return self._by_lowered_name.get(name.lower())


def index_by_name(
    entities: Sequence[CatalogEntityType],
) -> NameIndex[CatalogEntityType]:
    grouped: Dict[str, List[CatalogEntityType]] = defaultdict(list)
    for entity in entities:
        if entity.name:
            grouped[entity.name].append(entity)

    return NameIndex(
        by_name={
            name: candidates[0]
            for name, candidates in grouped.items()
            if len(candidates) == 1
        },
        ambiguous={
            name: candidates
            for name, candidates in grouped.items()
            if len(candidates) > 1
        },
    )
