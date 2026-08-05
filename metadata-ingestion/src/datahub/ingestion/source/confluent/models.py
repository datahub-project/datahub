from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Generic, List, Optional, Sequence, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

# GraphQL's JsonPrimitive scalar.
BusinessMetadataValue = Union[str, bool, int, float]


def empty_if_null(value: object) -> object:
    # Catalog returns null rather than [] for unset collections.
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


@dataclass(frozen=True)
class NameIndex(Generic[CatalogEntityType]):
    # Exact-name duplicates go in `ambiguous`. Names that only collide when
    # lowercased go in `case_ambiguous` so case-insensitive lookup cannot pick
    # a winner silently; exact-case lookups still use `by_name`.
    by_name: Dict[str, CatalogEntityType]
    ambiguous: Dict[str, List[CatalogEntityType]]
    case_ambiguous: Dict[str, List[CatalogEntityType]] = field(default_factory=dict)
    empty_name_count: int = 0
    _by_lowered_name: Dict[str, CatalogEntityType] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_by_lowered_name",
            {
                name.lower(): entity
                for name, entity in self.by_name.items()
                if name.lower() not in self.case_ambiguous
            },
        )

    def get(self, name: str) -> Optional[CatalogEntityType]:
        entity = self.by_name.get(name)
        if entity is not None:
            return entity
        return self._by_lowered_name.get(name.lower())


def index_by_name(
    entities: Sequence[CatalogEntityType],
) -> NameIndex[CatalogEntityType]:
    grouped: Dict[str, List[CatalogEntityType]] = defaultdict(list)
    empty_name_count = 0
    for entity in entities:
        if entity.name:
            grouped[entity.name].append(entity)
        else:
            empty_name_count += 1

    by_name = {
        name: candidates[0]
        for name, candidates in grouped.items()
        if len(candidates) == 1
    }
    ambiguous = {
        name: candidates for name, candidates in grouped.items() if len(candidates) > 1
    }

    lowered_groups: Dict[str, List[CatalogEntityType]] = defaultdict(list)
    for entity in by_name.values():
        lowered_groups[entity.name.lower()].append(entity)
    case_ambiguous = {
        lowered: candidates
        for lowered, candidates in lowered_groups.items()
        if len(candidates) > 1
    }

    return NameIndex(
        by_name=by_name,
        ambiguous=ambiguous,
        case_ambiguous=case_ambiguous,
        empty_name_count=empty_name_count,
    )
