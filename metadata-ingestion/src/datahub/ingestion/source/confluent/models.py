from typing import Dict, List, Mapping, Optional, Sequence, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

# GraphQL's JsonPrimitive scalar.
BusinessMetadataValue = Union[str, bool, int, float]


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

    # The catalog returns `null` rather than `[]` for unset collections.
    @field_validator("tags", "business_metadata", mode="before")
    @classmethod
    def default_empty_collection(cls, value: object) -> object:
        return value or []

    def properties_from_business_metadata(self) -> Dict[str, str]:
        return {
            attribute.name: str(attribute.value)
            for attribute in self.business_metadata
            if attribute.name and attribute.value is not None
        }


CatalogEntityType = TypeVar("CatalogEntityType", bound=CatalogEntity)


def index_by_name(
    entities: Sequence[CatalogEntityType],
) -> Dict[str, CatalogEntityType]:
    return {entity.name: entity for entity in entities if entity.name}


def lookup_by_name(
    index: Mapping[str, CatalogEntityType], name: str
) -> Optional[CatalogEntityType]:
    # The catalog's copy of a name can differ in case from the source's.
    entity = index.get(name)
    if entity is not None:
        return entity

    lowered = name.lower()
    for candidate_name, candidate in index.items():
        if candidate_name.lower() == lowered:
            return candidate
    return None
