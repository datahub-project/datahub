from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import (
    Annotated,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import LiteralString, TypeAliasType

from datahub.ingestion.api.source import SourceReport

BusinessMetadataValue = Union[str, bool, int, float]

BM_COLLISION_WITH_CONNECTOR_CONFIG: LiteralString = (
    "Ignoring Stream Catalog business metadata attributes whose names collide with "
    "connector config properties. Rename the attributes in Confluent to emit them."
)
BM_COLLISION_WITH_BROKER_TOPIC_PROPERTIES: LiteralString = (
    "Ignoring Stream Catalog business metadata attributes whose names collide with "
    "topic properties read from the broker. Rename the attributes in Confluent to emit them."
)
BM_DUPLICATE_ATTRIBUTE_NAMES: LiteralString = "Stream Catalog entity has duplicate business metadata attribute names; later values win"


def empty_if_null(value: object) -> object:
    # Catalog returns null rather than [] for unset collections.
    return [] if value is None else value


_T = TypeVar("_T")
NullAsEmptyList = TypeAliasType(
    "NullAsEmptyList",
    Annotated[List[_T], BeforeValidator(empty_if_null)],
    type_params=(_T,),
)


class CatalogModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class CatalogBusinessMetadataAttribute(CatalogModel):
    name: str
    value: Optional[BusinessMetadataValue] = None


class CatalogEntity(CatalogModel):
    name: str
    qualified_name: Optional[str] = Field(default=None, alias="qualifiedName")
    tags: NullAsEmptyList[str] = Field(default_factory=list)
    business_metadata: NullAsEmptyList[CatalogBusinessMetadataAttribute] = Field(
        default_factory=list
    )

    def properties_from_business_metadata(self) -> Dict[str, str]:
        return {
            attribute.name: str(attribute.value)
            for attribute in self.business_metadata
            if attribute.name and attribute.value is not None
        }

    def duplicate_business_metadata_names(self) -> List[str]:
        # Only names with multiple non-null values — null siblings do not overwrite.
        counts = Counter(
            attribute.name
            for attribute in self.business_metadata
            if attribute.name and attribute.value is not None
        )
        return sorted(name for name, count in counts.items() if count > 1)


CatalogEntityType = TypeVar("CatalogEntityType", bound=CatalogEntity)


def non_colliding_business_metadata(
    entity: CatalogEntity,
    existing: Mapping[str, str],
    report: SourceReport,
    collision_message: LiteralString,
    context: str,
) -> Dict[str, str]:
    duplicates = entity.duplicate_business_metadata_names()
    if duplicates:
        report.warning(
            message=BM_DUPLICATE_ATTRIBUTE_NAMES,
            context=f"{context}, attributes={duplicates}",
        )

    properties = entity.properties_from_business_metadata()
    if not properties:
        return {}

    collisions = sorted(properties.keys() & existing.keys())
    if collisions:
        report.warning(
            message=collision_message,
            context=f"{context}, attributes={collisions}",
        )
        properties = {
            name: value for name, value in properties.items() if name not in existing
        }
    return properties


@dataclass
class NameIndex(Generic[CatalogEntityType]):
    by_name: Dict[str, CatalogEntityType]
    ambiguous: Dict[str, List[CatalogEntityType]]
    empty_name_count: int = 0

    def get(self, name: str) -> Optional[CatalogEntityType]:
        # Exact match only: topic and connector names are case-sensitive, so a
        # case-insensitive fallback could attach one entity's metadata to another.
        return self.by_name.get(name)

    def report_issues(self, report: SourceReport, entity_label: str) -> None:
        if self.empty_name_count:
            report.warning(
                message="Skipped Stream Catalog entities that had an empty name",
                context=f"entity={entity_label}, count={self.empty_name_count}",
            )


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

    return NameIndex(
        by_name=by_name,
        ambiguous=ambiguous,
        empty_name_count=empty_name_count,
    )
