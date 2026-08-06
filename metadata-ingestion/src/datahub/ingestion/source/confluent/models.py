from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import LiteralString, TypeAliasType

from datahub.ingestion.api.source import SourceReport

# GraphQL's JsonPrimitive scalar.
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
    return value or []


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
        seen: Set[str] = set()
        duplicates: Set[str] = set()
        for attribute in self.business_metadata:
            if not attribute.name:
                continue
            if attribute.name in seen:
                duplicates.add(attribute.name)
            seen.add(attribute.name)
        return sorted(duplicates)


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


@dataclass(frozen=True)
class NameIndex(Generic[CatalogEntityType]):
    by_name: Dict[str, CatalogEntityType]
    ambiguous: Dict[str, List[CatalogEntityType]]
    case_ambiguous: Dict[str, List[str]] = field(default_factory=dict)
    empty_name_count: int = 0
    _by_lowered_name: Dict[str, CatalogEntityType] = field(init=False, repr=False)

    # frozen would otherwise generate a __hash__ that crashes on the dict fields.
    __hash__ = None  # type: ignore[assignment]

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

    def report_issues(self, report: SourceReport, entity_label: str) -> None:
        if self.empty_name_count:
            report.warning(
                message="Skipped Stream Catalog entities that had an empty name",
                context=f"entity={entity_label}, count={self.empty_name_count}",
            )
        for lowered, variants in self.case_ambiguous.items():
            report.warning(
                message=(
                    "Case-insensitive Stream Catalog lookup is disabled for a name that "
                    "matches more than one catalog entity; exact-case lookups still work"
                ),
                context=(
                    f"entity={entity_label}, name={lowered}, variants={sorted(variants)}"
                ),
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

    # Distinct casings under one lowered key — including ambiguous exact-names —
    # so a unique sibling cannot win case-insensitive get() for a duplicate.
    case_ambiguous: Dict[str, List[str]] = {}
    exact_names_by_lowered: Dict[str, Set[str]] = defaultdict(set)
    for name in grouped:
        exact_names_by_lowered[name.lower()].add(name)
    for lowered, exact_names in exact_names_by_lowered.items():
        if len(exact_names) >= 2:
            case_ambiguous[lowered] = sorted(exact_names)

    return NameIndex(
        by_name=by_name,
        ambiguous=ambiguous,
        case_ambiguous=case_ambiguous,
        empty_name_count=empty_name_count,
    )
