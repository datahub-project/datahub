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
from typing_extensions import TypeAliasType

from datahub.ingestion.api.source import SourceReport

# GraphQL's JsonPrimitive scalar.
BusinessMetadataValue = Union[str, bool, int, float]


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


CatalogEntityType = TypeVar("CatalogEntityType", bound=CatalogEntity)


def non_colliding_business_metadata(
    entity: CatalogEntity,
    existing: Mapping[str, str],
    report: SourceReport,
    collides_with: str,
    context: str,
) -> Dict[str, str]:
    properties = entity.properties_from_business_metadata()
    if not properties:
        return {}

    collisions = sorted(properties.keys() & existing.keys())
    if collisions:
        report.warning(
            message=(
                "Ignoring Stream Catalog business metadata attributes whose names "
                f"collide with {collides_with}. Rename the attributes in Confluent "
                "to emit them."
            ),
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
                message=f"Skipped Stream Catalog {entity_label}s that had an empty name",
                context=f"count={self.empty_name_count}",
            )
        for lowered, variants in self.case_ambiguous.items():
            report.warning(
                message=f"Case-insensitive Stream Catalog {entity_label} lookup is disabled for a "
                "name that matches more than one catalog entity; exact-case lookups still work",
                context=f"name={lowered}, variants={sorted(variants)}",
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
