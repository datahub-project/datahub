from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    TypedDict,
    TypeVar,
)

from datahub.ingestion.source.sap_datasphere.constants import (
    MALFORMED_COL_SENTINEL,
    UNNAMED_COL_SENTINEL,
)
from datahub.metadata.schema_classes import SchemaFieldClass
from datahub.utilities.str_enum import StrEnum

_T = TypeVar("_T")


def dedup_preserving_order(items: Iterable[_T]) -> List[_T]:
    seen: set = set()
    out: List[_T] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


class UnknownColumnType(NamedTuple):
    """A column whose source type literal (CDS or EDM) is not in the parser's
    type map. Emitted by both parsers so the source can report it uniformly."""

    type: str
    column: str


class ConnectionRecord(TypedDict, total=False):
    # The SAP connections API returns more fields (id, description, owner, ...)
    # but the connector only consumes name + typeId; total=False stays permissive
    # at the JSON boundary.
    name: str
    typeId: str


class ResolveSkipReason(StrEnum):
    UNKNOWN_TYPEID = "unknown_typeid"
    UNKNOWN_CONNECTION = "unknown_connection"
    DISABLED = "disabled"


@dataclass(frozen=True)
class ResolvedPlatform:
    # A disabled mapping is turned into a skip during resolution, so holding a
    # ResolvedPlatform always implies enabled.
    platform: str
    platform_instance: Optional[str]
    env: str


class ResolveResult(NamedTuple):
    # Exactly one side is populated: a ResolvedPlatform on success, or a
    # skip_reason when the asset's connection could not be resolved.
    platform: Optional[ResolvedPlatform]
    skip_reason: Optional[ResolveSkipReason]


class EdmxFetchReason(StrEnum):
    OK = "ok"
    FORBIDDEN = "forbidden"  # 403 — principal lacks OData read (already warned)
    NOT_FOUND = "not_found"  # 404 — asset not exposed via OData (benign)
    ERROR = "error"  # network / non-2xx / other


@dataclass(frozen=True)
class EdmxFetchResult:
    xml: Optional[str]
    reason: EdmxFetchReason

    def __post_init__(self) -> None:
        # Make the illegal states (OK-with-no-xml, non-OK carrying xml)
        # unrepresentable so callers can branch on reason and trust xml.
        if (self.reason is EdmxFetchReason.OK) != (self.xml is not None):
            raise ValueError(
                f"EdmxFetchResult invariant violated: xml must be set iff "
                f"reason is OK (got reason={self.reason}, xml_is_none="
                f"{self.xml is None})"
            )


@dataclass
class EdmxParseResult:
    fields: List[SchemaFieldClass]
    field_custom_props: Dict[str, Dict[str, str]]  # fieldPath → {key: value}
    entity_label: Optional[str]
    entity_custom_props: Dict[str, str]
    error: Optional[str] = None  # set when parse failed; None on success
    unknown_edm_types: List[UnknownColumnType] = field(default_factory=list)


@dataclass(frozen=True)
class BusinessLayer:
    fact_source_keys: List[str]
    dimension_source_keys: List[str]
    measure_names: List[str]
    attribute_names: List[str]
    variable_names: List[str]

    @property
    def upstream_keys(self) -> List[str]:
        # Derived (facts first) rather than stored so it can never drift.
        return dedup_preserving_order(
            self.fact_source_keys + self.dimension_source_keys
        )


class UpstreamColRef(NamedTuple):
    qname: str
    col: str


# Transform-operation labels DataHub's UI recognises for its transformation badge.
TransformOp = Literal["IDENTITY", "RENAME", "AGGREGATE", "TRANSFORMATION", "EXPRESSION"]


@dataclass(frozen=True)
class ColumnLineagePair:
    # downstream_col may be MALFORMED_COL_SENTINEL / UNNAMED_COL_SENTINEL for
    # structurally-broken or unnamed expressions. upstream_refs is empty for
    # pure-literal columns. Resolution of CSN qualified-names to DataHub URNs is
    # the source layer's job. unresolved_refs carries diagnostics for refs the
    # walker could not attribute, surfaced via report.column_lineage_unresolved.
    downstream_col: str
    upstream_refs: Tuple[UpstreamColRef, ...] = ()
    transform_op: Optional[TransformOp] = None
    unresolved_refs: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # A sentinel-valued pair with upstream refs would make the source build a
        # schemaField URN containing the sentinel string.
        if self.downstream_col in (MALFORMED_COL_SENTINEL, UNNAMED_COL_SENTINEL):
            if self.upstream_refs:
                raise ValueError(
                    f"Sentinel ColumnLineagePair must have empty upstream_refs; "
                    f"got downstream_col={self.downstream_col!r}, "
                    f"upstream_refs={self.upstream_refs!r}"
                )


@dataclass(frozen=True)
class ColumnLineageContext:
    # The pairs and the downstream URN are coupled (the URN is needed to build
    # the downstream field URN of every pair); bundling them keeps callers from
    # passing inconsistent combinations.
    pairs: Tuple[ColumnLineagePair, ...]
    downstream_dataset_urn: str


class FlowEndpoint(NamedTuple):
    """One input or output object of a flow, resolved to a routable location.

    ``connection is None`` (or ``is_local``) means the object lives in the
    tenant's own managed HANA Cloud and is emitted on the sap_datasphere
    platform in ``space``. Otherwise it is external and ``connection`` /
    ``connection_type`` drive platform mapping.
    """

    object_name: str
    is_local: bool
    connection: Optional[str] = None
    connection_type: Optional[str] = None


class FlowColumnMapping(NamedTuple):
    # object names must match a FlowEndpoint.object_name so the source layer can
    # resolve them to the same dataset URNs used for the table-level edges.
    downstream_object: str
    downstream_col: str
    upstream_object: str
    upstream_col: str


@dataclass(frozen=True)
class ParsedFlow:
    """A flow object reduced to what DataHub needs: its IO datasets and the
    column mappings between them. Platform/URN resolution is the source layer's
    job (it owns the per-space connection resolver)."""

    technical_name: str
    subtype: str
    inputs: Tuple[FlowEndpoint, ...] = ()
    outputs: Tuple[FlowEndpoint, ...] = ()
    column_mappings: Tuple[FlowColumnMapping, ...] = ()


@dataclass(frozen=True)
class RemoteTableSource:
    """The external origin of a federated Remote Table, parsed from the CSN
    ``@DataWarehouse.remote.*`` annotations."""

    connection: str
    path_parts: Tuple[str, ...]

    @property
    def qualified_name(self) -> str:
        # SAP encodes an absent database segment as the literal "<NULL>"; drop it
        # (and any empties) so the external URN name is "schema.table".
        meaningful = [p for p in self.path_parts if p and p != "<NULL>"]
        return ".".join(meaningful)
