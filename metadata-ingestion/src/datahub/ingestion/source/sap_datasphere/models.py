from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    TypedDict,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datahub.ingestion.source.sap_datasphere.config import ConnectionPlatformConfig
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


class TagDefinition(BaseModel):
    """Display name + description for a predefined tag URN."""

    model_config = ConfigDict(frozen=True)

    name: str
    description: str


class UnknownColumnType(BaseModel):
    """A column whose source type literal (CDS or EDM) is not in the parser's
    type map. Emitted by both parsers so the source can report it uniformly."""

    model_config = ConfigDict(frozen=True)

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


class ResolvedPlatform(BaseModel):
    # A disabled mapping is turned into a skip during resolution, so holding a
    # ResolvedPlatform always implies enabled.
    model_config = ConfigDict(frozen=True)

    platform: str
    platform_instance: Optional[str]
    env: str
    # Per-platform casing for external (federated / flow-target) URNs, matching how
    # the sibling native connector cases its URNs. Independent of the connector's
    # top-level flag (which governs managed assets).
    convert_urns_to_lowercase: bool = True
    # Optional leading name segment for external URNs that the Datasphere API
    # can't supply — chiefly the BigQuery GCP project, which is absent from the
    # connections/flow payloads. When set, it is prepended ahead of the
    # container-derived schema so the URN becomes `database.schema.table`.
    database: Optional[str] = None


class ResolveResult(BaseModel):
    # Exactly one side is populated: a ResolvedPlatform on success, or a
    # skip_reason when the asset's connection could not be resolved.
    model_config = ConfigDict(frozen=True)

    platform: Optional[ResolvedPlatform]
    skip_reason: Optional[ResolveSkipReason]


class TypeIdDefault(BaseModel):
    # Result of mapping a connection's typeId to its platform default: the raw
    # (still enabled/disabled-unchecked) config on success, else a skip reason.
    model_config = ConfigDict(frozen=True)

    config: Optional[ConnectionPlatformConfig]
    skip_reason: Optional[ResolveSkipReason]


class EdmxFetchReason(StrEnum):
    OK = "ok"
    FORBIDDEN = "forbidden"  # 403 — principal lacks OData read (already warned)
    NOT_FOUND = "not_found"  # 404 — asset not exposed via OData (benign)
    ERROR = "error"  # network / non-2xx / other


class EdmxFetchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    xml: Optional[str]
    reason: EdmxFetchReason

    @model_validator(mode="after")
    def _check_invariant(self) -> "EdmxFetchResult":
        # Make the illegal states (OK-with-no-xml, non-OK carrying xml)
        # unrepresentable so callers can branch on reason and trust xml.
        if (self.reason is EdmxFetchReason.OK) != (self.xml is not None):
            raise ValueError(
                f"EdmxFetchResult invariant violated: xml must be set iff "
                f"reason is OK (got reason={self.reason}, xml_is_none="
                f"{self.xml is None})"
            )
        return self


class EdmxParseResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fields: List[SchemaFieldClass]
    field_custom_props: Dict[str, Dict[str, str]]  # fieldPath → {key: value}
    entity_label: Optional[str]
    entity_custom_props: Dict[str, str]
    error: Optional[str] = None  # set when parse failed; None on success
    unknown_edm_types: List[UnknownColumnType] = Field(default_factory=list)


class CsnSchemaResult(BaseModel):
    # CSN-elements schema parse output: the fields plus any columns whose CDS
    # type literal was unmapped (surfaced in the report, not silently dropped).
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fields: List[SchemaFieldClass]
    unknown_types: List[UnknownColumnType] = Field(default_factory=list)
    # Names of association/composition elements skipped from the scalar schema;
    # their targets are turned into lineage edges by the lineage extractor.
    navigation_elements: List[str] = Field(default_factory=list)


class BusinessLayer(BaseModel):
    model_config = ConfigDict(frozen=True)

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


class UpstreamRef(BaseModel):
    """A table-level upstream reference the source layer resolves to a URN.

    ``qualified`` distinguishes a bare same-space technical name (space-prefixed
    at URN build time) from an already space-qualified name — e.g. a cross-space
    or built-in association target like ``SAP.TIME.M_TIME_DIMENSION_TDAY`` — which
    is used as-is.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    qualified: bool = False


class UpstreamColRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    qname: str
    col: str
    # True when ``qname`` is already space-qualified (association target); a bare
    # FROM-derived ref is space-prefixed by the source layer.
    qualified: bool = False


# Transform-operation labels DataHub's UI recognises for its transformation badge.
TransformOp = Literal["IDENTITY", "RENAME", "AGGREGATE", "TRANSFORMATION", "EXPRESSION"]


class ColumnLineagePair(BaseModel):
    # downstream_col may be MALFORMED_COL_SENTINEL / UNNAMED_COL_SENTINEL for
    # structurally-broken or unnamed expressions. upstream_refs is empty for
    # pure-literal columns. Resolution of CSN qualified-names to DataHub URNs is
    # the source layer's job. unresolved_refs carries diagnostics for refs the
    # walker could not attribute, surfaced via report.column_lineage_unresolved.
    model_config = ConfigDict(frozen=True)

    downstream_col: str
    upstream_refs: List[UpstreamColRef] = Field(default_factory=list)
    transform_op: Optional[TransformOp] = None
    unresolved_refs: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_sentinel(self) -> "ColumnLineagePair":
        # A sentinel-valued pair with upstream refs would make the source build a
        # schemaField URN containing the sentinel string.
        if self.downstream_col in (MALFORMED_COL_SENTINEL, UNNAMED_COL_SENTINEL):
            if self.upstream_refs:
                raise ValueError(
                    f"Sentinel ColumnLineagePair must have empty upstream_refs; "
                    f"got downstream_col={self.downstream_col!r}, "
                    f"upstream_refs={self.upstream_refs!r}"
                )
        return self


class CsnSelectEnvelope(BaseModel):
    # Tri-state result of validating a CSN query envelope: ``select`` set to
    # proceed, ``malformed`` set (with a diagnostic pair) for broken CSN, or both
    # None for a legitimate non-SELECT entity.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    select: Optional[Dict] = None
    malformed: Optional["ColumnLineagePair"] = None


class ColumnLineageContext(BaseModel):
    # The pairs and the downstream URN are coupled (the URN is needed to build
    # the downstream field URN of every pair); bundling them keeps callers from
    # passing inconsistent combinations.
    model_config = ConfigDict(frozen=True)

    pairs: List[ColumnLineagePair]
    downstream_dataset_urn: str


class FlowEndpoint(BaseModel):
    """One input or output object of a flow, resolved to a routable location.

    ``connection is None`` (or ``is_local``) means the object lives in the
    tenant's own managed HANA Cloud and is emitted on the sap_datasphere
    platform in ``space``. Otherwise it is external and ``connection`` /
    ``connection_type`` drive platform mapping.
    """

    model_config = ConfigDict(frozen=True)

    object_name: str
    is_local: bool
    connection: Optional[str] = None
    connection_type: Optional[str] = None
    # The source/target system's schema/dataset path (SAP calls it the
    # "container", e.g. "/CDS_EXTRACTION", "/SAPTCH", "/staging"). Prepended to
    # the bare object_name to build a fully-qualified external URN so it stitches
    # to the sibling connector's schema.table naming. None for local objects
    # (they are space-prefixed instead).
    container: Optional[str] = None


class FlowColumnMapping(BaseModel):
    # object names must match a FlowEndpoint.object_name so the source layer can
    # resolve them to the same dataset URNs used for the table-level edges.
    model_config = ConfigDict(frozen=True)

    downstream_object: str
    downstream_col: str
    upstream_object: str
    upstream_col: str


class SystemIdentity(BaseModel):
    # The external connection id + type of a replication flow's source/target
    # system, used to route its objects to a DataHub platform. ``container`` is
    # the system-wide schema/dataset path shared by every object on that side.
    model_config = ConfigDict(frozen=True)

    connection: Optional[str] = None
    connection_type: Optional[str] = None
    container: Optional[str] = None


class AttrMapping(BaseModel):
    # A single (downstream_col, upstream_col) column mapping parsed from a flow's
    # attributeMappings before it is attributed to concrete objects.
    model_config = ConfigDict(frozen=True)

    downstream_col: str
    upstream_col: str


class ProducerColumns(BaseModel):
    # A producer node's output object plus its column mappings, buffered until
    # the flow's sole input is known (column lineage needs both endpoints).
    model_config = ConfigDict(frozen=True)

    object_name: str
    mappings: List[AttrMapping] = Field(default_factory=list)


class ParsedFlow(BaseModel):
    """A flow object reduced to what DataHub needs: its IO datasets and the
    column mappings between them. Platform/URN resolution is the source layer's
    job (it owns the per-space connection resolver)."""

    model_config = ConfigDict(frozen=True)

    technical_name: str
    subtype: str
    inputs: List[FlowEndpoint] = Field(default_factory=list)
    outputs: List[FlowEndpoint] = Field(default_factory=list)
    column_mappings: List[FlowColumnMapping] = Field(default_factory=list)


class RemoteTableSource(BaseModel):
    """The external origin of a federated Remote Table, parsed from the CSN
    ``@DataWarehouse.remote.*`` annotations."""

    model_config = ConfigDict(frozen=True)

    connection: str
    path_parts: List[str]

    @property
    def qualified_name(self) -> str:
        # SAP encodes an absent database segment as the literal "<NULL>"; drop it
        # (and any empties) so the external URN name is "schema.table".
        meaningful = [p for p in self.path_parts if p and p != "<NULL>"]
        return ".".join(meaningful)
