from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.metabase.constants import (
    _CARD_TYPE_MODEL,
    _ENGINE_TO_DB_DETAIL_FIELD,
    _JDBC_DB_EXTENSION,
    _JDBC_URI_SCHEMES,
    _MBQL_REF_AGGREGATION,
    _MULTIPLE_UNDERSCORES_PATTERN,
    _QUERY_TYPE_NATIVE,
    _SPECIAL_CHARS_PATTERN,
    _TWO_TIER_PLATFORMS,
)
from datahub.ingestion.source.metabase.mbql import (
    MBQLFieldRefs,
    extract_mbql_field_refs,
)


class MetabaseBaseModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class MetabaseUser(MetabaseBaseModel):
    id: int
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    common_name: Optional[str] = None


class MetabaseResultMetadata(MetabaseBaseModel):
    name: Optional[str] = None
    display_name: Optional[str] = None
    base_type: Optional[str] = None
    semantic_type: Optional[str] = None
    field_ref: Optional[List] = None  # Heterogeneous list: ["field", "name", {...}]


class MetabaseNativeQuery(MetabaseBaseModel):
    query: Optional[str] = None
    template_tags: Optional[Dict] = Field(
        None, alias="template-tags"
    )  # Metabase template tag definitions


class MetabaseField(MetabaseBaseModel):
    id: int
    name: str
    display_name: Optional[str] = None
    table_id: Optional[int] = None
    base_type: Optional[str] = None


class MetabaseJoin(MetabaseBaseModel):
    source_table: Optional[Union[int, str]] = Field(None, alias="source-table")
    alias: Optional[str] = None
    condition: Optional[List[object]] = None


class MetabaseQuery(MetabaseBaseModel):
    source_table: Optional[Union[int, str]] = Field(None, alias="source-table")
    # MBQL clauses are heterogeneous arrays; List[object] forces isinstance narrowing at call sites.
    filter: Optional[List[object]] = None
    # aggregation may be a list-of-clauses or a bare single clause in older MBQL
    aggregation: Optional[List[object]] = None
    breakout: Optional[List[object]] = None
    fields: Optional[List[object]] = None
    joins: Optional[List[MetabaseJoin]] = None
    expressions: Optional[Dict[str, object]] = None

    @property
    def source_table_refs(self) -> List[Union[int, str]]:
        refs: List[Union[int, str]] = []
        if self.source_table is not None:
            refs.append(self.source_table)
        for join in self.joins or []:
            if join.source_table is not None:
                refs.append(join.source_table)
        return refs

    def is_passthrough(self) -> bool:
        return (
            self.source_table is not None
            and not self.filter
            and not self.aggregation
            and not self.breakout
            and not self.fields
            and not self.joins
            and not self.expressions
        )

    def collect_field_refs(self) -> MBQLFieldRefs:
        refs = MBQLFieldRefs()
        for clause_list in [
            self.fields or [],
            self.breakout or [],
            self.aggregation or [],
        ]:
            for clause in clause_list:
                if isinstance(clause, list):
                    refs.extend(extract_mbql_field_refs(clause))
        if self.expressions:
            for expr_clause in self.expressions.values():
                if isinstance(expr_clause, list):
                    refs.extend(extract_mbql_field_refs(expr_clause))
        return refs

    def collect_field_ids(self) -> List[int]:
        return self.collect_field_refs().ids


class MetabaseDatasetQuery(MetabaseBaseModel):
    type: str
    database: Optional[int] = None
    native: Optional[MetabaseNativeQuery] = None
    query: Optional[MetabaseQuery] = None


class MetabaseLastEditInfo(MetabaseBaseModel):
    id: Optional[int] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    timestamp: Optional[str] = None


class MetabaseCard(MetabaseBaseModel):
    id: int
    name: str
    description: Optional[str] = None
    display: Optional[str] = None
    dataset_query: MetabaseDatasetQuery = Field(
        default_factory=lambda: MetabaseDatasetQuery(type=_QUERY_TYPE_NATIVE)
    )
    query_type: Optional[str] = None
    result_metadata: List[MetabaseResultMetadata] = Field(default_factory=list)
    creator_id: Optional[int] = None
    collection_id: Optional[int] = None
    type: Optional[str] = None
    database_id: Optional[int] = None
    table_id: Optional[int] = None
    last_edit_info: Optional[MetabaseLastEditInfo] = Field(None, alias="last-edit-info")
    public_uuid: Optional[str] = None
    created_at: Optional[str] = None

    @property
    def custom_properties(self) -> Dict[str, str]:
        metrics, dimensions = [], []
        for meta in self.result_metadata:
            display_name = meta.display_name or ""
            if _MBQL_REF_AGGREGATION in str(meta.field_ref or ""):
                metrics.append(display_name)
            else:
                dimensions.append(display_name)
        filters: List = (
            self.dataset_query.query.filter or []
            if self.dataset_query and self.dataset_query.query
            else []
        )
        props = {
            "Metrics": ", ".join(metrics),
            "Filters": str(filters) if filters else "",
            "Dimensions": ", ".join(dimensions),
        }
        if self.database_id is not None:
            props["database_id"] = str(self.database_id)
        return props


class MetabaseCardInfo(MetabaseBaseModel):
    """Basic card info (used in dashcards)."""

    id: Optional[int] = None
    name: Optional[str] = None


class MetabaseDashCard(MetabaseBaseModel):
    id: int
    card: MetabaseCardInfo
    dashboard_id: int


class MetabaseDashboard(MetabaseBaseModel):
    id: int
    name: str
    description: Optional[str] = None
    creator_id: Optional[int] = None
    collection_id: Optional[int] = None
    dashcards: List[MetabaseDashCard] = Field(default_factory=list)
    last_edit_info: Optional[MetabaseLastEditInfo] = Field(None, alias="last-edit-info")


class MetabaseDashboardListItem(MetabaseBaseModel):
    """Dashboard item from collection items API (minimal fields)."""

    id: int
    name: Optional[str] = None
    model: str  # Always "dashboard" for these items


class MetabaseCardListItem(MetabaseBaseModel):
    """Card/question item from cards list API (minimal fields)."""

    id: int
    name: Optional[str] = None
    type: Optional[str] = None  # "question" or "model"

    @property
    def is_model(self) -> bool:
        return self.type == _CARD_TYPE_MODEL


class MetabaseCollectionItemsResponse(MetabaseBaseModel):
    """Response from /api/collection/{id}/items API."""

    data: List[MetabaseDashboardListItem] = Field(default_factory=list)
    total: Optional[int] = None


class MetabaseCollection(MetabaseBaseModel):
    # Metabase returns id='root' for the top-level "Our analytics" collection.
    id: Union[int, Literal["root"]]
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    archived: Optional[bool] = None

    @property
    def is_root(self) -> bool:
        return self.id == "root"

    @property
    def tag_slug(self) -> str:
        """Sanitized collection name for use in a DataHub tag URN."""
        sanitized = self.name.replace(" ", "_")
        sanitized = _SPECIAL_CHARS_PATTERN.sub("", sanitized)
        sanitized = sanitized.lower()
        sanitized = _MULTIPLE_UNDERSCORES_PATTERN.sub("_", sanitized)
        return sanitized.strip("_")


class MetabaseLoginResponse(MetabaseBaseModel):
    """Response from /api/session API."""

    id: str  # Session token


class MetabaseDatabaseDetails(MetabaseBaseModel):
    model_config = ConfigDict(extra="allow")

    host: Optional[str] = None
    port: Optional[int] = None
    dbname: Optional[str] = None
    db: Optional[str] = None
    database: Optional[str] = None
    catalog: Optional[str] = None
    project_id: Optional[str] = Field(None, alias="project-id")
    dataset_id: Optional[str] = Field(None, alias="dataset-id")
    schema_: Optional[str] = Field(None, alias="schema")
    service_name: Optional[str] = Field(None, alias="service-name")

    @property
    def _clean_db(self) -> Optional[str]:
        """
        Sanitize the `db` field for use as a dataset name.

        H2 stores a full JDBC path here (e.g. `file:/plugins/sample-database.db;USER=GUEST;PASSWORD=guest`).
        Other engines using this field (Redshift, Snowflake, SQL Server) store a plain name,
        so stripping is a no-op for them.
        """
        if not self.db:
            return None
        path = self.db.split(";")[0]
        for prefix in _JDBC_URI_SCHEMES:
            if path.startswith(prefix):
                path = path[len(prefix) :]
                break
        name = path.rsplit("/", 1)[-1]
        if name.endswith(_JDBC_DB_EXTENSION):
            name = name[: -len(_JDBC_DB_EXTENSION)]
        return name or None

    def get_database_name(self, engine: str) -> Optional[str]:
        """Return the logical database name for the given engine, or None if unknown."""
        field_name = _ENGINE_TO_DB_DETAIL_FIELD.get(engine)
        return getattr(self, field_name) if field_name else None


class MetabaseDatabase(MetabaseBaseModel):
    id: int
    name: str
    engine: str
    details: MetabaseDatabaseDetails = Field(default_factory=MetabaseDatabaseDetails)


class MetabaseTable(MetabaseBaseModel):
    id: int
    name: str
    display_name: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    db_id: int


class DatasourceInfo(MetabaseBaseModel):
    platform: str
    database_name: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    platform_instance: Optional[str] = None

    @property
    def has_schema(self) -> bool:
        return self.platform not in _TWO_TIER_PLATFORMS


class _MBQLContext(MetabaseBaseModel):
    """Shared context threaded through MBQL column-level lineage resolution."""

    query: MetabaseQuery
    datasource: DatasourceInfo
    resolved: Dict[int, MetabaseField] = Field(default_factory=dict)


class MetabaseCollectionKey(ContainerKey):
    collection_id: int

    def property_dict(self) -> Dict[str, str]:
        return {k: str(v) for k, v in super().property_dict().items()}
