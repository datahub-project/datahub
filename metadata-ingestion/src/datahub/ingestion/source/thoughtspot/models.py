"""Pydantic models for ThoughtSpot API responses.

This module defines data models that represent ThoughtSpot REST API v2.0 responses.
These models provide type-safe access to metadata fields and enable validation of
API response structure.
"""

import logging
from typing import Any, Final, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

logger = logging.getLogger(__name__)

# Drop-sink context keys for ``Model.model_validate(..., context={...})``.
# Defined as module-level constants so a rename here propagates to every
# validator and consumer without silently breaking drop-tracking.
VISUALIZATION_DROPS: Final[str] = "visualization_drops"
SOURCE_TABLE_DROPS: Final[str] = "source_table_drops"
COLUMN_SOURCE_DROPS: Final[str] = "column_source_drops"


def _validation_drop_sink(info: ValidationInfo, key: str) -> Optional[List[str]]:
    """Return the caller-supplied drop list from ``info.context``, if any.

    Callers opt into report-grade drop tracking by passing
    ``context={key: <list>}`` to ``Model.model_validate``. Callers that
    use the no-context ``__init__`` path get ``None`` here, and drops
    fall back to ``logger.warning`` only.
    """
    if info.context is None:
        return None
    sink = info.context.get(key)
    if isinstance(sink, list):
        return sink
    return None


def _record_drop(sink: Optional[List[str]], reason: str) -> None:
    if sink is not None:
        sink.append(reason)


class ThoughtSpotAuthor(BaseModel):
    """Represents a ThoughtSpot user (author/owner) from API responses.

    ThoughtSpot API returns author information in nested structures within
    metadata objects. This model captures the essential user identity fields.
    """

    id: str = Field(
        min_length=1,
        description="Unique identifier (GUID) for the user in ThoughtSpot",
    )

    name: str = Field(
        min_length=1,
        description="Username or login identifier (e.g., 'john.doe' or 'john.doe@example.com')",
    )

    display_name: Optional[str] = Field(
        default=None, description="Human-readable display name (e.g., 'John Doe')"
    )

    email: Optional[str] = Field(default=None, description="User's email address")


class TagRef(BaseModel):
    """A reference to a ThoughtSpot tag attached to a metadata object.

    The only wire shape currently observed from TS REST API v2 is
    ``{"id": "<guid>"}`` — pydantic deserialises it natively without a
    coercer. ``ThoughtSpotMetadataHeader.tags`` is therefore typed as
    ``Optional[List[TagRef]]`` directly. If a future TS version emits a
    different shape, the ingestion report will surface a parse failure
    (via the model-construction drop path) and we'll add explicit support
    then with evidence — rather than speculatively branching on shapes we
    don't see today.
    """

    id: str = Field(min_length=1, description="GUID of the referenced tag.")

    model_config = ConfigDict(extra="ignore")


class EntityStats(BaseModel):
    """Per-entity usage stats returned by TS REST v2 ``metadata_search``
    when ``include_stats=True`` is sent in the request.

    Available on LIVEBOARD and ANSWER responses. LOGICAL_TABLE returns
    ``null`` here (TS does not track per-dataset view counts).

    ``views`` matches the global cumulative counter shown in the TS UI's
    "Views" column — this is the metric the connector emits as
    ``DashboardUsageStatistics`` / ``ChartUsageStatistics``.
    """

    views: int = Field(
        default=0,
        ge=0,
        description="Cumulative view count across all users (global counter).",
    )
    favorites: int = Field(
        default=0,
        ge=0,
        description="Number of users who have marked this entity as a favourite.",
    )
    last_accessed: Optional[int] = Field(
        default=None,
        description="Most recent access timestamp (Unix epoch milliseconds).",
    )

    model_config = ConfigDict(extra="ignore")


class ThoughtSpotMetadataHeader(BaseModel):
    """Represents common metadata fields returned by ThoughtSpot API.

    All ThoughtSpot metadata objects (Liveboards, Answers, Worksheets, etc.)
    share a common set of header fields. The client flattens the nested
    'metadata_header' structure from API responses into this top-level model.

    This model corresponds to the flattened structure after client.py processing.
    """

    id: str = Field(
        min_length=1,
        description="Unique identifier (GUID) for the metadata object",
    )

    name: str = Field(min_length=1, description="Display name of the metadata object")

    description: Optional[str] = Field(
        default=None, description="User-provided description or documentation"
    )

    author: Optional[ThoughtSpotAuthor] = Field(
        default=None,
        description="Author/creator of the object (nested structure from API)",
    )

    author_name: Optional[str] = Field(
        default=None,
        alias="authorName",
        description=(
            "Flattened author username (e.g., 'john.doe'). TS REST v2 emits "
            "this as ``authorName`` (camelCase) alongside ``author`` which is "
            "often just the user GUID — this is the real human-readable login."
        ),
    )

    author_display_name: Optional[str] = Field(
        default=None,
        alias="authorDisplayName",
        description=(
            "Human-readable author display name (e.g., 'John Doe'). TS REST "
            "v2 emits this as ``authorDisplayName`` (camelCase). Surfaced as "
            "an ``author_display_name`` custom property so the UI shows the "
            "name without round-tripping to the CorpUser entity."
        ),
    )

    owner_id: Optional[str] = Field(
        default=None,
        description="ID of the workspace/org that owns this object (used for container linking)",
    )

    created: Optional[int] = Field(
        default=None,
        description="Creation timestamp (Unix epoch milliseconds)",
    )

    modified: Optional[int] = Field(
        default=None,
        description="Last modification timestamp (Unix epoch milliseconds)",
    )

    tags: Optional[List[TagRef]] = Field(
        default=None,
        description=(
            "References to tags attached to this object. Wire payload is "
            "``[{'id': 'guid'}, ...]``; pydantic deserialises directly. The "
            "source resolves each reference against the tag-name lookup "
            "built from ``/tags/search`` and emits ``GlobalTagsClass`` "
            "aspects."
        ),
    )

    stats: Optional[EntityStats] = Field(
        default=None,
        description=(
            "Per-entity usage stats from TS REST v2 ``metadata_search`` "
            "when ``include_stats=True`` is sent. Populated for "
            "LIVEBOARD and ANSWER; ``null`` for LOGICAL_TABLE."
        ),
    )

    @field_validator("author", mode="before")
    @classmethod
    def validate_author(cls, v: Any) -> Optional[Union[dict, ThoughtSpotAuthor]]:
        """Handle author field that can be either a string UUID or an object.

        ThoughtSpot API sometimes returns author as just a UUID string instead of
        an expanded object. When this happens, we create a minimal author object
        with just the ID field populated.
        """
        if v is None:
            return None
        if isinstance(v, str):
            # Author is just a UUID string - create minimal object
            return ThoughtSpotAuthor(id=v, name=v)
        # Author is already an object/dict
        return v

    model_config = ConfigDict(
        # ``ignore`` (rather than ``allow``) drops unknown fields silently
        # but does not stash them on the instance. This surfaces wire-side
        # typos faster than ``allow`` would (e.g., a renamed key shows up
        # as a missing-attr instead of bleeding onto the instance), while
        # still keeping forward-compat: new TS API fields don't break
        # ingestion until we explicitly model them.
        extra="ignore",
        validate_assignment=True,  # Run validators on post-construction
        # assignments so coercion (e.g. dicts -> VisualizationResponse on
        # LiveboardResponse) is applied consistently whether fields are set
        # in __init__ or later.
        populate_by_name=True,
        # Allow both snake_case attribute names and the camelCase wire aliases
        # (e.g. ``authorName``) so client code that constructs models from
        # already-snake_cased payloads still works while we also pick up the
        # real wire shape via ``alias=...``.
    )


class WorkspaceResponse(ThoughtSpotMetadataHeader):
    """Represents a ThoughtSpot Workspace (Org) from API responses.

    Workspaces are organizational containers in ThoughtSpot that group
    related content (liveboards, answers, worksheets). They map to
    DataHub containers.

    Inherits all common metadata fields from ThoughtSpotMetadataHeader:
    id, name, description, author, author_name, owner_id, created, modified.
    """

    # No additional fields needed beyond ThoughtSpotMetadataHeader
    pass


class LiveboardResponse(ThoughtSpotMetadataHeader):
    """Represents a ThoughtSpot Liveboard (Dashboard) from API responses.

    Liveboards are interactive dashboards containing one or more visualizations.
    They map to DataHub dashboards.

    Inherits all common metadata fields from ThoughtSpotMetadataHeader:
    id, name, description, author, author_name, owner_id, created, modified.
    """

    visualizations: Optional[List["VisualizationResponse"]] = Field(
        default=None,
        description="List of visualizations contained in this liveboard",
    )

    @field_validator("visualizations", mode="before")
    @classmethod
    def _coerce_visualizations(
        cls, v: Any, info: ValidationInfo
    ) -> Optional[List["VisualizationResponse"]]:
        # The TML enrichment path in ``client._get_liveboard_visualizations_via_tml``
        # populates this field with already-typed ``VisualizationResponse``
        # instances; the initial ``metadata/search`` response sometimes
        # carries it as raw dicts. We coerce dicts at construction time so
        # downstream code can iterate over a homogeneous typed list.
        #
        # Defined as a ``field_validator`` (rather than a ``model_validator``)
        # because pydantic's ``validate_assignment`` re-runs only
        # field-level validators on post-construction assignment — this
        # guarantees coercion fires whether the field is provided in
        # ``__init__`` or assigned later by the TML enrichment.
        #
        # Callers that want drops surfaced to the ingestion report pass
        # ``context={"visualization_drops": <list>}`` to
        # ``model_validate``; the validator appends a brief reason per
        # drop to that list. Callers that use ``__init__`` (which has no
        # context) still see the drops in run logs.
        drops = _validation_drop_sink(info, VISUALIZATION_DROPS)
        if v is None:
            return None
        coerced: List[VisualizationResponse] = []
        for item in v:
            if isinstance(item, VisualizationResponse):
                coerced.append(item)
            elif isinstance(item, dict):
                payload = dict(item)
                raw_id = payload.get("id")
                raw_name = payload.get("name")
                # An entry that has neither an id nor a name can't produce
                # a meaningful URN — emitting a placeholder would create a
                # chart entity with a garbage GUID. Drop it loudly.
                if not raw_id and not raw_name:
                    reason = (
                        "visualization entry missing both id and name; "
                        f"keys={list(item.keys())}"
                    )
                    _record_drop(drops, reason)
                    logger.warning(f"Dropping {reason}")
                    continue
                # Some wire payloads omit one of the two; fall back to the
                # other so the entry still produces a usable
                # VisualizationResponse rather than failing the required-field
                # check downstream.
                if not raw_name:
                    payload["name"] = raw_id
                if not raw_id:
                    payload["id"] = raw_name
                try:
                    coerced.append(VisualizationResponse(**payload))
                except Exception as e:
                    reason = (
                        "malformed visualization entry id="
                        f"{item.get('id', 'unknown')}: {e}"
                    )
                    _record_drop(drops, reason)
                    logger.warning(f"Dropping {reason}")
                    continue
            else:
                reason = f"unrecognised visualization entry type={type(item).__name__}"
                _record_drop(drops, reason)
                logger.warning(f"Dropping {reason}")
        return coerced

    source_table_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "GUIDs of LOGICAL_TABLE objects this liveboard depends on, derived "
            "from ``reportContent.sheets[].sheetContent.pinboardFilterDetails`` "
            "when the client fetched with ``include_details=True``. Used to "
            "emit dashboard upstream lineage to dataset entities."
        ),
    )


class VisualizationResponse(ThoughtSpotMetadataHeader):
    """Represents a ThoughtSpot Visualization (Chart) from API responses.

    Visualizations are individual charts/tables within liveboards or answers.
    They map to DataHub charts.

    Inherits all common metadata fields from ThoughtSpotMetadataHeader:
    id, name, description, author, author_name, owner_id, created, modified.
    """

    chart_type: Optional[str] = Field(
        default=None,
        description=(
            "Chart type (e.g., ``LINE``, ``COLUMN``, ``PIE``, ``TABLE``). "
            "Sourced from TML's ``answer.chart_type``. Surfaced as a "
            "``thoughtspot_chart_type`` custom property on the emitted "
            "DataHub chart."
        ),
    )

    question_text: Optional[str] = Field(
        default=None,
        description=(
            "Natural-language search query that produced this visualization. "
            "Sourced from TML's ``answer.search_query`` — the most "
            "distinctive ThoughtSpot field for understanding what a chart "
            "shows. Surfaced as a ``thoughtspot_question_text`` custom "
            "property."
        ),
    )

    answer_id: Optional[str] = Field(
        default=None,
        description="ID of the Answer this visualization is derived from (for lineage)",
    )

    source_table_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "GUIDs of LOGICAL_TABLE objects referenced by this visualization, "
            "extracted from TML's ``answer.tables[].fqn``. Populated by the "
            "TML-based viz fetch when accessible; ``None`` when the dashboard's "
            "TML export was forbidden or unavailable."
        ),
    )

    source_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Worksheet column names this visualization reads, parsed from "
            "TML's ``answer.search_query`` ``[bracketed]`` tokens. Used to "
            "emit Chart→Dataset column-level lineage via the InputFields "
            "aspect."
        ),
    )


class SourceTableRef(BaseModel):
    """A reference to a LOGICAL_TABLE backing an Answer or Visualization.

    Wire shape from ``client._get_answer_dependencies`` is
    ``{"id": "<guid>", "name": "<human name>"}``; older code paths
    sometimes passed a bare GUID string, which the coercer below tolerates.
    """

    id: str = Field(min_length=1, description="GUID of the referenced LOGICAL_TABLE.")
    name: Optional[str] = Field(default=None, description="Display name.")


class AnswerResponse(ThoughtSpotMetadataHeader):
    """Represents a ThoughtSpot Answer (Saved Search) from API responses.

    Answers are saved search queries that can be reused and shared.
    They map to DataHub charts (similar to visualizations).

    Inherits all common metadata fields from ThoughtSpotMetadataHeader:
    id, name, description, author, author_name, owner_id, created, modified.
    """

    chart_type: Optional[str] = Field(
        default=None,
        description=(
            "Chart type (e.g., ``LINE``, ``COLUMN``, ``PIE``, ``TABLE``). "
            "Sourced from TML's ``answer.chart_type``. Surfaced as a "
            "``thoughtspot_chart_type`` custom property."
        ),
    )

    question_text: Optional[str] = Field(
        default=None,
        description=(
            "Natural-language search query that produced this answer. "
            "Sourced from TML's ``answer.search_query``. Surfaced as a "
            "``thoughtspot_question_text`` custom property."
        ),
    )

    source_tables: Optional[List[SourceTableRef]] = Field(
        default=None,
        description="Source tables/worksheets referenced by this Answer (for lineage tracking)",
    )

    source_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Worksheet column names this answer reads, parsed from "
            "TML's ``answer.search_query`` ``[bracketed]`` tokens. Used to "
            "emit Chart→Dataset column-level lineage via the InputFields "
            "aspect — mirrors ``VisualizationResponse.source_columns``."
        ),
    )

    @field_validator("source_tables", mode="before")
    @classmethod
    def _coerce_source_tables(
        cls, v: Any, info: ValidationInfo
    ) -> Optional[List["SourceTableRef"]]:
        # ``client._get_answer_dependencies`` builds these as
        # ``[{"id": ..., "name": ...}]`` dicts; the coercer turns them
        # into typed ``SourceTableRef`` instances so source.py can
        # iterate without runtime ``isinstance`` dispatch.
        #
        # Callers that want drops surfaced to the ingestion report pass
        # ``context={"source_table_drops": <list>}`` to
        # ``model_validate``; see ``_coerce_visualizations`` for the
        # same pattern.
        drops = _validation_drop_sink(info, SOURCE_TABLE_DROPS)
        if v is None:
            return None
        coerced: List[SourceTableRef] = []
        for item in v:
            if isinstance(item, SourceTableRef):
                coerced.append(item)
            elif isinstance(item, dict):
                try:
                    coerced.append(SourceTableRef(**item))
                except Exception as e:
                    reason = (
                        "malformed source_tables entry id="
                        f"{item.get('id', 'unknown')}: {e}"
                    )
                    _record_drop(drops, reason)
                    logger.warning(f"Dropping {reason}")
            else:
                reason = f"unrecognised source_tables entry type={type(item).__name__}"
                _record_drop(drops, reason)
                logger.warning(f"Dropping {reason}")
        return coerced


class LogicalTableResponse(ThoughtSpotMetadataHeader):
    """Represents a ThoughtSpot Logical Table (Worksheet/View) from API responses.

    Logical tables are data abstractions in ThoughtSpot (worksheets, views, tables)
    that provide a user-friendly interface to underlying data sources.
    They map to DataHub datasets.

    Inherits all common metadata fields from ThoughtSpotMetadataHeader:
    id, name, description, author, author_name, owner_id, created, modified.

    When the client fetches with ``include_details=True``, the schema columns
    and the source-connection reference come back populated on the model so
    callers don't need a second detail call.
    """

    columns: Optional[List["ColumnResponse"]] = Field(
        default=None,
        description=(
            "Column-level schema, populated when the list call requested "
            "``include_details=True``. ``None`` when details were not requested."
        ),
    )

    type: Optional[str] = Field(
        default=None,
        description=(
            "ThoughtSpot-side discriminator for this logical table. Common "
            "values are ``WORKSHEET``, ``SQL_VIEW``, ``LOGICAL_VIEW``, "
            "``LOGICAL_TABLE`` (physical table), and ``MODEL``. Sourced "
            "from ``metadata_header.type`` in the TS API response and used "
            "by the DataHub source to pick the right ``DatasetSubTypes`` "
            "value when emitting the entity."
        ),
    )

    sql_view_definition: Optional[str] = Field(
        default=None,
        description=(
            "Raw SQL statement for ``SQL_VIEW`` datasets, extracted from "
            "TML. ``None`` for non-SQL_VIEW types and for SQL views the "
            "principal can't TML-export. Populated by "
            "``client.iter_logical_tables`` after the per-table fetch "
            "loop runs a single batched TML export over every SQL_VIEW."
        ),
    )

    data_source_id: Optional[str] = Field(
        default=None,
        description="GUID of the underlying ThoughtSpot connection backing this worksheet.",
    )

    data_source_type: Optional[str] = Field(
        default=None,
        description="Connection type discriminator (e.g. ``DEFAULT``, ``SNOWFLAKE``).",
    )

    physical_database_name: Optional[str] = Field(
        default=None,
        alias="physicalDatabaseName",
        description=(
            "External database/catalog name on the backing source. "
            "Sourced from ``metadata_detail.physicalDatabaseName``. "
            "Falls back to the connection's ``default_database`` when "
            "absent."
        ),
    )

    physical_schema_name: Optional[str] = Field(
        default=None,
        alias="physicalSchemaName",
        description="External schema. Falls back to ``default_schema``.",
    )

    physical_table_name: Optional[str] = Field(
        default=None,
        alias="physicalTableName",
        description=(
            "External table name. Falls back to the TS table's own "
            "``name`` if absent — TS preserves the source name in most "
            "cases."
        ),
    )

    join_count: Optional[int] = Field(
        default=None,
        description=(
            "Number of joins defined on this logical table (worksheet/view). "
            "Sourced from ``metadata_detail.joins`` length. Surfaced as a "
            "``thoughtspot_join_count`` custom property on the emitted "
            "DataHub dataset."
        ),
    )


class ColumnSourceRef(BaseModel):
    """A leaf upstream reference for a logical column's lineage.

    Each entry identifies the physical (table, column) the logical column
    depends on after the connector has walked joins, formulas, and
    logical-column references. The wire shape uses camelCase keys
    (``tableId``, ``columnName``); we keep snake-case Python attributes and
    accept both via pydantic's ``populate_by_name`` so existing dict-shape
    payloads coerce cleanly.
    """

    table_id: str = Field(
        min_length=1,
        alias="tableId",
        description="GUID of the upstream logical/physical table.",
    )
    table_name: Optional[str] = Field(
        default=None,
        alias="tableName",
        description="Display name of the upstream table (best-effort).",
    )
    column_id: Optional[str] = Field(
        default=None,
        alias="columnId",
        description="GUID of the upstream column on ``table_id`` (best-effort).",
    )
    column_name: str = Field(
        min_length=1,
        alias="columnName",
        description="Name of the upstream column on ``table_id``.",
    )

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class ColumnResponse(BaseModel):
    """Represents a ThoughtSpot Column from API responses.

    Columns are individual fields within logical tables, containing
    metadata about data type, aggregation, and relationships.
    They map to DataHub schema fields.
    """

    id: str = Field(min_length=1, description="Unique identifier (GUID) for the column")

    name: str = Field(description="Column name/identifier")

    description: Optional[str] = Field(default=None, description="Column description")

    data_type: Optional[str] = Field(
        default=None,
        description="Data type (e.g., 'INT', 'VARCHAR', 'FLOAT', 'BOOL')",
    )

    column_type: Optional[str] = Field(
        default=None,
        description="Column classification (e.g., 'MEASURE', 'ATTRIBUTE')",
    )

    physical_column_name: Optional[str] = Field(
        default=None,
        description=(
            "External (Databricks/Snowflake/etc.) column name this TS "
            "column maps to. Sourced from ``columns[*].physicalColumnName`` "
            "on the metadata_detail. Usually identical to ``name``, but TS "
            "allows the logical column to be renamed away from the "
            "physical one — when emitting cross-platform fine-grained "
            "lineage we use this field as the authoritative external "
            "column identifier."
        ),
    )

    sources: Optional[List[ColumnSourceRef]] = Field(
        default=None,
        description=(
            "Resolved upstream column references — one entry per leaf "
            "column this logical column depends on (after walking joins, "
            "formulas, and logical-column references). Used to emit "
            "DataHub fine-grained (column-level) lineage."
        ),
    )

    @field_validator("sources", mode="before")
    @classmethod
    def _coerce_sources(
        cls, v: Any, info: ValidationInfo
    ) -> Optional[List["ColumnSourceRef"]]:
        # Tolerate the raw wire dicts produced by TS metadata_detail by
        # coercing them into ``ColumnSourceRef`` instances. Callers that
        # want drops surfaced to the ingestion report pass
        # ``context={COLUMN_SOURCE_DROPS: <list>}`` to ``model_validate``;
        # see ``_coerce_visualizations`` for the same pattern.
        drops = _validation_drop_sink(info, COLUMN_SOURCE_DROPS)
        if v is None:
            return None
        coerced: List[ColumnSourceRef] = []
        for item in v:
            if isinstance(item, ColumnSourceRef):
                coerced.append(item)
            elif isinstance(item, dict):
                try:
                    coerced.append(ColumnSourceRef(**item))
                except Exception as e:
                    reason = (
                        "malformed column source entry "
                        f"tableId={item.get('tableId', 'unknown')}: {e}"
                    )
                    _record_drop(drops, reason)
                    logger.warning(f"Dropping {reason}")
            else:
                reason = f"unrecognised column source entry type={type(item).__name__}"
                _record_drop(drops, reason)
                logger.warning(f"Dropping {reason}")
        return coerced

    author: Optional[ThoughtSpotAuthor] = Field(
        default=None, description="Column creator/owner"
    )

    @field_validator("author", mode="before")
    @classmethod
    def validate_author(cls, v: Any) -> Optional[Union[dict, ThoughtSpotAuthor]]:
        """Handle author field that can be either a string UUID or an object.

        ThoughtSpot API sometimes returns author as just a UUID string instead of
        an expanded object. When this happens, we create a minimal author object
        with just the ID field populated.
        """
        if v is None:
            return None
        if isinstance(v, str):
            # Author is just a UUID string - create minimal object
            return ThoughtSpotAuthor(id=v, name=v)
        # Author is already an object/dict
        return v

    model_config = ConfigDict(extra="ignore")


class TagResponse(BaseModel):
    """Represents a ThoughtSpot tag returned by ``/tags/search``.

    ThoughtSpot tags are user-defined labels attached to liveboards,
    answers, worksheets, and other metadata objects. We surface them in
    DataHub as ``GlobalTagsClass`` aspects on the matching entity.
    """

    id: str = Field(min_length=1, description="Unique tag GUID")
    name: str = Field(
        min_length=1,
        description="Display name of the tag (becomes the urn:li:tag URN segment).",
    )
    color: Optional[str] = Field(
        default=None,
        description="Optional hex color the tag is rendered with in the TS UI.",
    )

    model_config = ConfigDict(extra="ignore")


# LogicalTableResponse forward-references ColumnResponse (which is defined
# below it in this module). Resolve the reference now that both types are
# known so pydantic can validate ``columns: List[ColumnResponse]`` payloads.
LogicalTableResponse.model_rebuild()


class ConnectionResponse(BaseModel):
    """Represents a ThoughtSpot Connection (the configured link to an
    external data source — Databricks / Snowflake / BigQuery / etc.).

    Wire shape is ``/connections/search`` camelCase keys. We accept both
    camelCase (via aliases) and snake_case so internal callers can use
    Python-idiomatic attribute names.
    """

    id: str = Field(min_length=1, description="Connection GUID.")
    name: str = Field(min_length=1, description="Display name (operator-facing).")
    data_source_type: str = Field(
        alias="data_warehouse_type",
        description=(
            "ThoughtSpot-side platform discriminator (e.g. ``DATABRICKS``, "
            "``SNOWFLAKE``). The wire field on ``/connection/search`` is "
            "``data_warehouse_type`` — distinct from the "
            "``dataSourceTypeEnum`` exposed on individual ``LOGICAL_TABLE`` "
            "metadata. Maps to a DataHub platform via the connector's "
            "``_TS_TO_DATAHUB_PLATFORM`` table."
        ),
    )
    default_database: Optional[str] = Field(
        default=None,
        description=(
            "Default database/catalog. TS doesn't expose this at the "
            "connection level on ``/connection/search``; left here for "
            "callers (or future TS versions) that surface it. Used as a "
            "fallback when a logical table's ``physicalDatabaseName`` is "
            "missing."
        ),
    )
    default_schema: Optional[str] = Field(
        default=None,
        description="Default schema. Same fallback role as ``default_database``.",
    )

    model_config = ConfigDict(extra="ignore", populate_by_name=True)
