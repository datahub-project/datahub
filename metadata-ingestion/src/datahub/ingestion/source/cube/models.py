from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.cube.constants import (
    ENTITY_TYPE_VIEW,
)


def _short_member_name(name: str) -> str:
    # Core reports members as "orders.count"; Cloud reports them as "count".
    # Normalise to the unqualified form so field paths are stable across both.
    return name.split(".")[-1]


def _clean_core_sql(sql: Optional[str]) -> Optional[str]:
    # Cube's /v1/meta returns a cube's `sql` as a backtick-wrapped template
    # string (e.g. "`SELECT * FROM public.orders`"). Strip the wrapping
    # backticks so the SQL can be parsed for lineage.
    if not sql:
        return sql
    stripped = sql.strip()
    if len(stripped) >= 2 and stripped.startswith("`") and stripped.endswith("`"):
        stripped = stripped[1:-1]
    return stripped


def _is_hidden(public: Optional[bool], is_visible: Optional[bool]) -> bool:
    # Cube exposes both the modern `public` flag and the legacy `isVisible`
    # flag; either being explicitly false marks the cube/member as hidden.
    return public is False or is_visible is False


def _names_from(items: List[Any]) -> List[str]:
    # Pre-aggregations/folder members may be reported either as bare strings or
    # as objects carrying a `name`; normalise both to a list of names.
    names: List[str] = []
    for item in items or []:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return names


# Core REST API (/v1/meta) raw models


class RawJoin(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    relationship: Optional[str] = None


class RawHierarchy(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str
    title: Optional[str] = None
    levels: List[str] = Field(default_factory=list)


class RawFolder(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    members: List[Any] = Field(default_factory=list)


class CoreMember(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str
    title: Optional[str] = None
    short_title: Optional[str] = Field(default=None, alias="shortTitle")
    description: Optional[str] = None
    type: Optional[str] = None
    agg_type: Optional[str] = Field(default=None, alias="aggType")
    primary_key: Optional[bool] = Field(default=None, alias="primaryKey")
    # For view members, the underlying cube member this aliases (e.g.
    # "base_orders.count"); used to derive view -> cube column lineage.
    alias_member: Optional[str] = Field(default=None, alias="aliasMember")
    meta: Optional[Dict[str, Any]] = None
    public: Optional[bool] = None
    is_visible: Optional[bool] = Field(default=None, alias="isVisible")
    format: Optional[str] = None
    drill_members: List[str] = Field(default_factory=list, alias="drillMembers")
    cumulative: Optional[bool] = None
    cumulative_total: Optional[bool] = Field(default=None, alias="cumulativeTotal")


class CoreCube(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: str = "cube"
    sql: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    connected_component: Optional[int] = Field(default=None, alias="connectedComponent")
    public: Optional[bool] = None
    is_visible: Optional[bool] = Field(default=None, alias="isVisible")
    file_name: Optional[str] = Field(default=None, alias="fileName")
    measures: List[CoreMember] = Field(default_factory=list)
    dimensions: List[CoreMember] = Field(default_factory=list)
    segments: List[CoreMember] = Field(default_factory=list)
    joins: List[RawJoin] = Field(default_factory=list)
    hierarchies: List[RawHierarchy] = Field(default_factory=list)
    folders: List[RawFolder] = Field(default_factory=list)
    nested_folders: List[RawFolder] = Field(default_factory=list, alias="nestedFolders")
    pre_aggregations: List[Any] = Field(default_factory=list, alias="preAggregations")


class CoreMetaResponse(BaseModel):
    cubes: List[CoreCube] = Field(default_factory=list)


# Cloud Metadata API raw models


class CloudColumnRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: Optional[str] = Field(default=None, alias="schema")
    table: Optional[str] = None
    column: Optional[str] = None


class CloudTableRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: Optional[str] = Field(default=None, alias="schema")
    table: str


class CloudMember(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    agg_type: Optional[str] = Field(default=None, alias="aggType")
    is_primary_key: bool = False
    column_references: List[CloudColumnRef] = Field(default_factory=list)
    member_references: List[str] = Field(default_factory=list)
    meta: Optional[Dict[str, Any]] = None
    public: Optional[bool] = None
    is_visible: Optional[bool] = Field(default=None, alias="isVisible")
    format: Optional[str] = None
    drill_members: List[str] = Field(default_factory=list, alias="drillMembers")
    cumulative: Optional[bool] = None
    cumulative_total: Optional[bool] = Field(default=None, alias="cumulativeTotal")


class CloudEntity(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str
    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    table_references: List[CloudTableRef] = Field(default_factory=list)
    cube_references: List[str] = Field(default_factory=list)
    measures: List[CloudMember] = Field(default_factory=list)
    dimensions: List[CloudMember] = Field(default_factory=list)
    public: Optional[bool] = None
    is_visible: Optional[bool] = Field(default=None, alias="isVisible")
    file_name: Optional[str] = Field(default=None, alias="fileName")
    joins: List[RawJoin] = Field(default_factory=list)
    hierarchies: List[RawHierarchy] = Field(default_factory=list)
    folders: List[RawFolder] = Field(default_factory=list)
    nested_folders: List[RawFolder] = Field(default_factory=list, alias="nestedFolders")
    pre_aggregations: List[Any] = Field(default_factory=list, alias="preAggregations")


class CloudPagination(BaseModel):
    offset: int = 0
    limit: int = 0
    total: int = 0


class CloudEntitiesPayload(BaseModel):
    entities: List[CloudEntity] = Field(default_factory=list)


class CloudEntitiesResponse(BaseModel):
    data: CloudEntitiesPayload = Field(default_factory=CloudEntitiesPayload)
    pagination: Optional[CloudPagination] = None


class CloudDataSource(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str
    type: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")


class CloudDataSourcesPayload(BaseModel):
    data_sources: List[CloudDataSource] = Field(default_factory=list)


class CloudDataSourcesResponse(BaseModel):
    data: CloudDataSourcesPayload = Field(default_factory=CloudDataSourcesPayload)


# Normalised domain models


class CubeColumnReference(BaseModel):
    schema_name: Optional[str] = None
    table: str
    column: Optional[str] = None

    def table_name(self, database: Optional[str] = None) -> str:
        parts = [p for p in (database, self.schema_name, self.table) if p]
        return ".".join(parts)


class CubeJoin(BaseModel):
    name: str
    relationship: Optional[str] = None


class CubeHierarchy(BaseModel):
    name: str
    levels: List[str] = Field(default_factory=list)


class CubeFolder(BaseModel):
    name: str
    members: List[str] = Field(default_factory=list)


class CubeMember(BaseModel):
    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    data_type: Optional[str] = None
    agg_type: Optional[str] = None
    is_measure: bool
    is_primary_key: bool = False
    is_temporal: bool = False
    is_hidden: bool = False
    format: Optional[str] = None
    drill_members: List[str] = Field(default_factory=list)
    cumulative: bool = False
    column_references: List[CubeColumnReference] = Field(default_factory=list)
    member_references: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


def _build_joins(raw: List[RawJoin]) -> List[CubeJoin]:
    return [CubeJoin(name=j.name, relationship=j.relationship) for j in raw]


def _build_hierarchies(raw: List[RawHierarchy]) -> List[CubeHierarchy]:
    return [
        CubeHierarchy(name=h.name, levels=[_short_member_name(x) for x in h.levels])
        for h in raw
    ]


def _build_folders(
    folders: List[RawFolder], nested: List[RawFolder]
) -> List[CubeFolder]:
    return [
        CubeFolder(
            name=f.name,
            members=[_short_member_name(n) for n in _names_from(f.members)],
        )
        for f in [*folders, *nested]
    ]


class CubeEntity(BaseModel):
    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    is_view: bool = False
    is_hidden: bool = False
    file_name: Optional[str] = None
    measures: List[CubeMember] = Field(default_factory=list)
    dimensions: List[CubeMember] = Field(default_factory=list)
    segment_names: List[str] = Field(default_factory=list)
    table_references: List[CubeColumnReference] = Field(default_factory=list)
    cube_references: List[str] = Field(default_factory=list)
    joins: List[CubeJoin] = Field(default_factory=list)
    hierarchies: List[CubeHierarchy] = Field(default_factory=list)
    folders: List[CubeFolder] = Field(default_factory=list)
    pre_aggregation_names: List[str] = Field(default_factory=list)
    sql: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @property
    def members(self) -> List[CubeMember]:
        return [*self.measures, *self.dimensions]

    def visible_members(self, include_hidden: bool) -> List[CubeMember]:
        if include_hidden:
            return self.members
        return [m for m in self.members if not m.is_hidden]

    @classmethod
    def from_core_cube(cls, cube: CoreCube) -> "CubeEntity":
        measures = [
            CubeMember(
                name=_short_member_name(m.name),
                title=m.title or m.short_title,
                description=m.description,
                data_type=m.type,
                agg_type=m.agg_type,
                is_measure=True,
                is_hidden=_is_hidden(m.public, m.is_visible),
                format=m.format,
                drill_members=[_short_member_name(d) for d in m.drill_members],
                cumulative=bool(m.cumulative or m.cumulative_total),
                member_references=[m.alias_member] if m.alias_member else [],
                meta=m.meta or {},
            )
            for m in cube.measures
        ]
        dimensions = [
            CubeMember(
                name=_short_member_name(m.name),
                title=m.title or m.short_title,
                description=m.description,
                data_type=m.type,
                is_measure=False,
                is_primary_key=bool(m.primary_key),
                is_temporal=m.type == "time",
                is_hidden=_is_hidden(m.public, m.is_visible),
                format=m.format,
                member_references=[m.alias_member] if m.alias_member else [],
                meta=m.meta or {},
            )
            for m in cube.dimensions
        ]
        return cls(
            name=cube.name,
            title=cube.title,
            description=cube.description,
            is_view=cube.type == ENTITY_TYPE_VIEW,
            is_hidden=_is_hidden(cube.public, cube.is_visible),
            file_name=cube.file_name,
            measures=measures,
            dimensions=dimensions,
            segment_names=[_short_member_name(s.name) for s in cube.segments],
            joins=_build_joins(cube.joins),
            hierarchies=_build_hierarchies(cube.hierarchies),
            folders=_build_folders(cube.folders, cube.nested_folders),
            pre_aggregation_names=_names_from(cube.pre_aggregations),
            sql=_clean_core_sql(cube.sql),
            meta=cube.meta or {},
        )

    @classmethod
    def from_cloud_entity(cls, entity: CloudEntity) -> "CubeEntity":
        def _convert(member: CloudMember, is_measure: bool) -> CubeMember:
            return CubeMember(
                name=_short_member_name(member.name),
                title=member.title,
                description=member.description,
                data_type=member.type,
                agg_type=member.agg_type if is_measure else None,
                is_measure=is_measure,
                is_primary_key=member.is_primary_key,
                is_temporal=member.type == "time",
                is_hidden=_is_hidden(member.public, member.is_visible),
                format=member.format,
                drill_members=[_short_member_name(d) for d in member.drill_members]
                if is_measure
                else [],
                cumulative=bool(member.cumulative or member.cumulative_total),
                meta=member.meta or {},
                column_references=[
                    CubeColumnReference(
                        schema_name=ref.schema_name,
                        table=ref.table,
                        column=ref.column,
                    )
                    for ref in member.column_references
                    if ref.table
                ],
                member_references=list(member.member_references),
            )

        return cls(
            name=entity.name,
            title=entity.title,
            description=entity.description,
            is_view=entity.type == ENTITY_TYPE_VIEW,
            is_hidden=_is_hidden(entity.public, entity.is_visible),
            file_name=entity.file_name,
            measures=[_convert(m, True) for m in entity.measures],
            dimensions=[_convert(d, False) for d in entity.dimensions],
            table_references=[
                CubeColumnReference(schema_name=ref.schema_name, table=ref.table)
                for ref in entity.table_references
            ],
            cube_references=list(entity.cube_references),
            joins=_build_joins(entity.joins),
            hierarchies=_build_hierarchies(entity.hierarchies),
            folders=_build_folders(entity.folders, entity.nested_folders),
            pre_aggregation_names=_names_from(entity.pre_aggregations),
        )


def _overlay_lineage(base: CubeEntity, lineage: CubeEntity) -> None:
    # The Metadata API carries warehouse/column lineage that /v1/meta lacks;
    # copy it onto the structural base without clobbering existing values.
    if lineage.table_references:
        base.table_references = lineage.table_references
    if lineage.cube_references:
        base.cube_references = lineage.cube_references

    lineage_members = {m.name: m for m in lineage.members}
    for member in base.members:
        source = lineage_members.get(member.name)
        if source is None:
            continue
        if source.column_references:
            member.column_references = source.column_references
        # Prefer the Metadata API's references, but keep /v1/meta's aliasMember
        # (view->cube) references when the Metadata API has none.
        if source.member_references:
            member.member_references = source.member_references


def merge_entities(
    core_entities: List[CubeEntity], cloud_entities: List[CubeEntity]
) -> List[CubeEntity]:
    # /v1/meta provides the structural/presentation metadata (folders,
    # hierarchies, joins, formats, visibility); the Metadata API provides
    # lineage. Merge by entity name so a Cloud ingestion gets both.
    cloud_by_name = {entity.name: entity for entity in cloud_entities}
    merged: List[CubeEntity] = []
    for entity in core_entities:
        lineage = cloud_by_name.get(entity.name)
        if lineage is not None:
            _overlay_lineage(entity, lineage)
        merged.append(entity)

    # Surface entities the Metadata API returns but /v1/meta omits (e.g. cubes
    # marked public: false, which /v1/meta excludes).
    core_names = {entity.name for entity in core_entities}
    merged.extend(entity for entity in cloud_entities if entity.name not in core_names)
    return merged
