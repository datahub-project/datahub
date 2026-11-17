from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
from datahub.ingestion.source.cube.constants import (
    CUBE_JOIN_CUBE_PLACEHOLDER,
    CUBE_JOIN_EQ_RE,
    CUBE_PLATFORM,
    CUBE_TYPE_TO_SCHEMA_FIELD_TYPE,
    KNOWN_MEASURE_AGG_TYPES,
)
from datahub.ingestion.source.cube.models import CubeEntity, CubeJoin, CubeMember
from datahub.metadata.schema_classes import (
    ContainerClass,
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
)
from datahub.metadata.urns import SemanticModelUrn
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    DialectExpressionInput,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

_CUBE_JOIN_RELATIONSHIP_TO_CARDINALITY: Dict[str, str] = {
    "many_to_one": ERModelRelationshipCardinalityClass.N_ONE,
    "belongsto": ERModelRelationshipCardinalityClass.N_ONE,
    "one_to_many": ERModelRelationshipCardinalityClass.ONE_N,
    "hasmany": ERModelRelationshipCardinalityClass.ONE_N,
    "one_to_one": ERModelRelationshipCardinalityClass.ONE_ONE,
    "hasone": ERModelRelationshipCardinalityClass.ONE_ONE,
    "many_to_many": ERModelRelationshipCardinalityClass.N_N,
}


class CubeJoinColumnPair(NamedTuple):
    from_column: str
    to_cube: str
    to_column: str


def parse_cube_join_sql(sql: Optional[str]) -> List[CubeJoinColumnPair]:
    # `{CUBE}` is always normalized to the from side, regardless of which
    # operand of `=` it appeared on in the source SQL.
    if not sql:
        return []
    pairs: List[CubeJoinColumnPair] = []
    for match in CUBE_JOIN_EQ_RE.finditer(sql):
        left_cube, left_col, right_cube, right_col = match.groups()
        if left_cube.upper() == CUBE_JOIN_CUBE_PLACEHOLDER:
            pairs.append(CubeJoinColumnPair(left_col, right_cube, right_col))
        elif right_cube.upper() == CUBE_JOIN_CUBE_PLACEHOLDER:
            pairs.append(CubeJoinColumnPair(right_col, left_cube, left_col))
    return pairs


class CubeSemanticModelMapper:
    def __init__(
        self,
        *,
        config: CubeSourceConfig,
        path: str,
        cube_dataset_urn_fn: Callable[[str], str],
        container_urn: str,
        report: CubeSourceReport,
    ) -> None:
        self.config = config
        self.path = path
        self._cube_dataset_urn_fn = cube_dataset_urn_fn
        self.container_urn = container_urn
        self.report = report
        self.view_chart_inputs: Dict[str, List[str]] = {}
        # view name -> semanticModel urn, set only once a model is fully emitted.
        # Kept separate from view_chart_inputs (which is populated for every
        # outcome, including "skipped" as []) so callers have an unambiguous
        # signal for "did a semanticModel actually get emitted for this view".
        self.emitted_model_urns: Dict[str, str] = {}

    def emit(
        self,
        view: CubeEntity,
        cubes_by_name: Dict[str, CubeEntity],
    ) -> Iterable[MetadataWorkUnit]:
        # Count all measures/dimensions (not just visible_members()), matching
        # _emit_entity's counting for the classic dataset path -- otherwise
        # toggling emit_semantic_model_entities on the same view would change
        # what these counters mean.
        self.report.measures_scanned += len(view.measures)
        self.report.dimensions_scanned += len(view.dimensions)
        members = view.visible_members(self.config.include_hidden)
        members_by_cube = self._dedupe_members_by_cube(
            view, self._members_by_cube(view, members)
        )
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]] = {
            cube_name: {m.name: self._semantic_field(m) for m in cube_members}
            for cube_name, cube_members in members_by_cube.items()
        }

        relationships = self._relationships(view, cubes_by_name, field_paths_by_cube)

        model_urn = str(
            SemanticModelUrn(platform=CUBE_PLATFORM, path=self.path, id=view.name)
        )
        datasets = self._logical_datasets(
            view, cubes_by_name, field_paths_by_cube, model_urn
        )
        if datasets is None:
            # Record an empty mapping so charts do not fall back to a view
            # dataset URN that SM mode never emits.
            self.view_chart_inputs[view.name] = []
            return

        # members_by_cube is already deduped by name (see _dedupe_members_by_cube),
        # so a Metric's id=member.name can't collide with another member of the
        # same cube here.
        dataset_by_alias = {ds.alias: ds for ds in datasets}
        metrics: List[Metric] = []
        for cube_name, cube_members in members_by_cube.items():
            logical = dataset_by_alias.get(cube_name)
            if logical is None:
                continue
            for member in cube_members:
                if not member.is_measure:
                    continue
                metrics.append(
                    self._metric(
                        view=view,
                        member=member,
                        alias=cube_name,
                        logical_urn=str(logical.urn),
                        model_urn=model_urn,
                    )
                )

        model = SemanticModel(
            platform=CUBE_PLATFORM,
            path=self.path,
            id=view.name,
            platform_instance=self.config.platform_instance,
            name=view.title or view.name,
            description=view.description,
            native_definition=view.includes_yaml(),
            datasets=datasets,
            relationships=relationships or None,
            extra_aspects=self._container_aspects(),
        )

        yield from model.as_workunits()
        for dataset in datasets:
            yield from dataset.as_workunits()
        for metric in metrics:
            yield from metric.as_workunits()

        self.view_chart_inputs[view.name] = [str(ds.urn) for ds in datasets]
        self.emitted_model_urns[view.name] = model_urn
        self.report.semantic_models_emitted += 1
        self.report.semantic_model_datasets_emitted += len(datasets)
        self.report.metrics_emitted += len(metrics)
        self.report.report_entity_emitted(is_view=True)

    def _logical_datasets(
        self,
        view: CubeEntity,
        cubes_by_name: Dict[str, CubeEntity],
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]],
        model_urn: str,
    ) -> Optional[List[SemanticModelDataset]]:
        datasets: List[SemanticModelDataset] = []
        for cube_name, fields_by_path in field_paths_by_cube.items():
            if not fields_by_path:
                continue
            cube = cubes_by_name.get(cube_name)
            if cube is None and cube_name != view.name:
                # A stale/renamed aliasMember or cube_references entry: don't
                # fabricate an upstream lineage edge to a cube we just
                # confirmed doesn't exist in the fetched model. Excludes the
                # cube_name == view.name case: that's _members_by_cube's own
                # ambiguous-member fallback bucket (already warned there),
                # not a genuinely missing cube reference.
                self.report.warning(
                    title="Cube reference not found",
                    message=(
                        "The logical dataset will have no description and no "
                        "upstream lineage; the referenced cube was not found "
                        "in the fetched model."
                    ),
                    context=f"{view.name} -> {cube_name}",
                )
            dataset = SemanticModelDataset(
                platform=CUBE_PLATFORM,
                name=self._logical_dataset_name(view.name, cube_name),
                semantic_model=model_urn,
                alias=cube_name,
                schema=list(fields_by_path.values()),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                description=cube.description if cube is not None else None,
                extra_aspects=self._container_aspects(),
            )
            if (
                self.config.include_lineage
                and cube_name != view.name
                and cube is not None
            ):
                dataset.set_upstreams([self._cube_dataset_urn_fn(cube_name)])
            datasets.append(dataset)

        if not datasets:
            self.report.warning(
                title="Cube view has no members to emit as a semantic model",
                message=(
                    "Skipping first-class semanticModel emission for this view; "
                    "it has no visible measures or dimensions."
                ),
                context=view.name,
            )
            return None
        return datasets

    def _logical_dataset_name(self, view_name: str, cube_name: str) -> str:
        # Dataset URN constructor prefixes platform_instance; do not bake it in.
        if self.config.platform_instance:
            return f"{view_name}.{cube_name}"
        return f"{self.path}.{view_name}.{cube_name}"

    def _container_aspects(self) -> List[ContainerClass]:
        return [ContainerClass(container=self.container_urn)]

    def _members_by_cube(
        self, view: CubeEntity, members: List[CubeMember]
    ) -> Dict[str, List[CubeMember]]:
        referenced = view.referenced_cube_names()
        grouped: Dict[str, List[CubeMember]] = {name: [] for name in referenced}
        # Only a single-cube view has an unambiguous fallback cube; a
        # multi-cube view with a member that can't be attributed to a specific
        # cube (e.g. missing aliasMember on Cube Core) has no good answer, so
        # it's bucketed under the view's own (non-cube) name and reported.
        single_cube_fallback = referenced[0] if len(referenced) == 1 else None
        for member in members:
            cube_name = member.source_cube_name()
            if cube_name is None:
                if single_cube_fallback is not None:
                    cube_name = single_cube_fallback
                else:
                    cube_name = view.name
                    self.report.warning(
                        title="Could not attribute Cube view member to a cube",
                        message=(
                            "This member could not be matched to one of the "
                            "view's referenced cubes; it will still be emitted, "
                            "but in a fallback logical dataset aliased to the "
                            "view itself, with no cube-level description or "
                            "upstream lineage."
                        ),
                        context=f"{view.name}.{member.name}",
                    )
            grouped.setdefault(cube_name, []).append(member)
        return grouped

    def _dedupe_members_by_cube(
        self, view: CubeEntity, members_by_cube: Dict[str, List[CubeMember]]
    ) -> Dict[str, List[CubeMember]]:
        # A duplicate name within one cube would otherwise silently collide:
        # last-write-wins in the schema-field dict keyed by field_path, but
        # first-write-wins if a naive per-loop dedup were applied only to
        # measures (as an earlier version of this fix did) -- giving the
        # metric and its own schema field two different descriptions for
        # "the same" field. Dedupe once, up front, for both measures and
        # dimensions, so every downstream consumer sees the same member.
        deduped: Dict[str, List[CubeMember]] = {}
        for cube_name, cube_members in members_by_cube.items():
            seen: Set[str] = set()
            kept: List[CubeMember] = []
            for member in cube_members:
                if member.name in seen:
                    self.report.warning(
                        title="Duplicate Cube member name",
                        message=(
                            "Skipping duplicate member; only the first "
                            "occurrence is emitted (as a schema field, and as "
                            "a metric if it's a measure)."
                        ),
                        context=f"{view.name}.{cube_name}.{member.name}",
                    )
                    continue
                seen.add(member.name)
                kept.append(member)
            deduped[cube_name] = kept
        return deduped

    @staticmethod
    def _measure_agg_type(member: CubeMember) -> Optional[str]:
        # Cube Cloud view members often report the aggregation only via
        # `type` (e.g. "count") and leave `aggType` unset.
        if member.agg_type:
            return member.agg_type
        # "number" is Cube's type for a measure with no built-in aggregation
        # (e.g. a custom SQL measure); it isn't a real aggregation function.
        if member.data_type in KNOWN_MEASURE_AGG_TYPES and member.data_type != "number":
            return member.data_type
        return None

    @staticmethod
    def _resolve_geo_type(data_type: Optional[str]) -> Optional[str]:
        # "geo" has no dedicated DataHub SQL-type primitive -- resolve_sql_type
        # returns None for it, which resolves to NullType downstream. Surface
        # it as a string instead, matching the classic dataset path's
        # CUBE_TYPE_TO_SCHEMA_FIELD_TYPE mapping (geo -> StringTypeClass).
        # Cube's own docs describe geo as primarily a dimension type, but a
        # calculated geo measure is possible too, so both paths need this.
        return "string" if data_type == "geo" else data_type

    @staticmethod
    def _measure_field_type(member: CubeMember) -> str:
        # A measure's `data_type` is usually an aggregation type (e.g.
        # "count"), not a SQL/native type -- passing it through as-is
        # resolves to NullType downstream, so default to numeric. But a
        # calculated measure can carry a real primitive type (string,
        # boolean, time, date, geo); those must pass through, matching the
        # classic dataset path's CUBE_TYPE_TO_SCHEMA_FIELD_TYPE lookup.
        data_type = CubeSemanticModelMapper._resolve_geo_type(member.data_type)
        if data_type in CUBE_TYPE_TO_SCHEMA_FIELD_TYPE:
            return data_type or "number"
        return "number"

    def _semantic_field(self, member: CubeMember) -> SemanticFieldInput:
        native_type = (
            self._measure_field_type(member)
            if member.is_measure
            else (self._resolve_geo_type(member.data_type) or "string")
        )
        return SemanticFieldInput(
            field_path=member.name,
            type=native_type,
            semantic_type=(
                SemanticFieldTypeClass.MEASURE
                if member.is_measure
                else SemanticFieldTypeClass.DIMENSION
            ),
            description=member.description or member.title,
            is_part_of_key=member.is_primary_key,
            is_time_dimension=member.is_temporal,
            aggregation_function=(
                self._measure_agg_type(member) if member.is_measure else None
            ),
        )

    def _metric(
        self,
        *,
        view: CubeEntity,
        member: CubeMember,
        alias: str,
        logical_urn: str,
        model_urn: str,
    ) -> Metric:
        expression = None
        agg_type = self._measure_agg_type(member)
        if agg_type:
            expression = DialectExpressionInput(
                expression=f"{agg_type}({alias}.{member.name})",
                dialect=DialectClass.ANSI_SQL,
            )
        return Metric(
            platform=CUBE_PLATFORM,
            path=f"{self.path}.{view.name}.{alias}",
            id=member.name,
            semantic_model=model_urn,
            platform_instance=self.config.platform_instance,
            name=member.title or member.name,
            description=member.description,
            expression=expression,
            upstream_datasets=[logical_urn],
        )

    def _relationships(
        self,
        view: CubeEntity,
        cubes_by_name: Dict[str, CubeEntity],
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]],
    ) -> List[SemanticModelRelationshipInput]:
        known = set(field_paths_by_cube)
        rels: List[SemanticModelRelationshipInput] = []
        seen: Set[Tuple[str, str]] = set()
        for cube_name in view.referenced_cube_names():
            cube = cubes_by_name.get(cube_name)
            if cube is None:
                self.report.warning(
                    title="Cube reference not found",
                    message=(
                        "Skipping this cube's joins as semantic-model "
                        "relationships; it was not found in the fetched model."
                    ),
                    context=f"{view.name} -> {cube_name}",
                )
                continue
            for join in cube.joins:
                if join.name not in known:
                    continue
                key = (cube_name, join.name)
                if key in seen:
                    continue
                rel = self._relationship_from_join(
                    join,
                    from_alias=cube_name,
                    cubes_by_name=cubes_by_name,
                    field_paths_by_cube=field_paths_by_cube,
                )
                if rel is None:
                    continue
                seen.add(key)
                rels.append(rel)
        return rels

    def _relationship_from_join(
        self,
        join: CubeJoin,
        *,
        from_alias: str,
        cubes_by_name: Dict[str, CubeEntity],
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]],
    ) -> Optional[SemanticModelRelationshipInput]:
        pairs = parse_cube_join_sql(join.sql)
        if not pairs:
            if join.sql and join.sql.strip():
                self.report.warning(
                    title="Could not parse Cube join SQL",
                    message=(
                        "Skipping this join as a semantic-model relationship; "
                        "expected `{CUBE}.col = {other}.col` (either order)."
                    ),
                    context=f"{from_alias}->{join.name}: {join.sql}",
                )
            return None
        from_columns: List[str] = []
        to_columns: List[str] = []
        for from_col, to_cube, to_col in pairs:
            if to_cube != join.name:
                continue
            from_field = self._ensure_join_column(
                from_alias, from_col, cubes_by_name, field_paths_by_cube
            )
            to_field = self._ensure_join_column(
                join.name, to_col, cubes_by_name, field_paths_by_cube
            )
            if from_field is None or to_field is None:
                continue
            from_columns.append(from_field)
            to_columns.append(to_field)
        if not from_columns:
            # The SQL parsed, but every pair either targeted a different cube
            # than this join, or named a column that doesn't exist on either
            # side (e.g. a typo) -- unlike the unparseable-SQL case above,
            # this degraded silently before.
            self.report.warning(
                title="Could not resolve Cube join columns",
                message=(
                    "Skipping this join as a semantic-model relationship; its "
                    "SQL parsed, but no column pair resolved to real members "
                    "of both cubes."
                ),
                context=f"{from_alias}->{join.name}: {join.sql}",
            )
            return None
        cardinality = None
        if join.relationship:
            cardinality = _CUBE_JOIN_RELATIONSHIP_TO_CARDINALITY.get(
                join.relationship.lower()
            )
            if cardinality is None:
                self.report.warning(
                    title="Unrecognized Cube join relationship",
                    message=(
                        "This relationship will be emitted without a cardinality."
                    ),
                    context=f"{from_alias}->{join.name}: {join.relationship}",
                )
        return SemanticModelRelationshipInput(
            from_alias=from_alias,
            from_columns=from_columns,
            to_alias=join.name,
            to_columns=to_columns,
            name=f"{from_alias}_to_{join.name}",
            cardinality=cardinality,
        )

    def _ensure_join_column(
        self,
        cube_name: str,
        column: str,
        cubes_by_name: Dict[str, CubeEntity],
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]],
    ) -> Optional[str]:
        # Join keys often are not in the view's `includes`; pull them from the
        # cube so the relationship still has columns the SDK can validate.
        # visible_members(), not the raw member list: every other schema/
        # lineage path in this connector treats include_hidden as an absolute
        # filter, and a hidden FK/PK used only as a join key (a common Cube
        # modeling pattern) must not become an unintended exception to that.
        #
        # `column` is the raw SQL identifier from the join SQL
        # ("{CUBE}.user_id"), which is commonly a Cube member's own `sql:`
        # expression rather than its JS-identifier `name` ("userId" with
        # `sql: user_id` is a common pattern) -- match either, and always key
        # `fields`/return by member.name, since that's the field_path
        # _semantic_field actually gives the schema field.
        cube = cubes_by_name.get(cube_name)
        if cube is None:
            return None
        member = next(
            (
                m
                for m in cube.visible_members(self.config.include_hidden)
                if m.name == column or m.sql_column == column
            ),
            None,
        )
        if member is None:
            return None
        fields = field_paths_by_cube.setdefault(cube_name, {})
        if member.name in fields:
            return member.name
        fields[member.name] = self._semantic_field(member)
        return member.name
