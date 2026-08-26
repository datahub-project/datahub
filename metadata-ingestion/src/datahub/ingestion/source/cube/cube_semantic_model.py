from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
from datahub.ingestion.source.cube.constants import (
    CUBE_JOIN_EQ_RE,
    CUBE_PLATFORM,
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


def parse_cube_join_sql(sql: Optional[str]) -> List[Tuple[str, str, str]]:
    # Returns (from_column, to_cube, to_column) pairs from Cube's `{CUBE}.x = {y}.z`.
    if not sql:
        return []
    return [
        (match.group(1), match.group(2), match.group(3))
        for match in CUBE_JOIN_EQ_RE.finditer(sql)
    ]


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

    def emit(
        self,
        view: CubeEntity,
        cubes_by_name: Dict[str, CubeEntity],
    ) -> Iterable[MetadataWorkUnit]:
        members = view.visible_members(self.config.include_hidden)
        members_by_cube = self._members_by_cube(view, members)
        field_paths_by_cube: Dict[str, Dict[str, SemanticFieldInput]] = {
            cube_name: {
                field.field_path: field
                for field in (self._semantic_field(m) for m in cube_members)
            }
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
            return

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
            if self.config.include_lineage and cube_name != view.name:
                dataset.set_upstreams([self._cube_dataset_urn_fn(cube_name)])
            datasets.append(dataset)

        if datasets:
            return datasets

        members = view.visible_members(self.config.include_hidden)
        fields = [self._semantic_field(member) for member in members]
        if not fields:
            self.report.warning(
                title="Cube view has no members to emit as a semantic model",
                message=(
                    "Skipping first-class semanticModel emission for this view; "
                    "it has no visible measures or dimensions."
                ),
                context=view.name,
            )
            return None
        return [
            SemanticModelDataset(
                platform=CUBE_PLATFORM,
                name=self._logical_dataset_name(view.name, view.name),
                semantic_model=model_urn,
                alias=view.name,
                schema=fields,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                extra_aspects=self._container_aspects(),
            )
        ]

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
        fallback = referenced[0] if len(referenced) == 1 else view.name
        for member in members:
            cube_name = member.source_cube_name() or fallback
            grouped.setdefault(cube_name, []).append(member)
        return grouped

    def _semantic_field(self, member: CubeMember) -> SemanticFieldInput:
        native_type = member.data_type or ("number" if member.is_measure else "string")
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
            aggregation_function=member.agg_type if member.is_measure else None,
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
        if member.agg_type:
            expression = DialectExpressionInput(
                expression=f"{member.agg_type}({alias}.{member.name})",
                dialect=DialectClass.ANSI_SQL,
            )
        return Metric(
            platform=CUBE_PLATFORM,
            path=f"{self.path}.{view.name}",
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
                continue
            for join in cube.joins:
                if join.name not in known or cube_name not in known:
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
            return None
        cardinality = None
        if join.relationship:
            cardinality = _CUBE_JOIN_RELATIONSHIP_TO_CARDINALITY.get(
                join.relationship.lower()
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
        fields = field_paths_by_cube.setdefault(cube_name, {})
        if column in fields:
            return column
        cube = cubes_by_name.get(cube_name)
        if cube is None:
            return None
        member = next((m for m in cube.members if m.name == column), None)
        if member is None:
            return None
        fields[column] = self._semantic_field(member)
        return column
