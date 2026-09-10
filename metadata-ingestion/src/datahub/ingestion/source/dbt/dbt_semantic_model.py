import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_tag_urn
from datahub.errors import SdkUsageError
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    METRIC_TYPE_RATIO,
    METRIC_TYPE_SIMPLE,
    DBTCommonConfig,
    DBTMetric,
    DBTNode,
    DBTSemanticDimension,
    DBTSemanticEntity,
    DBTSemanticMeasure,
    DBTSemanticModelDefinition,
    DBTSourceReport,
    get_upstreams,
)
from datahub.metadata.schema_classes import (
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
    SubTypesClass,
)
from datahub.metadata.urns import MetricUrn, SemanticModelUrn
from datahub.sdk.dataset import UpstreamLineageInputType
from datahub.sdk.entity import Entity
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    DialectExpressionInput,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

logger = logging.getLogger(__name__)

# A dbt project's semantic layer is modelled as a single semanticModel whose
# `path` carries the project. A constant id keeps `id` free for any future
# sub-project scoping, matching MicroStrategy's _SEMANTIC_MODEL_ID.
SEMANTIC_MODEL_ID = "semantic_layer"

# dbt supplies no SQL types for entities/dimensions/measures, so native types
# are synthesized. "timestamp" is deliberately absent: resolve_sql_type merges
# SQL Server's map last, where timestamp is a binary type, not a datetime.
_FIELD_TYPE_STRING = "string"
_FIELD_TYPE_NUMBER = "number"
_FIELD_TYPE_DATE = "date"
_FIELD_TYPE_DATETIME = "datetime"

_GRANULARITY_TO_FIELD_TYPE: Dict[str, str] = {
    "nanosecond": _FIELD_TYPE_DATETIME,
    "microsecond": _FIELD_TYPE_DATETIME,
    "millisecond": _FIELD_TYPE_DATETIME,
    "second": _FIELD_TYPE_DATETIME,
    "minute": _FIELD_TYPE_DATETIME,
    "hour": _FIELD_TYPE_DATETIME,
    "day": _FIELD_TYPE_DATE,
    "week": _FIELD_TYPE_DATE,
    "month": _FIELD_TYPE_DATE,
    "quarter": _FIELD_TYPE_DATE,
    "year": _FIELD_TYPE_DATE,
}

# MDX / TABLEAU / MAQL are BI-tool dialects and can never be a dbt adapter.
_TARGET_PLATFORM_TO_DIALECT: Dict[str, str] = {
    "snowflake": DialectClass.SNOWFLAKE,
    "databricks": DialectClass.DATABRICKS,
    "spark": DialectClass.DATABRICKS,
}


def _metric_subtype(metric_type: str) -> str:
    """Title-case dbt's metric type for display, e.g. "cumulative" -> Cumulative.

    dbt documents the set as conversion, cumulative, derived, ratio and simple;
    an unrecognized value is passed through title-cased rather than dropped, so
    a future dbt type still classifies something.
    """
    return metric_type.replace("_", " ").title()


_SEMANTIC_MODEL_DEPENDS_ON_PREFIX = "semantic_model."
_METRIC_DEPENDS_ON_PREFIX = "metric."


@dataclass(frozen=True)
class _JoinTarget:
    """A resolved join target: the owning dataset's alias and its field path."""

    alias: str
    field_path: str


@dataclass
class _ResolvedMetricInputs:
    """A metric's inputs, split by what they turned out to reference."""

    metric_urns: List[str] = field(default_factory=list)
    measure_names: List[str] = field(default_factory=list)


@dataclass
class _MeasureIndex:
    """Measure lookups a top-level metric needs, keyed by casefolded name."""

    dataset_urn_by_measure: Dict[str, str] = field(default_factory=dict)
    expression_by_measure: Dict[str, str] = field(default_factory=dict)
    dataset_urn_by_dbt_name: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class _PreparedModel:
    """One dbt semantic model, resolved into everything emission needs.

    Replaces four collections keyed by alias and dbt_name that had to be
    re-joined by every downstream step.
    """

    node: DBTNode
    definition: DBTSemanticModelDefinition
    alias: str
    fields: Dict[str, SemanticFieldInput]
    dataset: SemanticModelDataset


class DbtSemanticModelMapper:
    """Maps dbt semantic models and metrics to first-class DataHub entities.

    One semanticModel per dbt project, one logical dataset (subtype "Semantic
    Model Dataset") per dbt semantic model, and one metric per `create_metric`
    measure and per top-level `metrics:` definition. Relationships are derived
    from the entity primary/foreign name matching that MetricFlow itself joins
    on.
    """

    def __init__(
        self,
        *,
        config: DBTCommonConfig,
        report: DBTSourceReport,
        project_name: str,
    ) -> None:
        self.config = config
        self.report = report
        self.project_name = project_name
        self.path = self._build_path(config.platform_instance, project_name)
        self.model_urn = str(
            SemanticModelUrn(
                platform=DBT_PLATFORM, path=self.path, id=SEMANTIC_MODEL_ID
            )
        )
        self.dialect = _TARGET_PLATFORM_TO_DIALECT.get(
            (config.target_platform or "").lower(), DialectClass.ANSI_SQL
        )

    @staticmethod
    def _build_path(platform_instance: Optional[str], project_name: str) -> str:
        # semanticModelKey/metricKey have no platform_instance field, so the
        # instance is folded into the path. DataHub's documented multi-project
        # dbt setup sets platform_instance to the project name, so avoid
        # emitting "analytics.analytics" in that (common) case.
        if not platform_instance:
            return project_name
        if platform_instance == project_name:
            return platform_instance
        return f"{platform_instance}.{project_name}"

    def emit(
        self,
        *,
        semantic_model_nodes: List[DBTNode],
        metric_definitions: List[DBTMetric],
        all_nodes_map: Dict[str, DBTNode],
    ) -> Iterable[MetadataWorkUnit]:
        models = self._prepare(semantic_model_nodes, all_nodes_map)
        if not models:
            if semantic_model_nodes or metric_definitions:
                self.report.warning(
                    title="No dbt semantic models could be emitted",
                    message="No semanticModel, Semantic Model Dataset or metric "
                    "entities were produced. Metrics are attached to a semantic "
                    "model, so none can be emitted either.",
                    context=self.project_name,
                )
            return

        relationships = self._relationships(models)
        metrics = self._metrics(models, metric_definitions)

        model = SemanticModel(
            platform=DBT_PLATFORM,
            path=self.path,
            id=SEMANTIC_MODEL_ID,
            platform_instance=self.config.platform_instance,
            name=self.project_name,
            datasets=[prepared.dataset for prepared in models],
            relationships=relationships or None,
        )

        # The SDK validates strictly at as_mcps() time and raises SdkUsageError
        # on a bad alias or an unresolvable join column. Materialize up front so
        # a modelling problem degrades rather than aborting the run -- but only
        # SdkUsageError: any other exception is a bug in this mapper and must
        # surface as a crash rather than be laundered into a warning.
        try:
            workunits = list(model.as_workunits())
        except SdkUsageError as e:
            # The semanticModel carries the relationships and owns every
            # dataset, so losing it means the whole project is unusable.
            self.report.failure(
                title="Failed to emit dbt semantic model entities",
                message="No semanticModel, Semantic Model Dataset or metric "
                "entities were emitted for this project. Fix the reported "
                "modelling problem, or unset emit_semantic_model_entities to "
                "fall back to emitting semantic models as datasets.",
                context=f"dbt project {self.project_name}",
                exc=e,
            )
            return

        emitted_datasets = 0
        for prepared in models:
            # Per-entity, so one unrepresentable dataset does not cost the
            # whole project.
            entity_workunits = self._entity_workunits(
                prepared.dataset, f"semantic model {prepared.node.dbt_name}"
            )
            if entity_workunits is None:
                self.report.num_semantic_model_datasets_dropped += 1
                continue
            workunits.extend(entity_workunits)
            emitted_datasets += 1

        emitted_metrics = 0
        for metric in metrics:
            entity_workunits = self._entity_workunits(metric, f"metric {metric.urn}")
            if entity_workunits is None:
                self.report.num_metrics_dropped += 1
                continue
            workunits.extend(entity_workunits)
            emitted_metrics += 1

        # Counted before yielding, so the report is accurate even if the
        # pipeline stops part-way through the stream.
        self.report.num_semantic_model_entities_emitted += 1
        self.report.num_semantic_model_datasets_emitted += emitted_datasets
        self.report.num_semantic_model_relationships_emitted += len(relationships)
        self.report.num_metrics_emitted += emitted_metrics

        yield from workunits

    def _entity_workunits(
        self, entity: Entity, context: str
    ) -> Optional[List[MetadataWorkUnit]]:
        try:
            return list(entity.as_workunits())
        except SdkUsageError as e:
            self.report.warning(
                title="Failed to emit a dbt semantic model entity",
                message="Skipping this entity; the rest of the project's "
                "semantic model is still emitted.",
                context=context,
                exc=e,
            )
            return None

    def _prepare(
        self, semantic_model_nodes: List[DBTNode], all_nodes_map: Dict[str, DBTNode]
    ) -> List[_PreparedModel]:
        """Resolve each semantic model into everything downstream steps need.

        Sorted by dbt_name so alias and metric deduplication is stable across
        runs rather than dependent on manifest iteration order.
        """
        nodes = sorted(
            (node for node in semantic_model_nodes if self._has_definition(node)),
            key=lambda node: node.dbt_name,
        )
        alias_by_dbt_name = self._build_aliases(nodes)

        models: List[_PreparedModel] = []
        for node in nodes:
            definition = node.semantic_model_def
            if definition is None:
                continue  # unreachable: filtered by _has_definition
            alias = alias_by_dbt_name[node.dbt_name]
            fields = self._semantic_fields(node, definition)
            if not fields:
                self.report.warning(
                    title="dbt semantic model has no usable fields",
                    message="Skipping this semantic model; it declares no "
                    "entities, dimensions or measures that could be emitted as "
                    "schema fields.",
                    context=node.dbt_name,
                )
                self.report.semantic_models_skipped.append(node.dbt_name)
                continue
            models.append(
                _PreparedModel(
                    node=node,
                    definition=definition,
                    alias=alias,
                    fields=fields,
                    dataset=SemanticModelDataset(
                        platform=DBT_PLATFORM,
                        name=self._logical_dataset_name(alias),
                        semantic_model=self.model_urn,
                        alias=alias,
                        schema=list(fields.values()),
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                        description=node.description or None,
                        upstreams=self._upstreams(node, all_nodes_map) or None,
                    ),
                )
            )
        return models

    def _has_definition(self, node: DBTNode) -> bool:
        definition = node.semantic_model_def
        if definition is None or definition.is_empty():
            self.report.warning(
                title="dbt semantic model has no definition",
                message="Skipping this semantic model; its entities, dimensions "
                "and measures are all absent or empty.",
                context=node.dbt_name,
            )
            self.report.semantic_models_skipped.append(node.dbt_name)
            return False
        return True

    def _build_aliases(self, nodes: List[DBTNode]) -> Dict[str, str]:
        """Assign a unique alias per semantic model.

        The alias is the join identity in relationships and drives the logical
        dataset name, and the SDK raises when two attached datasets share one.
        Two semantic models can share a `name` across dbt packages, so
        disambiguate with the package name and then a numeric suffix.

        Uniqueness is checked in the same case space the dataset URN uses:
        with convert_urns_to_lowercase (the dbt default), "Orders" and "orders"
        are distinct aliases but would collide on one URN.
        """
        aliases: Dict[str, str] = {}
        taken: Set[str] = set()

        def key(alias: str) -> str:
            return alias.lower() if self.config.convert_urns_to_lowercase else alias

        for node in nodes:
            candidates = [node.name]
            if node.dbt_package_name:
                candidates.append(f"{node.dbt_package_name}_{node.name}")
            alias = next(
                (c for c in candidates if c and c.strip() and key(c) not in taken), None
            )
            if alias is None:
                base = node.name if node.name and node.name.strip() else node.dbt_name
                suffix = 2
                while key(f"{base}_{suffix}") in taken:
                    suffix += 1
                alias = f"{base}_{suffix}"
            if alias != node.name:
                self.report.warning(
                    title="Duplicate dbt semantic model name",
                    message="Two semantic models resolve to the same name, so "
                    "this one was given a disambiguated alias. Its logical "
                    "dataset URN differs from the semantic model's name.",
                    context=f"{node.dbt_name} -> {alias}",
                )
            taken.add(key(alias))
            aliases[node.dbt_name] = alias
        return aliases

    def _logical_dataset_name(self, alias: str) -> str:
        # platform_instance is applied by the Dataset URN builder, so it must
        # not be baked in here -- unlike self.path, which must contain it.
        #
        # Three parts, not two: DBTNode.get_db_fqn drops a falsy database, so a
        # plain dbt node can also be "<schema>.<name>". A two-part
        # "<project>.<alias>" could therefore collide with a real model whose
        # schema happens to be named after the project, and the semantic
        # model's schema would land on that model's dataset. The constant
        # segment matches the semanticModel URN's id.
        name = f"{self.project_name}.{SEMANTIC_MODEL_ID}.{alias}"
        # Mirror DBTNode.get_urn's casing rule: AutoLowercaseUrnsProcessor only
        # runs when convert_urns_to_lowercase is set explicitly in the recipe,
        # so relying on it alone would give mixed casing against the physical
        # upstream URN.
        return name.lower() if self.config.convert_urns_to_lowercase else name

    def _upstreams(
        self, node: DBTNode, all_nodes_map: Dict[str, DBTNode]
    ) -> UpstreamLineageInputType:
        # Reuses the shared helper so semantic-model lineage honours
        # skip_sources_in_lineage, ephemeral nodes and target_platform_instance
        # exactly as every other dbt edge does.
        urns: List[str] = get_upstreams(
            upstreams=node.upstream_nodes,
            all_nodes=all_nodes_map,
            target_platform=self.config.target_platform,
            target_platform_instance=self.config.target_platform_instance,
            environment=self.config.env,
            platform_instance=self.config.platform_instance,
            skip_sources_in_lineage=self.config.skip_sources_in_lineage,
        )
        # List is invariant, so widen the element type for the SDK's input alias.
        return list(urns)

    def _semantic_fields(
        self, node: DBTNode, definition: DBTSemanticModelDefinition
    ) -> Dict[str, SemanticFieldInput]:
        """Build the logical dataset's schema, keyed by field path.

        dbt only enforces name uniqueness within each of entities/dimensions/
        measures, so a name can repeat across the three kinds. The SDK raises
        on a duplicate field_path, so dedupe here with entities > dimensions >
        measures precedence.
        """
        fields: Dict[str, SemanticFieldInput] = {}
        key_entity_names = self._key_entity_names(definition)

        for entity in definition.entities:
            self._add_field(
                fields,
                node,
                entity.name,
                "entity",
                self._entity_field(entity, key_entity_names),
            )
        for dimension in definition.dimensions:
            self._add_field(
                fields,
                node,
                dimension.name,
                "dimension",
                self._dimension_field(dimension),
            )
        for measure in definition.measures:
            self._add_field(
                fields, node, measure.name, "measure", self._measure_field(measure)
            )
        return fields

    def _add_field(
        self,
        fields: Dict[str, SemanticFieldInput],
        node: DBTNode,
        name: str,
        kind: str,
        field_input: SemanticFieldInput,
    ) -> None:
        if not name or not name.strip():
            self.report.warning(
                title="dbt semantic model field has no name",
                message="Skipping an unnamed entity, dimension or measure; it "
                "cannot be emitted as a schema field.",
                context=f"{node.dbt_name} ({kind})",
            )
            return
        if name in fields:
            self.report.warning(
                title="Duplicate dbt semantic model field name",
                message="Skipping this field; one with the same name was already "
                "emitted (entities take precedence over dimensions, which take "
                "precedence over measures).",
                context=f"{node.dbt_name}.{name} ({kind})",
            )
            return
        fields[name] = field_input

    @staticmethod
    def _key_entity_names(definition: DBTSemanticModelDefinition) -> Set[str]:
        """Entity names that identify a row, and so are valid join targets."""
        names = {entity.name for entity in definition.entities if entity.is_key}
        if definition.primary_entity:
            names.add(definition.primary_entity)
        return names

    def _expression(self, expr: Optional[str]) -> Optional[DialectExpressionInput]:
        # build_metric_expression rejects a blank expression, and the SDK
        # synthesizes "alias.field_path" when none is given -- which is what
        # Snowflake's mapper does too.
        if not expr or not expr.strip():
            return None
        return DialectExpressionInput(expression=expr, dialect=self.dialect)

    def _entity_field(
        self, entity: DBTSemanticEntity, key_entity_names: Set[str]
    ) -> SemanticFieldInput:
        # A MetricFlow entity is a groupable, joinable column, which is what a
        # dimension is; FILTER and OTHER fit nothing dbt produces.
        return SemanticFieldInput(
            field_path=entity.name,
            type=_FIELD_TYPE_STRING,
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=entity.description or None,
            is_part_of_key=entity.name in key_entity_names,
            expression=self._expression(entity.expr),
        )

    def _dimension_field(self, dimension: DBTSemanticDimension) -> SemanticFieldInput:
        if dimension.is_time:
            field_type = _GRANULARITY_TO_FIELD_TYPE.get(
                (dimension.time_granularity or "").lower(), _FIELD_TYPE_DATE
            )
        else:
            field_type = _FIELD_TYPE_STRING
        return SemanticFieldInput(
            field_path=dimension.name,
            type=field_type,
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=dimension.description or None,
            is_time_dimension=dimension.is_time,
            expression=self._expression(dimension.expr),
        )

    def _measure_field(self, measure: DBTSemanticMeasure) -> SemanticFieldInput:
        agg = (measure.agg or "").strip().lower()
        return SemanticFieldInput(
            field_path=measure.name,
            type=_FIELD_TYPE_NUMBER,
            semantic_type=SemanticFieldTypeClass.MEASURE,
            description=measure.description or None,
            aggregation_function=agg or None,
            expression=self._expression(measure.expr),
        )

    def _relationships(
        self, models: List[_PreparedModel]
    ) -> List[SemanticModelRelationshipInput]:
        """Derive relationships from MetricFlow's own join semantics.

        MetricFlow joins semantic model A to B when A declares an entity named
        X as foreign (or unique/natural) and B declares X as a key. Entity
        names need not be unique across semantic models -- that name match *is*
        the join. The referencing model is the many side, so cardinality is
        N_ONE; a `unique` target is not one-to-one, since dbt documents joining
        a single unique key to multiple foreign keys.
        """
        # Keyed by the folded name so a case-differing reference resolves, and
        # valued with the field path actually present in the target's schema so
        # the join column the SDK validates is the one that exists.
        owners_by_entity: Dict[str, List[_JoinTarget]] = defaultdict(list)
        for prepared in models:
            for name in sorted(self._key_entity_names(prepared.definition)):
                field_path = self._field_path(prepared, name)
                if field_path is None:
                    # A primary_entity naming no `entities:` entry has no
                    # column, so it cannot be a join target. dbt allows this
                    # precisely for models with no such column, so emitting a
                    # synthetic field would fabricate one.
                    self.report.semantic_model_relationships_unresolved.append(
                        f"{prepared.node.dbt_name}.{name} (declared as a key but "
                        "has no matching entity, so it cannot be joined to)"
                    )
                    continue
                owners_by_entity[name.casefold()].append(
                    _JoinTarget(alias=prepared.alias, field_path=field_path)
                )

        relationships: List[SemanticModelRelationshipInput] = []
        seen: Set[Tuple[str, str, str]] = set()
        for prepared in models:
            for entity in sorted(prepared.definition.entities, key=lambda e: e.name):
                if not entity.is_join_source:
                    continue
                from_field = self._field_path(prepared, entity.name)
                if from_field is None:
                    self.report.warning(
                        title="dbt semantic model join column not in schema",
                        message="Skipping this relationship; the referencing "
                        "entity was dropped from the schema, most likely as a "
                        "duplicate field name.",
                        context=f"{prepared.node.dbt_name}.{entity.name}",
                    )
                    continue
                owners = [
                    owner
                    for owner in owners_by_entity.get(entity.name.casefold(), [])
                    # A self-join is meaningless as a relationship, and
                    # from_ == to would read as a loop in the UI.
                    if owner.alias != prepared.alias
                ]
                if not owners:
                    # Legitimate when the target model is outside the ingested
                    # scope, so this is a counter rather than a warning.
                    self.report.semantic_model_relationships_unresolved.append(
                        f"{prepared.node.dbt_name}.{entity.name}"
                    )
                    continue
                if len(owners) > 1:
                    self.report.warning(
                        title="Ambiguous dbt semantic model join",
                        message="More than one semantic model declares this "
                        "entity as a key, so the join target is ambiguous. "
                        "Skipping this relationship rather than guessing.",
                        context=f"{prepared.node.dbt_name}.{entity.name} -> "
                        f"{sorted(owner.alias for owner in owners)}",
                    )
                    continue
                target = owners[0]
                key = (prepared.alias, target.alias, entity.name.casefold())
                if key in seen:
                    continue
                seen.add(key)
                relationships.append(
                    SemanticModelRelationshipInput(
                        from_alias=prepared.alias,
                        # The join column is always the entity's name, never its
                        # `expr`: `name` is the field_path in the schema, and the
                        # SDK raises on a join column it cannot find there.
                        from_columns=[from_field],
                        to_alias=target.alias,
                        to_columns=[target.field_path],
                        name=f"{prepared.alias}_to_{target.alias}_on_{entity.name}",
                        cardinality=ERModelRelationshipCardinalityClass.N_ONE,
                    )
                )
        return relationships

    @staticmethod
    def _field_path(prepared: _PreparedModel, name: str) -> Optional[str]:
        """The schema field path for an entity name, matched case-insensitively.

        Resolving owners case-insensitively but checking schema membership
        case-sensitively would resolve a join and then fail it.
        """
        if name in prepared.fields:
            return name
        folded = name.casefold()
        return next(
            (path for path in prepared.fields if path.casefold() == folded), None
        )

    def _index_measures(self, models: List[_PreparedModel]) -> _MeasureIndex:
        """Index measures by name so top-level metrics can resolve their inputs.

        Measure names are unique per semantic model but not across a project,
        so a duplicated name is ambiguous. It is reported and attributed to the
        first model in sort order, matching how an ambiguous join target is
        reported rather than silently resolved.
        """
        index = _MeasureIndex()
        for prepared in models:
            dataset_urn = str(prepared.dataset.urn)
            index.dataset_urn_by_dbt_name[prepared.node.dbt_name] = dataset_urn
            for measure in prepared.definition.measures:
                key = measure.name.casefold()
                if key in index.dataset_urn_by_measure:
                    self.report.warning(
                        title="Ambiguous dbt measure name",
                        message="More than one semantic model declares a measure "
                        "with this name, so a metric referencing it by name is "
                        "ambiguous. Attributing it to the first model in name "
                        "order.",
                        context=f"{prepared.node.dbt_name}.{measure.name}",
                    )
                    continue
                index.dataset_urn_by_measure[key] = dataset_urn
                expression = self._measure_expression(measure, prepared.alias)
                if expression:
                    index.expression_by_measure[key] = expression
        return index

    def _metrics_from_measures(self, models: List[_PreparedModel]) -> Dict[str, Metric]:
        """One metric per measure that sets `create_metric: true`."""
        metrics: Dict[str, Metric] = {}
        for prepared in models:
            for measure in prepared.definition.measures:
                if not measure.create_metric:
                    continue
                key = measure.name.casefold()
                if key in metrics:
                    self.report.warning(
                        title="Duplicate dbt metric name",
                        message="Two measures with create_metric share a name. "
                        "Only the first is emitted; they would otherwise collide "
                        "on one metric URN.",
                        context=f"{prepared.node.dbt_name}.{measure.name}",
                    )
                    continue
                metrics[key] = self._metric_from_measure(
                    measure=measure,
                    alias=prepared.alias,
                    dataset_urn=str(prepared.dataset.urn),
                )
        return metrics

    def _metrics(
        self, models: List[_PreparedModel], metric_definitions: List[DBTMetric]
    ) -> List[Metric]:
        """Build metrics from `create_metric` measures and `metrics:` entries.

        Both share one project-flat URN namespace, because MetricFlow queries
        metrics by bare name with no qualifier. A name declared both ways is
        therefore one logical metric: the top-level definition wins, since it
        carries a label, description, type and derivation.
        """
        index = self._index_measures(models)
        metrics = self._metrics_from_measures(models)
        self.report.num_metrics_from_measures += len(metrics)
        # unique_id of the definition that claimed each name, for collision
        # reporting.
        from_manifest: Dict[str, str] = {}

        # Canonical id per folded name, so a derivedFrom edge is built from the
        # case the metric was actually emitted with rather than the case the
        # reference happened to use.
        canonical_id_by_name = {key: metric.urn.id for key, metric in metrics.items()}
        canonical_id_by_name.update(
            {
                definition.name.casefold(): definition.name
                for definition in metric_definitions
                if definition.name
            }
        )

        for metric_definition in sorted(metric_definitions, key=lambda m: m.unique_id):
            if not metric_definition.name:
                self.report.warning(
                    title="dbt metric has no name",
                    message="Skipping this metric; a name is required for its URN.",
                    context=metric_definition.unique_id,
                )
                continue
            key = metric_definition.name.casefold()
            if key in from_manifest:
                # Two top-level definitions, not a measure collision: the
                # earlier one would be silently replaced.
                self.report.warning(
                    title="Duplicate dbt metric name",
                    message="Two top-level metrics resolve to the same name, so "
                    "they would collide on one metric URN. Only the first is "
                    "emitted.",
                    context=f"{from_manifest[key]} and {metric_definition.unique_id}",
                )
                continue
            if key in metrics:
                self.report.warning(
                    title="dbt metric shadows a create_metric measure",
                    message="A measure with create_metric and a top-level metric "
                    "share a name. The top-level definition is emitted, since it "
                    "also carries a label, type and derivation.",
                    context=metric_definition.unique_id,
                )
                # It is a manifest metric now, not a measure one, so move the
                # count rather than dropping it.
                self.report.num_metrics_from_measures -= 1
            from_manifest[key] = metric_definition.unique_id
            self.report.num_metrics_from_manifest += 1
            metrics[key] = self._metric_from_definition(
                metric_definition=metric_definition,
                index=index,
                canonical_id_by_name=canonical_id_by_name,
            )

        return [metrics[key] for key in sorted(metrics)]

    def _metric_urn(self, metric_id: str) -> str:
        return str(MetricUrn(platform=DBT_PLATFORM, path=self.path, id=metric_id))

    @staticmethod
    def _measure_expression(measure: DBTSemanticMeasure, alias: str) -> Optional[str]:
        # No aggregation means we cannot say how the metric is computed, which
        # is more honest as an absent expression than as a fabricated one.
        agg = (measure.agg or "").strip().lower()
        return f"{agg}({alias}.{measure.name})" if agg else None

    def _metric_from_measure(
        self, *, measure: DBTSemanticMeasure, alias: str, dataset_urn: str
    ) -> Metric:
        expression = self._expression(self._measure_expression(measure, alias))
        return Metric(
            platform=DBT_PLATFORM,
            path=self.path,
            id=measure.name,
            semantic_model=self.model_urn,
            platform_instance=self.config.platform_instance,
            name=measure.name,
            description=measure.description or None,
            expression=expression,
            upstream_datasets=[dataset_urn],
            extra_aspects=[
                SubTypesClass(typeNames=[_metric_subtype(METRIC_TYPE_SIMPLE)])
            ],
        )

    def _metric_from_definition(
        self,
        *,
        metric_definition: DBTMetric,
        index: _MeasureIndex,
        canonical_id_by_name: Dict[str, str],
    ) -> Metric:
        resolved = self._resolve_metric_inputs(
            metric_definition, index, canonical_id_by_name
        )

        upstreams: List[str] = []
        measure_names = [
            measure_input.name for measure_input in metric_definition.measures
        ]
        measure_names.extend(resolved.measure_names)
        for measure_name in measure_names:
            dataset_urn = index.dataset_urn_by_measure.get(measure_name.casefold())
            if dataset_urn and dataset_urn not in upstreams:
                upstreams.append(dataset_urn)
        for dbt_name in metric_definition.depends_on:
            if not dbt_name.startswith(_SEMANTIC_MODEL_DEPENDS_ON_PREFIX):
                continue
            dataset_urn = index.dataset_urn_by_dbt_name.get(dbt_name)
            if dataset_urn and dataset_urn not in upstreams:
                upstreams.append(dataset_urn)
        if not upstreams:
            # A derived metric over other metrics legitimately has no direct
            # logical-dataset upstream, so emit it rather than skipping it.
            self.report.num_metrics_without_upstreams += 1

        return Metric(
            platform=DBT_PLATFORM,
            path=self.path,
            id=metric_definition.name,
            semantic_model=self.model_urn,
            platform_instance=self.config.platform_instance,
            name=metric_definition.display_name,
            description=metric_definition.description or None,
            expression=self._metric_definition_expression(metric_definition, index),
            upstream_datasets=upstreams,
            derived_from=resolved.metric_urns,
            tags=[make_tag_urn(tag) for tag in metric_definition.tags] or None,
            extra_aspects=[
                SubTypesClass(typeNames=[_metric_subtype(metric_definition.type)])
            ],
        )

    def _metric_definition_expression(
        self, metric_definition: DBTMetric, index: _MeasureIndex
    ) -> Optional[DialectExpressionInput]:
        expression = self._unfiltered_metric_expression(metric_definition, index)
        if expression is None:
            return None
        if not metric_definition.filter:
            return self._expression(expression)
        # MetricInfo.expression has no filter field, and a metric whose filter
        # is dropped computes a different number than the dbt definition -- so
        # fold the predicate into the expression rather than publishing a
        # broader metric as authoritative.
        return self._expression(
            f"{expression} FILTER (WHERE {metric_definition.filter})"
        )

    def _unfiltered_metric_expression(
        self, metric_definition: DBTMetric, index: _MeasureIndex
    ) -> Optional[str]:
        expr = (metric_definition.expr or "").strip()
        # dbt materializes a `create_metric: true` measure into `metrics` itself
        # and sets type_params.expr to the bare measure name. Honouring that
        # verbatim would publish an identifier where a computation belongs and
        # lose the aggregation, so fall through to the aggregation form. An
        # author who writes `expr: revenue` over measure `revenue` means the
        # same thing, so preferring the aggregation is right either way.
        if expr and not self._expr_is_bare_measure_name(metric_definition, expr):
            return expr
        if (
            metric_definition.type == METRIC_TYPE_RATIO
            and len(metric_definition.input_metrics) == 2
        ):
            numerator, denominator = metric_definition.input_metrics
            return f"{numerator.name} / {denominator.name}"
        # A simple metric is just its measure's aggregation, so reuse it rather
        # than leaving the metric with no expression at all.
        if len(metric_definition.measures) == 1:
            return index.expression_by_measure.get(
                metric_definition.measures[0].name.casefold()
            )
        return None

    @staticmethod
    def _expr_is_bare_measure_name(metric_definition: DBTMetric, expr: str) -> bool:
        return len(metric_definition.measures) == 1 and (
            expr.casefold() == metric_definition.measures[0].name.casefold()
        )

    def _resolve_metric_inputs(
        self,
        metric_definition: DBTMetric,
        index: _MeasureIndex,
        canonical_id_by_name: Dict[str, str],
    ) -> _ResolvedMetricInputs:
        """Split a metric's inputs into metric references and measure names.

        A ratio's numerator/denominator names metrics in modern dbt but named
        measures in dbt 1.6, and the manifest does not say which. Resolve
        against the known metric names first, then fall back to the measure
        index -- a measure-valued input becomes an upstream logical dataset
        rather than a derivedFrom edge.
        """
        if not metric_definition.references_metrics:
            return _ResolvedMetricInputs()
        names: List[str] = [
            metric_input.name for metric_input in metric_definition.input_metrics
        ]
        names.extend(
            dbt_name.split(".")[-1]
            for dbt_name in metric_definition.depends_on
            if dbt_name.startswith(_METRIC_DEPENDS_ON_PREFIX)
        )
        resolved = _ResolvedMetricInputs()
        for name in names:
            folded = name.casefold()
            if folded == metric_definition.name.casefold():
                continue
            canonical_id = canonical_id_by_name.get(folded)
            if canonical_id is not None:
                # Built from the id the referenced metric was emitted with, not
                # from the referencing string's case, so the edge cannot dangle.
                urn = self._metric_urn(canonical_id)
                if urn not in resolved.metric_urns:
                    resolved.metric_urns.append(urn)
            elif folded in index.dataset_urn_by_measure:
                if name not in resolved.measure_names:
                    resolved.measure_names.append(name)
            else:
                # derivedFrom is indexed as lineage, so a wrong edge is worse
                # than a missing one.
                self.report.warning(
                    title="dbt metric references an unknown metric",
                    message="Dropping this derivedFrom edge; the referenced name "
                    "matched neither a metric nor a measure in the manifest.",
                    context=f"{metric_definition.unique_id} -> {name}",
                )
        return resolved
