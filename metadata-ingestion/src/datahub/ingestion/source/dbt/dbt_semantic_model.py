import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    METRIC_TYPE_RATIO,
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
)
from datahub.metadata.urns import MetricUrn, SemanticModelUrn
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

_SEMANTIC_MODEL_DEPENDS_ON_PREFIX = "semantic_model."
_METRIC_DEPENDS_ON_PREFIX = "metric."


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
        # Sorted for deterministic output, and so alias/metric dedupe is
        # stable across runs rather than manifest-iteration-order dependent.
        nodes = sorted(
            (node for node in semantic_model_nodes if self._has_definition(node)),
            key=lambda node: node.dbt_name,
        )
        alias_by_dbt_name = self._build_aliases(nodes)

        datasets: List[SemanticModelDataset] = []
        dataset_by_alias: Dict[str, SemanticModelDataset] = {}
        fields_by_alias: Dict[str, Dict[str, SemanticFieldInput]] = {}
        definitions: List[Tuple[DBTNode, DBTSemanticModelDefinition]] = []

        for node in nodes:
            definition = node.semantic_model_def
            assert definition is not None  # filtered by _has_definition
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
            dataset = SemanticModelDataset(
                platform=DBT_PLATFORM,
                name=self._logical_dataset_name(alias),
                semantic_model=self.model_urn,
                alias=alias,
                schema=list(fields.values()),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                description=node.description or None,
                upstreams=self._upstreams(node, all_nodes_map) or None,
            )
            datasets.append(dataset)
            dataset_by_alias[alias] = dataset
            fields_by_alias[alias] = fields
            definitions.append((node, definition))

        if not datasets:
            if semantic_model_nodes or metric_definitions:
                self.report.warning(
                    title="No dbt semantic models could be emitted",
                    message="No semanticModel, Semantic Model Dataset or metric "
                    "entities were produced. Metrics are attached to a semantic "
                    "model, so none can be emitted either.",
                    context=self.project_name,
                )
            return

        relationships = self._relationships(
            definitions, alias_by_dbt_name, fields_by_alias
        )
        metrics = self._metrics(
            definitions=definitions,
            alias_by_dbt_name=alias_by_dbt_name,
            dataset_by_alias=dataset_by_alias,
            metric_definitions=metric_definitions,
        )

        model = SemanticModel(
            platform=DBT_PLATFORM,
            path=self.path,
            id=SEMANTIC_MODEL_ID,
            platform_instance=self.config.platform_instance,
            name=self.project_name,
            datasets=datasets,
            relationships=relationships or None,
        )

        # The SDK validates strictly at as_mcps() time and raises on a bad
        # alias or an unresolvable join column. Materialize everything up
        # front so a modelling problem degrades to a warning instead of
        # aborting the whole ingestion run.
        try:
            workunits = list(model.as_workunits())
            for dataset in datasets:
                workunits.extend(dataset.as_workunits())
            for metric in metrics:
                workunits.extend(metric.as_workunits())
        except Exception as e:
            self.report.record_node_failure(
                context=f"dbt project {self.project_name}",
                exc=e,
                title="Failed to emit dbt semantic model entities",
                message="No semanticModel, Semantic Model Dataset or metric "
                "entities were emitted for this project.",
                kind="emission",
            )
            return

        yield from workunits

        self.report.num_semantic_model_entities_emitted += 1
        self.report.num_semantic_model_datasets_emitted += len(datasets)
        self.report.num_semantic_model_relationships_emitted += len(relationships)
        self.report.num_metrics_emitted += len(metrics)

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
        """
        aliases: Dict[str, str] = {}
        taken: Set[str] = set()
        for node in nodes:
            candidates = [node.name]
            if node.dbt_package_name:
                candidates.append(f"{node.dbt_package_name}_{node.name}")
            alias = next((c for c in candidates if c and c not in taken), None)
            if alias is None:
                suffix = 2
                while f"{node.name}_{suffix}" in taken:
                    suffix += 1
                alias = f"{node.name}_{suffix}"
            if alias != node.name:
                self.report.warning(
                    title="Duplicate dbt semantic model name",
                    message="Two semantic models share a name, so this one was "
                    "given a disambiguated alias. Its logical dataset URN "
                    "differs from the semantic model's name.",
                    context=f"{node.dbt_name} -> {alias}",
                )
            taken.add(alias)
            aliases[node.dbt_name] = alias
        return aliases

    def _logical_dataset_name(self, alias: str) -> str:
        # platform_instance is applied by the Dataset URN builder, so it must
        # not be baked in here -- unlike self.path, which must contain it.
        name = f"{self.project_name}.{alias}"
        # Mirror DBTNode.get_urn's casing rule: AutoLowercaseUrnsProcessor only
        # runs when convert_urns_to_lowercase is set explicitly in the recipe,
        # so relying on it alone would give mixed casing against the physical
        # upstream URN.
        return name.lower() if self.config.convert_urns_to_lowercase else name

    def _upstreams(
        self, node: DBTNode, all_nodes_map: Dict[str, DBTNode]
    ) -> List[str]:
        # Reuses the shared helper so semantic-model lineage honours
        # skip_sources_in_lineage, ephemeral nodes and target_platform_instance
        # exactly as every other dbt edge does.
        return get_upstreams(
            upstreams=node.upstream_nodes,
            all_nodes=all_nodes_map,
            target_platform=self.config.target_platform,
            target_platform_instance=self.config.target_platform_instance,
            environment=self.config.env,
            platform_instance=self.config.platform_instance,
            skip_sources_in_lineage=self.config.skip_sources_in_lineage,
        )

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
                fields, node, dimension.name, "dimension", self._dimension_field(dimension)
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
                message=f"Skipping an unnamed {kind}; it cannot be emitted as a "
                "schema field.",
                context=node.dbt_name,
            )
            return
        if name in fields:
            self.report.warning(
                title="Duplicate dbt semantic model field name",
                message=f"Skipping this {kind}; a field with the same name was "
                "already emitted (entities take precedence over dimensions, "
                "which take precedence over measures).",
                context=f"{node.dbt_name}.{name}",
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
        self,
        definitions: List[Tuple[DBTNode, DBTSemanticModelDefinition]],
        alias_by_dbt_name: Dict[str, str],
        fields_by_alias: Dict[str, Dict[str, SemanticFieldInput]],
    ) -> List[SemanticModelRelationshipInput]:
        """Derive relationships from MetricFlow's own join semantics.

        MetricFlow joins semantic model A to B when A declares an entity named
        X as foreign (or unique/natural) and B declares X as a key. Entity
        names need not be unique across semantic models -- that name match *is*
        the join. The referencing model is the many side, so cardinality is
        N_ONE; a `unique` target is not one-to-one, since dbt documents joining
        a single unique key to multiple foreign keys.
        """
        owners_by_entity: Dict[str, List[str]] = defaultdict(list)
        for node, definition in definitions:
            alias = alias_by_dbt_name[node.dbt_name]
            for name in sorted(self._key_entity_names(definition)):
                owners_by_entity[name.lower()].append(alias)

        relationships: List[SemanticModelRelationshipInput] = []
        seen: Set[Tuple[str, str, str]] = set()
        for node, definition in definitions:
            from_alias = alias_by_dbt_name[node.dbt_name]
            for entity in sorted(definition.entities, key=lambda e: e.name):
                if not entity.is_join_source:
                    continue
                owners = [
                    owner
                    for owner in owners_by_entity.get(entity.name.lower(), [])
                    # A self-join is meaningless as a relationship, and
                    # from_ == to would read as a loop in the UI.
                    if owner != from_alias
                ]
                if not owners:
                    # Legitimate when the target model is outside the ingested
                    # scope, so this is a counter rather than a warning.
                    self.report.semantic_model_relationships_unresolved.append(
                        f"{node.dbt_name}.{entity.name}"
                    )
                    continue
                if len(owners) > 1:
                    self.report.warning(
                        title="Ambiguous dbt semantic model join",
                        message="More than one semantic model declares this "
                        "entity as a key, so the join target is ambiguous. "
                        "Skipping this relationship rather than guessing.",
                        context=f"{node.dbt_name}.{entity.name} -> {sorted(owners)}",
                    )
                    continue
                to_alias = owners[0]
                # The join column is always the entity name, never its `expr`:
                # `name` is the field_path in the schema, and the SDK raises on
                # a join column it cannot find there.
                if entity.name not in fields_by_alias.get(
                    from_alias, {}
                ) or entity.name not in fields_by_alias.get(to_alias, {}):
                    self.report.warning(
                        title="dbt semantic model join column not in schema",
                        message="Skipping this relationship; the join column is "
                        "absent from one side's schema, most likely dropped as a "
                        "duplicate field name.",
                        context=f"{node.dbt_name}.{entity.name}",
                    )
                    continue
                key = (from_alias, to_alias, entity.name.lower())
                if key in seen:
                    continue
                seen.add(key)
                relationships.append(
                    SemanticModelRelationshipInput(
                        from_alias=from_alias,
                        from_columns=[entity.name],
                        to_alias=to_alias,
                        to_columns=[entity.name],
                        name=f"{from_alias}_to_{to_alias}_on_{entity.name}",
                        cardinality=ERModelRelationshipCardinalityClass.N_ONE,
                    )
                )
        return relationships

    def _metrics(
        self,
        *,
        definitions: List[Tuple[DBTNode, DBTSemanticModelDefinition]],
        alias_by_dbt_name: Dict[str, str],
        dataset_by_alias: Dict[str, SemanticModelDataset],
        metric_definitions: List[DBTMetric],
    ) -> List[Metric]:
        """Build metrics from `create_metric` measures and `metrics:` entries.

        Both share one project-flat URN namespace, because MetricFlow queries
        metrics by bare name with no qualifier. A name declared both ways is
        therefore one logical metric: the top-level definition wins, since it
        carries a label, description, type and derivation.
        """
        dataset_urn_by_measure: Dict[str, str] = {}
        dataset_urn_by_dbt_name: Dict[str, str] = {}
        expression_by_measure: Dict[str, str] = {}
        for node, definition in definitions:
            alias = alias_by_dbt_name[node.dbt_name]
            dataset = dataset_by_alias.get(alias)
            if dataset is None:
                continue
            dataset_urn_by_dbt_name[node.dbt_name] = str(dataset.urn)
            for measure in definition.measures:
                dataset_urn_by_measure.setdefault(
                    measure.name.lower(), str(dataset.urn)
                )
                expression = self._measure_expression(measure, alias)
                if expression:
                    expression_by_measure.setdefault(measure.name.lower(), expression)

        metrics: Dict[str, Metric] = {}
        for node, definition in definitions:
            alias = alias_by_dbt_name[node.dbt_name]
            dataset = dataset_by_alias.get(alias)
            if dataset is None:
                continue
            for measure in definition.measures:
                if not measure.create_metric:
                    continue
                metric_id = measure.name
                if metric_id.lower() in metrics:
                    self.report.warning(
                        title="Duplicate dbt metric name",
                        message="Two measures with create_metric share a name. "
                        "Only the first is emitted; they would otherwise collide "
                        "on one metric URN.",
                        context=f"{node.dbt_name}.{measure.name}",
                    )
                    continue
                metrics[metric_id.lower()] = self._metric_from_measure(
                    measure=measure, alias=alias, dataset_urn=str(dataset.urn)
                )
                self.report.num_metrics_from_measures += 1

        known_metric_names = {
            definition.name.lower() for definition in metric_definitions
        } | set(metrics)
        for metric_definition in sorted(metric_definitions, key=lambda m: m.unique_id):
            if not metric_definition.name:
                self.report.warning(
                    title="dbt metric has no name",
                    message="Skipping this metric; a name is required for its URN.",
                    context=metric_definition.unique_id,
                )
                continue
            if metric_definition.name.lower() in metrics:
                self.report.warning(
                    title="dbt metric shadows a create_metric measure",
                    message="A measure with create_metric and a top-level metric "
                    "share a name. The top-level definition is emitted, since it "
                    "also carries a label, type and derivation.",
                    context=metric_definition.unique_id,
                )
            metrics[metric_definition.name.lower()] = self._metric_from_definition(
                metric_definition=metric_definition,
                dataset_urn_by_measure=dataset_urn_by_measure,
                dataset_urn_by_dbt_name=dataset_urn_by_dbt_name,
                expression_by_measure=expression_by_measure,
                known_metric_names=known_metric_names,
            )
            self.report.num_metrics_from_manifest += 1

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
        )

    def _metric_from_definition(
        self,
        *,
        metric_definition: DBTMetric,
        dataset_urn_by_measure: Dict[str, str],
        dataset_urn_by_dbt_name: Dict[str, str],
        expression_by_measure: Dict[str, str],
        known_metric_names: Set[str],
    ) -> Metric:
        derived_from, measure_referenced_metrics = self._resolve_metric_inputs(
            metric_definition, known_metric_names, dataset_urn_by_measure
        )

        upstreams: List[str] = []
        measure_names = [
            measure_input.name for measure_input in metric_definition.measures
        ]
        measure_names.extend(measure_referenced_metrics)
        for measure_name in measure_names:
            dataset_urn = dataset_urn_by_measure.get(measure_name.lower())
            if dataset_urn and dataset_urn not in upstreams:
                upstreams.append(dataset_urn)
        for dbt_name in metric_definition.depends_on:
            if not dbt_name.startswith(_SEMANTIC_MODEL_DEPENDS_ON_PREFIX):
                continue
            dataset_urn = dataset_urn_by_dbt_name.get(dbt_name)
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
            expression=self._metric_definition_expression(
                metric_definition, expression_by_measure
            ),
            upstream_datasets=upstreams,
            derived_from=derived_from,
        )

    def _metric_definition_expression(
        self,
        metric_definition: DBTMetric,
        expression_by_measure: Dict[str, str],
    ) -> Optional[DialectExpressionInput]:
        if metric_definition.expr and metric_definition.expr.strip():
            return self._expression(metric_definition.expr)
        if (
            metric_definition.type == METRIC_TYPE_RATIO
            and len(metric_definition.input_metrics) == 2
        ):
            numerator, denominator = metric_definition.input_metrics
            return self._expression(f"{numerator.name} / {denominator.name}")
        # A simple metric is just its measure's aggregation, so reuse it rather
        # than leaving the metric with no expression at all.
        if len(metric_definition.measures) == 1:
            return self._expression(
                expression_by_measure.get(metric_definition.measures[0].name.lower())
            )
        return None

    def _resolve_metric_inputs(
        self,
        metric_definition: DBTMetric,
        known_metric_names: Set[str],
        dataset_urn_by_measure: Dict[str, str],
    ) -> Tuple[List[str], List[str]]:
        """Split a metric's inputs into metric references and measure names.

        A ratio's numerator/denominator names metrics in modern dbt but named
        measures in dbt 1.6, and the manifest does not say which. Resolve
        against the known metric names first, then fall back to the measure
        index -- a measure-valued input becomes an upstream logical dataset
        rather than a derivedFrom edge.
        """
        if not metric_definition.references_metrics:
            return [], []
        names: List[str] = [
            metric_input.name for metric_input in metric_definition.input_metrics
        ]
        names.extend(
            dbt_name.split(".")[-1]
            for dbt_name in metric_definition.depends_on
            if dbt_name.startswith(_METRIC_DEPENDS_ON_PREFIX)
        )
        derived_from: List[str] = []
        measure_names: List[str] = []
        for name in names:
            if name.lower() == metric_definition.name.lower():
                continue
            if name.lower() in known_metric_names:
                urn = self._metric_urn(name)
                if urn not in derived_from:
                    derived_from.append(urn)
            elif name.lower() in dataset_urn_by_measure:
                if name not in measure_names:
                    measure_names.append(name)
            else:
                # derivedFrom is indexed as lineage, so a wrong edge is worse
                # than a missing one.
                self.report.warning(
                    title="dbt metric references an unknown metric",
                    message="Dropping this derivedFrom edge; the referenced name "
                    "matched neither a metric nor a measure in the manifest.",
                    context=f"{metric_definition.unique_id} -> {name}",
                )
        return derived_from, measure_names
