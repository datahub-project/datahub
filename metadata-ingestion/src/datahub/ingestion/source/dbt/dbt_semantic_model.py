import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_dataplatform_instance_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    METRIC_TYPE_RATIO,
    METRIC_TYPE_SIMPLE,
    DBTCommonConfig,
    DBTMetric,
    DBTMetricInput,
    DBTNode,
    DBTSemanticDimension,
    DBTSemanticEntity,
    DBTSemanticMeasure,
    DBTSemanticModelDefinition,
    DBTSourceReport,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
    SemanticModelPropertiesClass,
    SubTypesClass,
)
from datahub.metadata.urns import MetricUrn, SchemaFieldUrn, SemanticModelUrn
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

# MDX / TABLEAU / MAQL are BI-tool dialects and can never be a dbt adapter.
_TARGET_PLATFORM_TO_DIALECT: Dict[str, str] = {
    "snowflake": DialectClass.SNOWFLAKE,
    "databricks": DialectClass.DATABRICKS,
    "spark": DialectClass.DATABRICKS,
}

_SEMANTIC_MODEL_DEPENDS_ON_PREFIX = "semantic_model."
_METRIC_DEPENDS_ON_PREFIX = "metric."

# The only dataset-anchored aspect this mapper contributes. Everything else the
# SDK's SemanticModelDataset builds - datasetProperties, schemaMetadata,
# subTypes, status, dataPlatformInstance - is already written for this urn by
# the connector's ordinary dbt dataset path, which carries the dbt provenance
# (dbt_unique_id, dbt_file_path, language), the real descriptions, tags, owners
# and upstream lineage, and runs them through write_semantics so a PATCH run
# merges with server-side edits. Re-emitting them from here would be a plain
# UPSERT of a much thinner aspect and would silently undo all of that, so the
# dataset MCPs are filtered down to this one. Field-anchored MCPs
# (semanticFieldAnnotation, aiContext) are kept by urn prefix instead: nothing
# else writes them.
_ADDITIVE_DATASET_ASPECTS = frozenset({SemanticModelPropertiesClass.ASPECT_NAME})

_SCHEMA_FIELD_URN_PREFIX = f"urn:li:{SchemaFieldUrn.ENTITY_TYPE}:"


def _accumulation_note(metric_definition: DBTMetric) -> Optional[str]:
    """How a cumulative metric accumulates, for the expression comment.

    Without it a cumulative metric's expression is the aggregation it is built
    on - identical to the simple metric over the same measure, so a 7-day
    running total and a plain total read the same.
    """
    if metric_definition.window:
        return f"cumulative over {metric_definition.window}"
    if metric_definition.grain_to_date:
        return f"cumulative to date by {metric_definition.grain_to_date}"
    return None


def _metric_subtype(metric_type: str) -> str:
    """Title-case dbt's metric type for display, e.g. "cumulative" -> Cumulative.

    metricInfo has no field for the metric type, and dropping it would make a
    ratio, a derived metric and a plain sum indistinguishable in search. dbt
    documents the set as conversion, cumulative, derived, ratio and simple; an
    unrecognized value is passed through title-cased rather than dropped, so a
    future dbt type still classifies something.
    """
    return metric_type.replace("_", " ").title()


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


@dataclass(frozen=True)
class _MetricComputation:
    """How a metric is computed, plus any predicate that constrains it.

    The predicate is held apart from the expression so a filter on the measure
    input and the metric's own filter can land in one `FILTER (WHERE ...)`
    clause; emitting a clause per filter would not be valid SQL.
    """

    expression: str
    measure_predicate: Optional[str] = None
    # Appended as a trailing SQL comment, after any FILTER clause.
    annotation: Optional[str] = None
    # False for a ratio or an author-written expr: SQL's FILTER clause attaches
    # to an aggregate call, so hanging one off `a / b` reads as constraining `b`
    # alone when dbt applies the filter to the whole metric.
    takes_filter_clause: bool = True


@dataclass(frozen=True)
class _MeasureLocation:
    """Where a measure lives, and how it aggregates."""

    dataset_urn: str
    expression: Optional[str]


@dataclass
class _MeasureIndex:
    """Measure lookups a top-level metric needs, keyed by casefolded name.

    Two levels, because a measure name is unique only within its semantic
    model: `by_model` resolves a metric that names its owning model in
    `depends_on` (dbt always does for a metric built on a measure), and
    `by_name` is the project-wide fallback for anything that does not.
    """

    by_name: Dict[str, _MeasureLocation] = field(default_factory=dict)
    by_model: Dict[str, Dict[str, _MeasureLocation]] = field(default_factory=dict)
    dataset_urn_by_dbt_name: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class _PreparedModel:
    """One dbt semantic model, resolved into everything emission needs."""

    node: DBTNode
    definition: DBTSemanticModelDefinition
    alias: str
    fields: Dict[str, SemanticFieldInput]
    dataset: SemanticModelDataset


class DbtSemanticModelMapper:
    """Maps dbt semantic models and metrics to first-class DataHub entities.

    One `semanticModel` per dbt project holding the join relationships, one
    `metric` per `create_metric` measure and per top-level `metrics:` entry,
    and - layered onto the dataset the connector already emits for each dbt
    semantic model - the aspects that make it a semantic-model dataset.

    The dataset urn is untouched, and so is every aspect the ordinary dbt path
    writes for it: see `_ADDITIVE_DATASET_ASPECTS`.
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
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the semantic-model layer.

        Deliberately takes no view of the rest of the manifest: the dataset
        this layer annotates, and its lineage to the dbt model it sits on, are
        already emitted by the ordinary dbt path.
        """
        models = self._prepare(semantic_model_nodes)
        if not models:
            if semantic_model_nodes or metric_definitions:
                self.report.warning(
                    title="No dbt semantic models could be emitted",
                    message="No semanticModel or metric entities were produced, "
                    "and no dataset was annotated. Metrics hang off a semantic "
                    "model, so none can be emitted either. The datasets "
                    "themselves are unaffected.",
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
            extra_aspects=self._common_aspects(),
        )

        # The SDK validates strictly at as_mcps() time and raises SdkUsageError
        # on a bad alias or an unresolvable join column. Materialize up front so
        # a modelling problem degrades rather than aborting the run - but only
        # SdkUsageError: any other exception is a bug in this mapper and must
        # surface as a crash rather than be laundered into a warning.
        try:
            workunits = list(model.as_workunits())
        except SdkUsageError as e:
            # The semanticModel carries the relationships and owns every
            # dataset's membership, so losing it makes the whole layer useless.
            self.report.failure(
                title="Failed to emit dbt semantic model entities",
                message="No semanticModel or metric entities were emitted for "
                "this project, and no dataset was annotated. The datasets "
                "themselves are unaffected. Fix the reported modelling "
                "problem, or set emit_semantic_model_entities to false.",
                context=f"dbt project {self.project_name}",
                exc=e,
            )
            # Everything built for this project is now dropped. Counted so the
            # report still reconciles: from_measures + from_manifest ==
            # emitted + dropped.
            self.report.num_semantic_model_datasets_dropped += len(models)
            self.report.num_metrics_dropped += len(metrics)
            return

        annotated_datasets = 0
        for prepared in models:
            # Per-entity, so one unrepresentable dataset does not cost the
            # whole project.
            dataset_workunits = self._dataset_annotation_workunits(prepared)
            if dataset_workunits is None:
                self.report.num_semantic_model_datasets_dropped += 1
                continue
            workunits.extend(dataset_workunits)
            annotated_datasets += 1

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
        self.report.num_semantic_model_datasets_annotated += annotated_datasets
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

    def _dataset_annotation_workunits(
        self, prepared: _PreparedModel
    ) -> Optional[List[MetadataWorkUnit]]:
        """The semantic-model aspects to layer onto an already-emitted dataset.

        See `_ADDITIVE_DATASET_ASPECTS` for why the SDK's dataset-anchored
        aspects are filtered rather than emitted wholesale.
        """
        try:
            mcps = prepared.dataset.as_mcps()
        except SdkUsageError as e:
            self.report.warning(
                title="Failed to annotate a dbt semantic model dataset",
                message="Skipping this dataset's semantic-model aspects; the "
                "dataset itself is emitted as usual, and the rest of the "
                "project's semantic model is still emitted.",
                context=f"semantic model {prepared.node.dbt_name}",
                exc=e,
            )
            return None
        return [
            MetadataWorkUnit(id=MetadataWorkUnit.generate_workunit_id(mcp), mcp=mcp)
            for mcp in mcps
            if self._is_additive(mcp)
        ]

    @staticmethod
    def _is_additive(mcp: MetadataChangeProposalWrapper) -> bool:
        entity_urn = mcp.entityUrn or ""
        if entity_urn.startswith(_SCHEMA_FIELD_URN_PREFIX):
            return True
        return mcp.aspectName in _ADDITIVE_DATASET_ASPECTS

    def _common_aspects(self) -> List[BrowsePathsV2Class]:
        """Browse path for the entities this mapper emits.

        Needed because neither `semanticModel` nor `metric` has a `container`
        aspect in the entity registry - containers model the *physical*
        organization of an asset (database, schema), and these are logical. So
        the usual route, AutoBrowsePathV2Processor deriving a path from the
        container hierarchy, is closed to them, and without an explicit aspect
        GMS falls back to BrowsePathV2Utils, which puts anything that is not a
        dataset/chart/dashboard/dataJob into a literal "Default" folder.

        Flat under the project, unlike Snowflake's db/schema/view nesting: dbt
        emits exactly one semanticModel per project, so a `<project>/<model>`
        level would partition identically to `<project>` and only add a step.

        TODO: dbt emits no containers at all, so every dbt entity - not just
        these - relies on the GMS fallback for its browse path (which happens
        to work for datasets, whose dotted name it can split). Giving the
        connector a real container hierarchy is a broader change than this
        feature and is deliberately left out of it.
        """
        instance = self.config.platform_instance
        entries: List[BrowsePathEntryClass] = []
        if instance:
            entries.append(
                BrowsePathEntryClass(
                    id=instance,
                    urn=make_dataplatform_instance_urn(DBT_PLATFORM, instance),
                )
            )
        # Same fold as _build_path: the documented multi-project dbt setup sets
        # platform_instance to the project name, and a second identical level
        # would partition nothing. Keep the instance entry, which carries the
        # dataPlatformInstance urn the project-only entry has no equivalent of.
        if instance != self.project_name:
            entries.append(BrowsePathEntryClass(id=self.project_name))
        return [BrowsePathsV2Class(path=entries)]

    def _prepare(self, semantic_model_nodes: List[DBTNode]) -> List[_PreparedModel]:
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
                    message="Skipping this semantic model's semantic-model "
                    "aspects; it declares no entities, dimensions or measures "
                    "that could be annotated. The dataset itself is emitted as "
                    "usual.",
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
                    # Deliberately minimal. `name` reproduces the urn the dbt
                    # path already emitted for this node, and nothing else the
                    # Dataset builder can set is passed, because every other
                    # aspect belongs to that path. See
                    # `_ADDITIVE_DATASET_ASPECTS`.
                    dataset=SemanticModelDataset(
                        platform=DBT_PLATFORM,
                        name=self._dataset_name(node),
                        semantic_model=self.model_urn,
                        alias=alias,
                        schema=list(fields.values()),
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    ),
                )
            )
        return models

    def _dataset_name(self, node: DBTNode) -> str:
        """The dataset name the connector already emitted for this node.

        Reuses DBTNode.get_db_fqn and its casing rule rather than restating
        them, so the annotated urn cannot drift from the emitted one.
        """
        db_fqn = node.get_db_fqn()
        return db_fqn.lower() if node.convert_urns_to_lowercase else db_fqn

    def _has_definition(self, node: DBTNode) -> bool:
        definition = node.semantic_model_def
        if definition is None or definition.has_no_fields():
            self.report.warning(
                title="dbt semantic model has no definition",
                message="Skipping this semantic model's semantic-model "
                "aspects; its entities, dimensions and measures are all absent "
                "or empty. The dataset itself is emitted as usual.",
                context=node.dbt_name,
            )
            self.report.semantic_models_skipped.append(node.dbt_name)
            return False
        return True

    def _build_aliases(self, nodes: List[DBTNode]) -> Dict[str, str]:
        """Assign a unique alias per semantic model.

        The alias is the join identity in relationships and the qualifier in
        every synthesized expression, and the SDK raises when two attached
        datasets share one. Two semantic models can share a `name` across dbt
        packages, so disambiguate with the package name and then a numeric
        suffix.

        Uniqueness is checked in the same case space the dataset urn uses:
        with convert_urns_to_lowercase, "Orders" and "orders" are distinct
        aliases but would collide on one urn.
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
                    "this one was given a disambiguated alias. Relationships "
                    "and expressions refer to it by that alias, not its name.",
                    context=f"{node.dbt_name} -> {alias}",
                )
            taken.add(key(alias))
            aliases[node.dbt_name] = alias
        return aliases

    def _semantic_fields(
        self, node: DBTNode, definition: DBTSemanticModelDefinition
    ) -> Dict[str, SemanticFieldInput]:
        """Build the annotated field set, keyed by field path.

        dbt only enforces name uniqueness within each of entities/dimensions/
        measures, so a name can repeat across the three kinds. The SDK raises
        on a duplicate field_path, so dedupe here with entities > dimensions >
        measures precedence - the same precedence
        `convert_semantic_model_fields_to_columns` gives the dataset's schema,
        so the annotations line up with the columns that are already there.

        `type` on each spec only reaches the schemaMetadata the SDK builds,
        which is filtered out (`_ADDITIVE_DATASET_ASPECTS`) because the dbt
        path already emitted that aspect. It is still set to the same
        `entity:`/`dimension:`/`measure:` string that path uses, so the two
        representations cannot disagree if the schema is ever emitted.
        """
        fields: Dict[str, SemanticFieldInput] = {}

        for entity in definition.entities:
            self._add_field(
                fields,
                node,
                entity.name,
                "entity",
                self._entity_field(entity),
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
                "cannot be annotated.",
                context=f"{node.dbt_name} ({kind})",
            )
            return
        if name in fields:
            self.report.warning(
                title="Duplicate dbt semantic model field name",
                message="Skipping this field; one with the same name was already "
                "annotated (entities take precedence over dimensions, which take "
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
        # synthesizes "alias.field_path" when none is given - which is what
        # Snowflake's mapper does too.
        if not expr or not expr.strip():
            return None
        return DialectExpressionInput(expression=expr, dialect=self.dialect)

    def _entity_field(self, entity: DBTSemanticEntity) -> SemanticFieldInput:
        # A MetricFlow entity is a groupable, joinable column, which is what a
        # dimension is; FILTER and OTHER fit nothing dbt produces.
        return SemanticFieldInput(
            field_path=entity.name,
            type=f"entity:{entity.type}",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=entity.description or None,
            expression=self._expression(entity.expr),
        )

    def _dimension_field(self, dimension: DBTSemanticDimension) -> SemanticFieldInput:
        return SemanticFieldInput(
            field_path=dimension.name,
            type=f"dimension:{dimension.type}",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=dimension.description or None,
            # The granularity itself has nowhere to go: semanticFieldAnnotation
            # carries only a boolean. TODO: emit it when the aspect grows a
            # field for it.
            is_time_dimension=dimension.is_time,
            expression=self._expression(dimension.expr),
        )

    def _measure_field(self, measure: DBTSemanticMeasure) -> SemanticFieldInput:
        return SemanticFieldInput(
            field_path=measure.name,
            type=f"measure:{measure.agg}",
            semantic_type=SemanticFieldTypeClass.MEASURE,
            description=measure.description or None,
            aggregation_function=measure.aggregation,
            expression=self._expression(measure.expr),
        )

    def _relationships(
        self, models: List[_PreparedModel]
    ) -> List[SemanticModelRelationshipInput]:
        """Derive relationships from MetricFlow's own join semantics.

        MetricFlow joins semantic model A to B when A declares an entity named
        X as foreign (or unique/natural) and B declares X as a key. Entity
        names need not be unique across semantic models - that name match *is*
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
                        "entity was dropped from the annotated fields, most "
                        "likely as a duplicate field name.",
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
                    # scope (a package this run filtered out, or a key declared
                    # in another project), so this is a counter rather than a
                    # warning.
                    self.report.semantic_model_relationships_unresolved.append(
                        f"{prepared.node.dbt_name}.{entity.name}"
                    )
                    continue
                # One relationship per owner. Two models declaring the same
                # entity as a key is valid dbt - MetricFlow joins the
                # referencing model to each of them - so both edges are real,
                # and emitting neither would lose joins rather than avoid a
                # guess.
                for target in sorted(owners, key=lambda owner: owner.alias):
                    key = (prepared.alias, target.alias, entity.name.casefold())
                    if key in seen:
                        continue
                    seen.add(key)
                    relationships.append(
                        SemanticModelRelationshipInput(
                            from_alias=prepared.alias,
                            # The join column is always the entity's name, never
                            # its `expr`: `name` is the field_path in the schema,
                            # and the SDK raises on a join column it cannot find
                            # there.
                            from_columns=[from_field],
                            to_alias=target.alias,
                            to_columns=[target.field_path],
                            name=(
                                f"{prepared.alias}_to_{target.alias}_on_{entity.name}"
                            ),
                            cardinality=ERModelRelationshipCardinalityClass.N_ONE,
                        )
                    )
        return relationships

    @staticmethod
    def _field_path(prepared: _PreparedModel, name: str) -> Optional[str]:
        """The field path for an entity name, matched case-insensitively.

        Resolving owners case-insensitively but checking membership
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
                location = _MeasureLocation(
                    dataset_urn=dataset_urn,
                    expression=self._measure_expression(measure, prepared.alias),
                )
                index.by_model.setdefault(prepared.node.dbt_name, {})[key] = location
                if key in index.by_name:
                    self.report.warning(
                        title="Ambiguous dbt measure name",
                        message="More than one semantic model declares a measure "
                        "with this name. A metric naming its semantic model in "
                        "`depends_on` still resolves to the right one; anything "
                        "referencing the measure by name alone is attributed to "
                        "the first model in name order.",
                        context=f"{prepared.node.dbt_name}.{measure.name}",
                    )
                    continue
                index.by_name[key] = location
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
                        "on one metric urn.",
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

        Both share one project-flat urn namespace, because MetricFlow queries
        metrics by bare name with no qualifier. A name declared both ways is
        therefore one logical metric: the top-level definition wins, since it
        carries a label, description, type and derivation.
        """
        index = self._index_measures(models)
        metrics = self._metrics_from_measures(models)
        self.report.num_metrics_from_measures += len(metrics)
        accepted = self._accepted_metric_definitions(metric_definitions, set(metrics))

        # Canonical id per folded name, so a derivedFrom edge is built from the
        # case the metric was actually emitted with rather than the case the
        # reference happened to use. Built from the accepted definitions only:
        # two names differing just by case collide on one urn, and taking the
        # skipped one's case would point every edge at a urn never emitted.
        canonical_id_by_name = {key: metric.urn.id for key, metric in metrics.items()}
        canonical_id_by_name.update(
            {definition.name.casefold(): definition.name for definition in accepted}
        )

        for metric_definition in accepted:
            metrics[metric_definition.name.casefold()] = self._metric_from_definition(
                metric_definition=metric_definition,
                index=index,
                canonical_id_by_name=canonical_id_by_name,
            )

        return [metrics[key] for key in sorted(metrics)]

    def _accepted_metric_definitions(
        self, metric_definitions: List[DBTMetric], measure_metric_keys: Set[str]
    ) -> List[DBTMetric]:
        """The top-level definitions that will be emitted, in build order.

        Resolved before any metric is built, because a metric's `derivedFrom`
        edges are built from the id its target was emitted with - so which of
        two colliding definitions wins has to be settled first.
        """
        accepted: List[DBTMetric] = []
        # unique_id of the definition that claimed each name, for collision
        # reporting.
        claimed: Dict[str, str] = {}
        for metric_definition in sorted(metric_definitions, key=lambda m: m.unique_id):
            if not metric_definition.name:
                self.report.warning(
                    title="dbt metric has no name",
                    message="Skipping this metric; a name is required for its urn.",
                    context=metric_definition.unique_id,
                )
                continue
            key = metric_definition.name.casefold()
            if key in claimed:
                # Two top-level definitions, not a measure collision: the
                # earlier one would be silently replaced.
                self.report.warning(
                    title="Duplicate dbt metric name",
                    message="Two top-level metrics resolve to the same name, so "
                    "they would collide on one metric urn. Only the first is "
                    "emitted.",
                    context=f"{claimed[key]} and {metric_definition.unique_id}",
                )
                continue
            if key in measure_metric_keys:
                if not self._is_dbt_generated_measure_metric(metric_definition):
                    self.report.warning(
                        title="dbt metric shadows a create_metric measure",
                        message="A measure with create_metric and a top-level "
                        "metric share a name. The top-level definition is "
                        "emitted, since it also carries a label, type and "
                        "derivation.",
                        context=metric_definition.unique_id,
                    )
                # It is a manifest metric now, not a measure one, so move the
                # count rather than dropping it.
                self.report.num_metrics_from_measures -= 1
            claimed[key] = metric_definition.unique_id
            self.report.num_metrics_from_manifest += 1
            accepted.append(metric_definition)
        return accepted

    @staticmethod
    def _locate_measure(
        metric_definition: DBTMetric, index: _MeasureIndex, measure_name: str
    ) -> Optional[_MeasureLocation]:
        """Find a measure, preferring the semantic model the metric declares.

        A measure name is unique per semantic model but not per project, so the
        project-wide map can point at the wrong model - which would both add a
        bogus upstream edge and qualify the synthesized expression with the
        wrong alias.
        """
        folded = measure_name.casefold()
        for dbt_name in metric_definition.depends_on:
            if not dbt_name.startswith(_SEMANTIC_MODEL_DEPENDS_ON_PREFIX):
                continue
            located = index.by_model.get(dbt_name, {}).get(folded)
            if located:
                return located
        return index.by_name.get(folded)

    def _metric_urn(self, metric_id: str) -> str:
        return str(MetricUrn(platform=DBT_PLATFORM, path=self.path, id=metric_id))

    @staticmethod
    def _measure_expression(measure: DBTSemanticMeasure, alias: str) -> Optional[str]:
        # No aggregation means we cannot say how the metric is computed, which
        # is more honest as an absent expression than as a fabricated one.
        agg = measure.aggregation
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
                SubTypesClass(typeNames=[_metric_subtype(METRIC_TYPE_SIMPLE)]),
                *self._common_aspects(),
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
            located = self._locate_measure(metric_definition, index, measure_name)
            if located and located.dataset_urn not in upstreams:
                upstreams.append(located.dataset_urn)
        for dbt_name in metric_definition.depends_on:
            if not dbt_name.startswith(_SEMANTIC_MODEL_DEPENDS_ON_PREFIX):
                continue
            dataset_urn = index.dataset_urn_by_dbt_name.get(dbt_name)
            if dataset_urn and dataset_urn not in upstreams:
                upstreams.append(dataset_urn)
        if not upstreams:
            # A derived metric over other metrics legitimately has no direct
            # dataset upstream, so emit it rather than skipping it.
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
                SubTypesClass(typeNames=[_metric_subtype(metric_definition.type)]),
                *self._common_aspects(),
            ],
        )

    def _metric_definition_expression(
        self, metric_definition: DBTMetric, index: _MeasureIndex
    ) -> Optional[DialectExpressionInput]:
        computation = self._metric_computation(metric_definition, index)
        if computation is None:
            return None
        # metricInfo.expression has no filter field, and a metric whose filter
        # is dropped computes a different number than the dbt definition - so
        # fold the predicates into the expression rather than publishing a
        # broader metric as authoritative. dbt keeps these as Jinja templates
        # (`{{ Dimension(...) }}`), so the result is not always parseable SQL;
        # the leading aggregation still names the column it reads, which is
        # more than a dropped expression would carry.
        #
        # TODO: resolve the Jinja to real field references, which would make
        # both filter kinds fully parseable and usable for column-level
        # lineage: `{{ Dimension('payment__payment_amount') }}` is
        # `<alias>.payment_amount`, and this mapper already holds the alias and
        # the model's field paths. Deliberately not done here - a dimension
        # reached through a join belongs to another model's alias, and
        # `TimeDimension('metric_time', 'month')` implies a grain to render, so
        # it needs its own design rather than a regex.
        predicates = [
            predicate
            for predicate in (computation.measure_predicate, metric_definition.filter)
            if predicate
        ]
        if predicates and not computation.takes_filter_clause:
            # Stated as a comment rather than a FILTER clause it cannot carry,
            # so the predicate is still visible and the expression still reads
            # as the computation dbt declared.
            return self._expression(
                self._annotated(
                    computation.expression,
                    "; ".join(
                        [f"filtered: {' AND '.join(predicates)}"]
                        + ([computation.annotation] if computation.annotation else [])
                    ),
                )
            )
        if not predicates:
            return self._expression(
                self._annotated(computation.expression, computation.annotation)
            )
        if len(predicates) > 1:
            # AND binds tighter than OR, so joining `a OR b` to `c OR d` bare
            # yields `a OR (b AND c) OR d` - a different set of rows than dbt
            # applying both predicates. A lone predicate needs no grouping,
            # since nothing is joined to it.
            predicates = [f"({predicate})" for predicate in predicates]
        return self._expression(
            self._annotated(
                f"{computation.expression} FILTER (WHERE {' AND '.join(predicates)})",
                computation.annotation,
            )
        )

    @staticmethod
    def _annotated(expression: str, annotation: Optional[str]) -> str:
        """Append an annotation as a SQL comment.

        A comment rather than invented syntax: it is legal wherever whitespace
        is, so the expression stays parseable and the leading aggregation still
        yields its column reference.

        TODO: this is tactical. The proper home for a cumulative metric's
        window is a field on metricInfo; until the aspect has one, anything we
        do here is a convention this connector owns. Drop the comment in favour
        of the real field when it exists.
        """
        return f"{expression} /* {annotation} */" if annotation else expression

    def _metric_computation(
        self, metric_definition: DBTMetric, index: _MeasureIndex
    ) -> Optional[_MetricComputation]:
        expr = (metric_definition.expr or "").strip()
        # dbt materializes a `create_metric: true` measure into `metrics` itself
        # and sets type_params.expr to the bare measure name. Honouring that
        # verbatim would publish an identifier where a computation belongs and
        # lose the aggregation, so fall through to the aggregation form. An
        # author who writes `expr: revenue` over measure `revenue` means the
        # same thing, so preferring the aggregation is right either way.
        if expr and not self._expr_is_bare_measure_name(metric_definition, expr):
            # An author's expression is arbitrary SQL, not necessarily an
            # aggregate call, so a FILTER clause cannot be hung off it either.
            return _MetricComputation(expr, takes_filter_clause=False)
        numerator = metric_definition.numerator
        denominator = metric_definition.denominator
        if (
            metric_definition.type == METRIC_TYPE_RATIO
            and numerator is not None
            and denominator is not None
        ):
            # A filter on either input is not carried. These are metric names,
            # not aggregates, and SQL FILTER attaches only to an aggregate
            # call, so there is nowhere in `a / b` to put a predicate that
            # constrains just `a`. Folding it at the top level would be worse
            # than omitting it: `(a / b) FILTER (WHERE p)` constrains both.
            return _MetricComputation(
                f"{self._ratio_side(metric_definition, index, numerator)} / "
                f"{self._ratio_side(metric_definition, index, denominator)}",
                takes_filter_clause=False,
            )
        # A simple metric is just its measure's aggregation, so reuse it rather
        # than leaving the metric with no expression at all.
        if len(metric_definition.measures) == 1:
            measure_input = metric_definition.measures[0]
            located = self._locate_measure(metric_definition, index, measure_input.name)
            if located is None or located.expression is None:
                return None
            # Only the synthesized aggregation carries the input's filter: an
            # author-written `expr` is theirs, and folding a predicate into it
            # would be putting words in their mouth.
            return _MetricComputation(
                located.expression,
                measure_predicate=measure_input.filter,
                annotation=_accumulation_note(metric_definition),
            )
        return None

    @staticmethod
    def _is_dbt_generated_measure_metric(metric_definition: DBTMetric) -> bool:
        """True when this is dbt's own copy of a `create_metric: true` measure.

        dbt materializes such a measure into `metrics:` itself, as a simple
        metric over the same-named measure. So the name overlap is dbt's doing
        rather than an author's mistake, and warning about it would fire on
        every `create_metric` measure in every project.
        """
        return (
            metric_definition.type == METRIC_TYPE_SIMPLE
            and len(metric_definition.measures) == 1
            and metric_definition.measures[0].name.casefold()
            == metric_definition.name.casefold()
        )

    def _ratio_side(
        self, metric_definition: DBTMetric, index: _MeasureIndex, side: DBTMetricInput
    ) -> str:
        """One side of a ratio, as its aggregation where that is knowable.

        Preferred over the bare name because the two sides of a rate metric
        often name the *same* measure and differ only by filter - so names
        alone would render as `x / x`. A side that resolves to no measure is a
        reference to another metric, which has no aggregation to render and
        nothing for a FILTER clause to attach to.
        """
        located = self._locate_measure(metric_definition, index, side.name)
        if located is None or located.expression is None:
            return side.name
        if side.filter:
            return f"{located.expression} FILTER (WHERE {side.filter})"
        return located.expression

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
        index - a measure-valued input becomes an upstream dataset rather than
        a derivedFrom edge.
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
            elif self._locate_measure(metric_definition, index, name) is not None:
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
