from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import MICROSTRATEGY_PLATFORM
from datahub.ingestion.source.microstrategy.lineage import (
    AttributeRelationship,
    MicroStrategyLineageExtractor,
    WarehouseLineageContext,
    coerce_dicts,
    consolidation_attribute_ids,
    metric_attribute_ids_from_model,
    metric_consolidation_ids_from_model,
    metric_fact_ids_from_model,
    metric_metric_ids_from_model,
    normalize_object_id,
    object_id,
    parse_attribute_relationships,
    physical_table_name,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.metadata.schema_classes import (
    ContainerClass,
    DialectClass,
    ERModelRelationshipCardinalityClass,
    SemanticFieldTypeClass,
)
from datahub.metadata.urns import MetricUrn, SemanticModelUrn
from datahub.sdk.metric import DialectExpressionInput, Metric
from datahub.sdk.semantic_model import (
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

# MicroStrategy's attribute-relationship API states cardinality parent-to-child
# (e.g. "one_to_many" == one parent value maps to many child values). The
# SemanticModelRelationshipInput this module emits runs the other direction
# (from=child table, to=parent table, matching a fact/child table joining up
# to a dimension/parent table), so the cardinality is inverted at the call site.
_PARENT_TO_CHILD_CARDINALITY: Dict[str, str] = {
    "one_to_one": ERModelRelationshipCardinalityClass.ONE_ONE,
    "one_to_many": ERModelRelationshipCardinalityClass.ONE_N,
    "many_to_one": ERModelRelationshipCardinalityClass.N_ONE,
    "many_to_many": ERModelRelationshipCardinalityClass.N_N,
}
_INVERT_CARDINALITY: Dict[str, str] = {
    ERModelRelationshipCardinalityClass.ONE_ONE: ERModelRelationshipCardinalityClass.ONE_ONE,
    ERModelRelationshipCardinalityClass.ONE_N: ERModelRelationshipCardinalityClass.N_ONE,
    ERModelRelationshipCardinalityClass.N_ONE: ERModelRelationshipCardinalityClass.ONE_N,
    ERModelRelationshipCardinalityClass.N_N: ERModelRelationshipCardinalityClass.N_N,
}

_SEMANTIC_MODEL_ID = "schema"


@dataclass
class _TableInfo:
    alias: str
    logical_table_id: Optional[str]
    warehouse_urn: Optional[str]
    fields: Dict[str, SemanticFieldInput]  # normalized object id -> field


def _optional_str(value: object) -> Optional[str]:
    if isinstance(value, str) and value:
        return value
    return None


def _dedupe_field_path(name: str, seen: Set[str]) -> str:
    cleaned = name.strip() or "unknown"
    if cleaned not in seen:
        seen.add(cleaned)
        return cleaned
    suffix = 2
    while f"{cleaned}_{suffix}" in seen:
        suffix += 1
    deduped = f"{cleaned}_{suffix}"
    seen.add(deduped)
    return deduped


class MicroStrategySemanticModelMapper:
    """Emits one SemanticModel per MicroStrategy project: Attributes and Facts
    are project-wide schema objects reusable across many reports/cubes, so the
    project (not any one report) is the natural unit of a semantic model --
    unlike this connector's other, per-report DatasetObject/mapper.py path."""

    def __init__(
        self,
        *,
        config: MicroStrategyConfig,
        report: MicroStrategyReport,
        client: MicroStrategyClient,
        lineage: MicroStrategyLineageExtractor,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client
        self.lineage = lineage

    def emit(
        self,
        *,
        project_id: str,
        project_name: str,
        model_tables: List[Dict[str, object]],
        warehouse_context: Optional[WarehouseLineageContext],
        project_container_key: ContainerKey,
    ) -> Iterable[MetadataWorkUnit]:
        model_urn = str(
            SemanticModelUrn(
                platform=MICROSTRATEGY_PLATFORM,
                path=project_id,
                id=_SEMANTIC_MODEL_ID,
            )
        )

        tables, fact_table_aliases, attribute_table_aliases = self._build_table_index(
            model_tables, warehouse_context
        )
        if not tables:
            self.report.warning(
                title="MicroStrategy Semantic Model Has No Usable Tables",
                message=(
                    "Skipping semantic-model emission for this project; no "
                    "logical table exposed a usable attribute or fact."
                ),
                context=f"project_id={project_id}",
            )
            return

        datasets, alias_to_dataset = self._logical_datasets(
            project_id, model_urn, tables, project_container_key
        )
        relationships = self._relationships(project_id, tables, attribute_table_aliases)
        metrics = list(
            self._metrics(
                project_id=project_id,
                model_urn=model_urn,
                fact_table_aliases=fact_table_aliases,
                attribute_table_aliases=attribute_table_aliases,
                alias_to_dataset=alias_to_dataset,
            )
        )

        model = SemanticModel(
            platform=MICROSTRATEGY_PLATFORM,
            path=project_id,
            id=_SEMANTIC_MODEL_ID,
            platform_instance=self.config.platform_instance,
            name=project_name,
            datasets=datasets,
            relationships=relationships or None,
            extra_aspects=self._container_aspects(project_container_key),
        )

        yield from model.as_workunits()
        for dataset in datasets:
            yield from dataset.as_workunits()
        for metric in metrics:
            yield from metric.as_workunits()

        self.report.report_semantic_model_emitted()
        self.report.report_semantic_model_datasets_emitted(len(datasets))
        self.report.report_semantic_model_metrics_emitted(len(metrics))
        self.report.report_semantic_model_relationships_emitted(len(relationships))

    def _container_aspects(
        self, project_container_key: ContainerKey
    ) -> List[ContainerClass]:
        return [ContainerClass(container=project_container_key.as_urn())]

    # ------------------------------------------------------------------
    # Tables -> logical datasets
    # ------------------------------------------------------------------

    def _build_table_index(
        self,
        model_tables: List[Dict[str, object]],
        warehouse_context: Optional[WarehouseLineageContext],
    ) -> Tuple[List[_TableInfo], Dict[str, Set[str]], Dict[str, Set[str]]]:
        tables: List[_TableInfo] = []
        fact_table_aliases: Dict[str, Set[str]] = {}
        attribute_table_aliases: Dict[str, Set[str]] = {}

        for table in model_tables:
            physical_table = table.get("physicalTable")
            if not isinstance(physical_table, dict):
                continue
            physical_alias = self._table_alias(physical_table, warehouse_context)
            if not physical_alias:
                continue
            # The logical table (this `table` entry's own "information" block,
            # distinct from physicalTable's) is what the attribute-hierarchy
            # API's relationshipTable refers to, and it is what distinguishes
            # two logical tables role-playing the same physical table (e.g.
            # "Order Date"/"Ship Date" both mapping to one shared date-lookup
            # table) -- the physical name alone can't tell them apart.
            logical_information = table.get("information")
            logical_table_id = (
                object_id(logical_information)
                if isinstance(logical_information, dict)
                else None
            )
            alias = self._dataset_alias(logical_information, physical_alias)
            warehouse_urn = (
                self.lineage.warehouse_dataset_urn(warehouse_context, physical_alias)
                if warehouse_context is not None
                else None
            )

            fields: Dict[str, SemanticFieldInput] = {}
            seen_field_paths: Set[str] = set()
            for fact in coerce_dicts(table.get("facts")):
                fact_id = object_id(fact.get("information"))
                field = self._fact_field(fact)
                if fact_id is None or field is None:
                    continue
                normalized = normalize_object_id(fact_id)
                field.field_path = _dedupe_field_path(
                    field.field_path, seen_field_paths
                )
                fields[normalized] = field
                fact_table_aliases.setdefault(normalized, set()).add(alias)
            for attribute in coerce_dicts(table.get("attributes")):
                attribute_id = object_id(attribute.get("information"))
                field = self._attribute_field(attribute)
                if attribute_id is None or field is None:
                    continue
                normalized = normalize_object_id(attribute_id)
                field.field_path = _dedupe_field_path(
                    field.field_path, seen_field_paths
                )
                fields[normalized] = field
                attribute_table_aliases.setdefault(normalized, set()).add(alias)

            if not fields:
                continue
            tables.append(
                _TableInfo(
                    alias=alias,
                    logical_table_id=(
                        normalize_object_id(logical_table_id)
                        if logical_table_id
                        else None
                    ),
                    warehouse_urn=warehouse_urn,
                    fields=fields,
                )
            )

        return tables, fact_table_aliases, attribute_table_aliases

    @staticmethod
    def _table_alias(
        physical_table: Dict[str, object],
        warehouse_context: Optional[WarehouseLineageContext],
    ) -> Optional[str]:
        if warehouse_context is not None:
            qualified = physical_table_name(physical_table, warehouse_context)
            if qualified:
                return qualified
        # No warehouse context (or it couldn't resolve a name) -- fall back to
        # MicroStrategy's own logical table identity so tables still get a
        # stable, reasonably-unique alias.
        raw = physical_table.get("tableName")
        if not isinstance(raw, str) or not raw:
            information = physical_table.get("information")
            if isinstance(information, dict):
                raw = information.get("name")
        if not isinstance(raw, str) or not raw:
            return None
        namespace = physical_table.get("namespace")
        return f"{namespace}.{raw}" if isinstance(namespace, str) and namespace else raw

    @staticmethod
    def _dataset_alias(
        logical_information: object,
        physical_alias: str,
    ) -> str:
        # Prefer the logical table's own name as this dataset's identity: in
        # the common (non-role-playing) case it matches the physical table
        # name anyway, and when a physical table is role-played by more than
        # one logical table, the logical names are what differ (e.g. "Order
        # Date" vs "Ship Date" both backed by one date-lookup table) -- using
        # the physical name here would collapse those into one dataset.
        if isinstance(logical_information, dict):
            name = logical_information.get("name")
            if isinstance(name, str) and name:
                return name
        return physical_alias

    @staticmethod
    def _fact_field(fact: Dict[str, object]) -> Optional[SemanticFieldInput]:
        information = fact.get("information")
        if not isinstance(information, dict):
            return None
        name = information.get("name")
        if not isinstance(name, str) or not name:
            return None
        # A fact's own data type isn't part of /api/model/tables' response;
        # it is a raw, pre-aggregation numeric value by definition (the
        # aggregated business measure is the Metric layered on top).
        return SemanticFieldInput(
            field_path=name,
            type="number",
            semantic_type=SemanticFieldTypeClass.MEASURE,
            description=_optional_str(information.get("description")),
        )

    @classmethod
    def _attribute_field(
        cls, attribute: Dict[str, object]
    ) -> Optional[SemanticFieldInput]:
        information = attribute.get("information")
        if not isinstance(information, dict):
            return None
        name = information.get("name")
        if not isinstance(name, str) or not name:
            return None
        return SemanticFieldInput(
            field_path=name,
            type="string",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=_optional_str(information.get("description")),
            is_time_dimension=cls._is_temporal_attribute(attribute, name),
        )

    @staticmethod
    def _is_temporal_attribute(attribute: Dict[str, object], name: str) -> bool:
        # Best-effort: no single field reliably marks an attribute as a date/time
        # dimension across MicroStrategy versions, so this checks form category
        # first (more reliable when present) and falls back to a name-token
        # heuristic, matching this connector's existing _is_temporal in mapper.py.
        for form in coerce_dicts(attribute.get("forms")):
            category = str(
                form.get("baseFormCategory") or form.get("category") or ""
            ).lower()
            if any(token in category for token in ("date", "time")):
                return True
        lowered = name.lower()
        return any(
            token in lowered
            for token in ("date", "time", "year", "quarter", "month", "day", "week")
        )

    def _logical_dataset_name(self, project_id: str, alias: str) -> str:
        return f"{project_id}.{alias}"

    def _logical_datasets(
        self,
        project_id: str,
        model_urn: str,
        tables: List[_TableInfo],
        project_container_key: ContainerKey,
    ) -> Tuple[List[SemanticModelDataset], Dict[str, SemanticModelDataset]]:
        datasets: List[SemanticModelDataset] = []
        alias_to_dataset: Dict[str, SemanticModelDataset] = {}
        for table_info in tables:
            dataset = SemanticModelDataset(
                platform=MICROSTRATEGY_PLATFORM,
                name=self._logical_dataset_name(project_id, table_info.alias),
                semantic_model=model_urn,
                alias=table_info.alias,
                schema=list(table_info.fields.values()),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                extra_aspects=self._container_aspects(project_container_key),
            )
            if table_info.warehouse_urn:
                dataset.set_upstreams([table_info.warehouse_urn])
            datasets.append(dataset)
            alias_to_dataset[table_info.alias] = dataset
        return datasets, alias_to_dataset

    # ------------------------------------------------------------------
    # Attribute hierarchy -> relationships
    # ------------------------------------------------------------------

    def _relationships(
        self,
        project_id: str,
        tables: List[_TableInfo],
        attribute_table_aliases: Dict[str, Set[str]],
    ) -> List[SemanticModelRelationshipInput]:
        table_alias_by_logical_id = {
            info.logical_table_id: info.alias
            for info in tables
            if info.logical_table_id
        }
        table_info_by_alias = {info.alias: info for info in tables}

        # Fetching hierarchy relationships costs one API call per attribute --
        # scope it to attributes actually shared across more than one table
        # (the only case that can produce a cross-table relationship at all).
        shared_attribute_ids = sorted(
            attribute_id
            for attribute_id, aliases in attribute_table_aliases.items()
            if len(aliases) >= 2
        )

        relationships: List[SemanticModelRelationshipInput] = []
        seen_pairs: Set[Tuple[str, str]] = set()
        for attribute_id in shared_attribute_ids:
            try:
                response = self.client.get_attribute_relationships(
                    project_id, attribute_id
                )
            except MicroStrategyAPIError as error:
                self.report.report_semantic_model_attribute_relationship_api_failure()
                self.report.warning(
                    title="MicroStrategy Attribute Relationship Unavailable",
                    message=(
                        "Skipping this attribute's hierarchy relationships for "
                        "the semantic model."
                    ),
                    context=f"project_id={project_id}, attribute_id={attribute_id}",
                    exc=error,
                )
                continue

            for relationship in parse_attribute_relationships(response):
                pair = (
                    relationship.parent_attribute_id,
                    relationship.child_attribute_id,
                )
                if pair in seen_pairs:
                    continue
                built = self._relationship_input(
                    project_id,
                    relationship,
                    table_alias_by_logical_id,
                    attribute_table_aliases,
                    table_info_by_alias,
                )
                if built is None:
                    continue
                seen_pairs.add(pair)
                relationships.append(built)
        return relationships

    def _relationship_input(
        self,
        project_id: str,
        relationship: AttributeRelationship,
        table_alias_by_logical_id: Dict[str, str],
        attribute_table_aliases: Dict[str, Set[str]],
        table_info_by_alias: Dict[str, _TableInfo],
    ) -> Optional[SemanticModelRelationshipInput]:
        parent_aliases = attribute_table_aliases.get(
            relationship.parent_attribute_id, set()
        )
        child_aliases = attribute_table_aliases.get(
            relationship.child_attribute_id, set()
        )
        parent_alias = (
            table_alias_by_logical_id.get(relationship.relationship_table_id)
            if relationship.relationship_table_id
            else None
        )
        if parent_alias is None or parent_alias not in parent_aliases:
            if len(parent_aliases) != 1:
                return None
            parent_alias = next(iter(parent_aliases))

        # A relationship is only meaningful across two DIFFERENT logical
        # datasets; a hierarchy encoded entirely within one lookup table's
        # columns (the common case) has nothing to join.
        candidate_child_aliases = sorted(child_aliases - {parent_alias})
        if not candidate_child_aliases:
            return None
        if len(candidate_child_aliases) > 1:
            self.report.warning(
                title="Ambiguous MicroStrategy Attribute Relationship",
                message=(
                    "The child attribute appears on multiple other tables; "
                    "using the first (sorted) as a best-effort choice."
                ),
                context=(
                    f"project_id={project_id}, "
                    f"child_attribute_id={relationship.child_attribute_id}, "
                    f"candidates={candidate_child_aliases}"
                ),
            )
        child_alias = candidate_child_aliases[0]

        parent_field = table_info_by_alias[parent_alias].fields.get(
            relationship.parent_attribute_id
        )
        child_field = table_info_by_alias[child_alias].fields.get(
            relationship.child_attribute_id
        )
        if parent_field is None or child_field is None:
            return None

        cardinality = None
        if relationship.relationship_type:
            parent_to_child = _PARENT_TO_CHILD_CARDINALITY.get(
                relationship.relationship_type.lower()
            )
            if parent_to_child is None:
                self.report.warning(
                    title="Unrecognized MicroStrategy Relationship Type",
                    message="This relationship will be emitted without a cardinality.",
                    context=(
                        f"project_id={project_id}, "
                        f"relationship_type={relationship.relationship_type}"
                    ),
                )
            else:
                cardinality = _INVERT_CARDINALITY[parent_to_child]

        return SemanticModelRelationshipInput(
            from_alias=child_alias,
            from_columns=[child_field.field_path],
            to_alias=parent_alias,
            to_columns=[parent_field.field_path],
            name=f"{child_alias}_to_{parent_alias}",
            cardinality=cardinality,
        )

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _metrics(
        self,
        *,
        project_id: str,
        model_urn: str,
        fact_table_aliases: Dict[str, Set[str]],
        attribute_table_aliases: Dict[str, Set[str]],
        alias_to_dataset: Dict[str, SemanticModelDataset],
    ) -> Iterable[Metric]:
        consolidation_cache: Dict[str, List[str]] = {}
        try:
            metric_objects = list(self.client.search_metrics(project_id))
        except MicroStrategyAPIError as error:
            # Metric discovery is a separate API call from the tables fetch
            # that already succeeded by the time this runs; losing it must
            # not take the structural semantic-model entities (the model and
            # its logical datasets, already built by the caller) down with
            # it, so this degrades to "no metrics" rather than propagating.
            self.report.report_semantic_model_metric_search_api_failure()
            self.report.warning(
                title="MicroStrategy Metric Search Unavailable",
                message=(
                    "Skipping metric emission for the semantic model; project "
                    "metrics could not be enumerated."
                ),
                context=f"project_id={project_id}",
                exc=error,
            )
            return
        for metric_object in metric_objects:
            metric_id = normalize_object_id(metric_object.id)
            try:
                model = self.client.get_metric_model(project_id, metric_object.id)
            except MicroStrategyAPIError as error:
                self.report.report_metric_expression_api_failure()
                self.report.report_failed_metric_model(metric_object.id)
                self.report.warning(
                    title="MicroStrategy Metric Model Unavailable",
                    message=(
                        "Skipping this metric for the semantic model; its "
                        "expression could not be fetched."
                    ),
                    context=f"project_id={project_id}, metric_id={metric_object.id}",
                    exc=error,
                )
                continue

            upstream_aliases: Set[str] = set()
            for fact_id in metric_fact_ids_from_model(model):
                upstream_aliases |= fact_table_aliases.get(fact_id, set())
            for attribute_id in metric_attribute_ids_from_model(model):
                upstream_aliases |= attribute_table_aliases.get(attribute_id, set())
            for consolidation_id in metric_consolidation_ids_from_model(model):
                for attribute_id in self._resolve_consolidation(
                    project_id, consolidation_id, consolidation_cache
                ):
                    upstream_aliases |= attribute_table_aliases.get(attribute_id, set())

            upstream_dataset_urns = sorted(
                str(alias_to_dataset[alias].urn)
                for alias in upstream_aliases
                if alias in alias_to_dataset
            )
            derived_from = [
                str(
                    MetricUrn(
                        platform=MICROSTRATEGY_PLATFORM,
                        path=project_id,
                        id=referenced_metric_id,
                    )
                )
                for referenced_metric_id in metric_metric_ids_from_model(model)
            ]

            model_expression = model.get("expression")
            expression_text = _optional_str(
                model_expression.get("text")
                if isinstance(model_expression, dict)
                else None
            )
            expression = (
                DialectExpressionInput(
                    expression=expression_text, dialect=DialectClass.OTHER
                )
                if expression_text
                else None
            )

            yield Metric(
                platform=MICROSTRATEGY_PLATFORM,
                path=project_id,
                id=metric_id,
                semantic_model=model_urn,
                platform_instance=self.config.platform_instance,
                name=metric_object.name,
                description=metric_object.description,
                expression=expression,
                upstream_datasets=upstream_dataset_urns,
                derived_from=derived_from,
            )

    def _resolve_consolidation(
        self,
        project_id: str,
        consolidation_id: str,
        cache: Dict[str, List[str]],
    ) -> List[str]:
        if consolidation_id in cache:
            return cache[consolidation_id]
        try:
            model = self.client.get_consolidation_model(project_id, consolidation_id)
        except MicroStrategyAPIError as error:
            self.report.report_semantic_model_consolidation_api_failure()
            self.report.warning(
                title="MicroStrategy Consolidation Unavailable",
                message=(
                    "Skipping this consolidation's attribute lineage for the "
                    "semantic model."
                ),
                context=f"project_id={project_id}, consolidation_id={consolidation_id}",
                exc=error,
            )
            cache[consolidation_id] = []
            return []
        attribute_ids = consolidation_attribute_ids(model)
        cache[consolidation_id] = attribute_ids
        return attribute_ids
