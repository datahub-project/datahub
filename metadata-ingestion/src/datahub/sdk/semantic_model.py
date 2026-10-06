from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Set, Type, Union, cast

from typing_extensions import Self, TypeAlias

from datahub.emitter.mce_builder import DEFAULT_ENV, parse_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    AiContextClass,
    ChangeTypeClass,
    DimensionClass,
    GlobalTagsClass,
    MetricExpressionClass,
    SchemaFieldClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SemanticModelRelationshipClass,
    StatusClass,
)
from datahub.metadata.urns import (
    DatasetUrn,
    SchemaFieldUrn,
    SemanticModelUrn,
    Urn,
)
from datahub.sdk._semantic_shared import (
    AiContextInput,
    DialectExpressionInput,
    MetricExpressionInputType,
    as_input_list,
    build_ai_context,
    build_metric_expression,
    make_audit_stamp,
    validate_semantic_model_urn,
)
from datahub.sdk._shared import (
    DomainInputType,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasStructuredProperties,
    HasTags,
    HasTerms,
    LinksInputType,
    OwnersInputType,
    StructuredPropertyInputType,
    TagsInputType,
    TermsInputType,
)
from datahub.sdk.dataset import Dataset, UpstreamLineageInputType
from datahub.sdk.entity import Entity, ExtraAspectsType

logger = logging.getLogger(__name__)

__all__ = [
    "AiContextInput",
    "DialectExpressionInput",
    "MetricExpressionInputType",
    "SemanticFieldInput",
    "SemanticModel",
    "SemanticModelDataset",
    "SemanticModelDatasetInputType",
    "SemanticModelRelationshipInput",
]


@dataclass
class SemanticModelRelationshipInput:
    """A join path between two logical datasets in a semantic model.

    ``from_alias``/``to_alias`` must match the ``alias`` of the corresponding
    logical datasets' ``semanticModelProperties``.
    """

    from_alias: str
    from_columns: List[str]
    to_alias: str
    to_columns: List[str]
    name: Optional[str] = None
    cardinality: Optional[str] = None
    ai_context: Optional[AiContextInput] = None


@dataclass
class SemanticFieldInput:
    """A schema field plus its semantic annotation and optional AI context.

    ``expression`` is required on the emitted ``semanticFieldAnnotation``; when
    omitted it is auto-synthesized as ``f"{alias}.{field_path}"`` so the field
    references its own logical dataset by alias.
    """

    field_path: str
    type: str
    semantic_type: Union[str, SemanticFieldTypeClass]
    description: Optional[str] = None
    nullable: bool = True
    is_part_of_key: bool = False
    tags: Optional[TagsInputType] = None
    expression: Optional[MetricExpressionInputType] = None
    aggregation_function: Optional[str] = None
    is_time_dimension: bool = False
    ai_context: Optional[AiContextInput] = None


SemanticModelDatasetInputType: TypeAlias = "SemanticModelDataset"


@dataclass
class _FieldAnnotation:
    """Internal pairing of a field's annotation aspect with its AI context."""

    annotation: SemanticFieldAnnotationClass
    ai_context: Optional[AiContextClass] = None


class SemanticModel(
    HasPlatformInstance,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    HasStructuredProperties,
    Entity,
):
    """A semantic model: a container of logical datasets, relationships, and metrics.

    The semantic model groups logical datasets (with dimensional context) and
    the business metrics calculated over them. Membership is member-side only:
    each logical dataset carries ``semanticModelProperties.semanticModel``
    (``IsPartOf``), and each metric carries ``metricInfo.semanticModel``
    (``ModeledBy``). There are no ``datasets`` / ``metrics`` arrays on
    ``semanticModelInfo``.

    The canonical lineage chain is::

        Metric -> Logical Dataset -> Physical Dataset

    expressed by ``metricUpstreams.datasetUpstreams`` (Metric → SMD) and each
    logical dataset's own ``upstreamLineage`` (SMD → Physical).

    Server compatibility: requires a server build that includes the
    semanticModel/metric model (operator's responsibility — no automatic
    check). See :func:`datahub.sdk.require_metrics_support` for an opt-in
    preflight helper.
    """

    __slots__ = ("_attached_logical_datasets",)

    @classmethod
    def get_urn_type(cls) -> Type[SemanticModelUrn]:
        return SemanticModelUrn

    def __init__(
        self,
        *,
        platform: str,
        path: str,
        id: str,
        platform_instance: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        native_definition: Optional[str] = None,
        datasets: Optional[Sequence[SemanticModelDatasetInputType]] = None,
        relationships: Optional[Sequence[SemanticModelRelationshipInput]] = None,
        ai_context: Optional[AiContextInput] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = SemanticModelUrn(platform=platform, path=path, id=id)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)
        self._set_platform_instance(urn.platform, platform_instance)
        # Status is part of the producer contract for this entity.
        self._set_aspect(StatusClass(removed=False))
        self._attached_logical_datasets: List["SemanticModelDataset"] = []
        self._ensure_model_props()

        if name is not None:
            self.set_name(name)
        if description is not None:
            self.set_description(description)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)
        if native_definition is not None:
            self.set_native_definition(native_definition)
        if relationships is not None:
            self.set_relationships(relationships)
        if ai_context is not None:
            self.set_ai_context(ai_context)
        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if tags is not None:
            self.set_tags(tags)
        if terms is not None:
            self.set_terms(terms)
        if domain is not None:
            self.set_domain(domain)
        if structured_properties is not None:
            for key, value in structured_properties.items():
                self.set_structured_property(property_urn=key, values=value)
        # datasets last so add_dataset can reconcile back-refs against the
        # already-populated alias on each SemanticModelDataset.
        if datasets is not None:
            self.set_datasets(datasets)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: object) -> Self:  # type: ignore[override]
        assert isinstance(urn, SemanticModelUrn)
        entity = cls(
            platform=urn.platform,
            path=urn.path,
            id=urn.id,
        )
        return entity._init_from_graph(current_aspects)  # type: ignore[arg-type]

    @property
    def urn(self) -> SemanticModelUrn:
        return self._urn  # type: ignore

    def _ensure_model_props(self) -> SemanticModelInfoClass:
        props = self._get_aspect(SemanticModelInfoClass)
        if props is None:
            # name is required on the aspect; default to the URN id so the
            # entity is always constructible without an explicit name.
            props = SemanticModelInfoClass(name=self.urn.id)
            self._set_aspect(props)
        return props

    @property
    def name(self) -> str:
        return self._ensure_model_props().name

    def set_name(self, name: str) -> None:
        self._ensure_model_props().name = name

    @property
    def description(self) -> Optional[str]:
        return self._ensure_model_props().description

    def set_description(self, description: str) -> None:
        self._ensure_model_props().description = description

    @property
    def created(self) -> Optional[datetime]:
        stamp = self._ensure_model_props().created
        if stamp is None or stamp.time == 0:
            return None
        return parse_ts_millis(stamp.time)

    def set_created(self, created: datetime) -> None:
        self._ensure_model_props().created = make_audit_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        stamp = self._ensure_model_props().lastModified
        if stamp is None or stamp.time == 0:
            return None
        return parse_ts_millis(stamp.time)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_model_props().lastModified = make_audit_stamp(last_modified)

    @property
    def native_definition(self) -> Optional[str]:
        return self._ensure_model_props().nativeDefinition

    def set_native_definition(self, native_definition: str) -> None:
        self._ensure_model_props().nativeDefinition = native_definition

    @property
    def datasets(self) -> List[str]:
        """URNs of logical datasets attached via :meth:`add_dataset` / ``datasets=``.

        Membership itself is stored on each dataset's
        ``semanticModelProperties.semanticModel``, not on this model.
        """
        # Order-preserving dedupe — duplicates may be attached but collapse for
        # the public membership list (and for relationship coverage checks).
        return list(
            dict.fromkeys(str(ds.urn) for ds in self._attached_logical_datasets)
        )

    def add_dataset(self, dataset: SemanticModelDatasetInputType) -> None:
        """Attach a logical dataset to this model.

        Reconciles ``semanticModelProperties.semanticModel`` to this model's URN
        (so the caller does not have to set it twice) and leaves the dataset's
        alias as the source of truth for relationship join paths.

        Insertion order is preserved across re-emits.
        """
        # The annotation already excludes anything else, but untyped callers
        # (e.g. a bare URN string) must still fail loudly rather than silently
        # attaching an object with no alias or schema.
        if not isinstance(cast(object, dataset), SemanticModelDataset):
            raise SdkUsageError(
                "SemanticModel.add_dataset requires a SemanticModelDataset; "
                "bare dataset URNs are not supported. Membership is authored on "
                "the logical dataset via semanticModelProperties."
            )
        dataset._set_semantic_model_back_ref(self.urn)
        self._attached_logical_datasets.append(dataset)

    def set_datasets(self, datasets: Sequence[SemanticModelDatasetInputType]) -> None:
        self._attached_logical_datasets = []
        for dataset in as_input_list(datasets):
            self.add_dataset(dataset)

    @property
    def relationships(self) -> Optional[List[SemanticModelRelationshipClass]]:
        return self._ensure_model_props().relationships

    def set_relationships(
        self, relationships: Sequence[SemanticModelRelationshipInput]
    ) -> None:
        self._ensure_model_props().relationships = [
            self._build_relationship(rel) for rel in relationships
        ]
        self._validate_relationships()

    def add_relationship(self, relationship: SemanticModelRelationshipInput) -> None:
        props = self._ensure_model_props()
        if props.relationships is None:
            props.relationships = []
        props.relationships.append(self._build_relationship(relationship))
        self._validate_relationships()

    @staticmethod
    def _build_relationship(
        rel: SemanticModelRelationshipInput,
    ) -> SemanticModelRelationshipClass:
        return SemanticModelRelationshipClass(
            name=rel.name,
            from_=rel.from_alias,
            fromColumns=list(rel.from_columns),
            to=rel.to_alias,
            toColumns=list(rel.to_columns),
            cardinality=rel.cardinality,
            aiContext=build_ai_context(rel.ai_context),
        )

    def _attached_datasets_by_urn(self) -> Dict[str, "SemanticModelDataset"]:
        # Keyed by URN so duplicate attachments of the same dataset collapse.
        return {str(ds.urn): ds for ds in self._attached_logical_datasets}

    def _attached_dataset_aliases(self) -> Set[str]:
        return {ds.alias for ds in self._attached_datasets_by_urn().values()}

    def _validate_relationships(self, *, strict: bool = False) -> None:
        """Warn — or, when ``strict``, raise — when a relationship references an
        alias with no matching attached dataset, or a join column absent from
        that dataset's schema.

        Datasets are commonly attached *after* relationships, so at construction
        time this can only flag what it can see. Re-run with ``strict=True`` at
        emit time (:meth:`as_mcps`).

        Alias checks raise (under ``strict``) whenever datasets are attached as
        :class:`SemanticModelDataset` objects (all aliases known), including the
        empty-attachment case (relationships with no datasets). Column checks
        run only for aliases whose dataset has a non-empty schema.
        """
        props = self._ensure_model_props()
        rels = props.relationships
        if not rels:
            return
        attached_by_urn = self._attached_datasets_by_urn()

        def flag(message: str, *, definitive: bool) -> None:
            if strict and definitive:
                raise SdkUsageError(message)
            logger.warning(message)

        # Structural checks on the relationship inputs themselves — always
        # definitive (independent of which datasets are attached).
        for rel in rels:
            if not (rel.from_ or "").strip() or not (rel.to or "").strip():
                flag(
                    f"SemanticModel {str(self.urn)}: relationship has a blank "
                    f"from/to alias (from={rel.from_!r}, to={rel.to!r}).",
                    definitive=True,
                )
            if not rel.fromColumns or not rel.toColumns:
                flag(
                    f"SemanticModel {str(self.urn)}: relationship "
                    f"{rel.from_!r}->{rel.to!r} has an empty join column list.",
                    definitive=True,
                )
            elif len(rel.fromColumns) != len(rel.toColumns):
                flag(
                    f"SemanticModel {str(self.urn)}: relationship "
                    f"{rel.from_!r}->{rel.to!r} joins {len(rel.fromColumns)} "
                    f"column(s) to {len(rel.toColumns)}; counts must match.",
                    definitive=True,
                )

        # Distinct datasets must not share an alias — the by-alias resolution
        # below would otherwise silently collapse them.
        alias_to_urn: Dict[str, str] = {}
        for ds_urn, ds in attached_by_urn.items():
            prior = alias_to_urn.get(ds.alias)
            if prior is not None and prior != ds_urn:
                flag(
                    f"SemanticModel {str(self.urn)}: alias {ds.alias!r} is "
                    f"assigned to multiple datasets ({prior}, {ds_urn}).",
                    definitive=True,
                )
            alias_to_urn[ds.alias] = ds_urn

        by_alias = {ds.alias: ds for ds in attached_by_urn.values()}
        known_aliases = set(by_alias)
        # Non-strict callers are construction-time setters that commonly run
        # before datasets are attached; stay quiet until there is something to
        # check. Strict (emit-time) validation still runs with no aliases so the
        # relationships-without-datasets case is caught.
        if not known_aliases and not strict:
            return
        # Graph-hydrated models do not reverse-lookup member datasets.
        # Structural checks above stay definitive; alias/column checks warn.
        hydrated = self._prev_aspects is not None

        for rel in rels:
            for alias, columns in (
                (rel.from_, rel.fromColumns),
                (rel.to, rel.toColumns),
            ):
                if not alias or not alias.strip():
                    continue  # blank aliases are handled structurally above
                if alias not in known_aliases:
                    flag(
                        f"SemanticModel {str(self.urn)}: relationship alias "
                        f"{alias!r} does not match any attached dataset alias "
                        f"(known: {sorted(known_aliases)}). Join path may be "
                        f"broken.",
                        definitive=not hydrated,
                    )
                    continue
                field_paths = {f.field_path for f in by_alias[alias].schema}
                if not field_paths:
                    continue  # schema unavailable; cannot validate columns
                missing = [c for c in (columns or []) if c not in field_paths]
                if missing:
                    flag(
                        f"SemanticModel {str(self.urn)}: relationship join "
                        f"column(s) {missing} not found in dataset {alias!r} "
                        f"(known: {sorted(field_paths)}).",
                        definitive=not hydrated,
                    )

    @property
    def ai_context(self) -> Optional[AiContextClass]:
        return self._get_aspect(AiContextClass)

    def set_ai_context(self, ai_context: AiContextInput) -> None:
        built = build_ai_context(ai_context)
        if built is not None:
            self._set_aspect(built)
            return
        # Empty input clears it. On a graph-hydrated model that previously had
        # an aiContext, emit an empty aspect to overwrite the server value —
        # as_mcps only emits present aspects, so a plain pop would leave the
        # server copy intact.
        if AiContextClass.ASPECT_NAME in (self._prev_aspects or {}):
            self._set_aspect(AiContextClass())
        else:
            self._aspects.pop(AiContextClass.ASPECT_NAME, None)  # type: ignore

    def as_mcps(
        self,
        change_type: Union[str, ChangeTypeClass] = ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:
        # By emit time all datasets and relationships are attached, so validate
        # join-path aliases against the full picture (a construction-time check
        # sees an incomplete set when datasets are attached after relationships).
        self._validate_relationships(strict=True)

        aspect_name = SemanticModelInfoClass.ASPECT_NAME
        prev = (self._prev_aspects or {}).get(aspect_name)
        curr = self._aspects.get(aspect_name)

        if prev is not None and curr is not None and curr == prev:
            # Hydrated, user did not touch semanticModelInfo. Skip emit so stored
            # datasets is preserved for migration read-back.
            popped = self._aspects.pop(aspect_name)  # type: ignore[misc]
            try:
                return super().as_mcps(change_type=change_type)
            finally:
                self._aspects[aspect_name] = popped  # type: ignore[literal-required]

        if isinstance(curr, SemanticModelInfoClass) and (curr.datasets or []):
            # Touched (or authored from scratch with stale datasets): strip
            # datasets on the emit copy so we do not write to the deprecated
            # field. Do not mutate the persistent aspect.
            emit_copy = copy.deepcopy(curr)
            emit_copy.datasets = []
            self._aspects[aspect_name] = emit_copy  # type: ignore[literal-required]
            try:
                return super().as_mcps(change_type=change_type)
            finally:
                self._aspects[aspect_name] = curr  # type: ignore[literal-required]

        return super().as_mcps(change_type=change_type)


class SemanticModelDataset(Dataset):
    """A logical dataset exposed by a :class:`SemanticModel`.

    This is a standard :class:`Dataset` carrying the ``Semantic Model Dataset``
    subtype and a ``semanticModelProperties`` back-reference to its owning
    semantic model. Per-field semantic metadata (``semanticFieldAnnotation``)
    and per-field AI hints (``aiContext``) are layered on each field's
    ``schemaField`` URN and emitted on serialization alongside the
    dataset-anchored aspects.

    The dataset ``name`` should encode ``<sm_path>.<sm_id>.<view_name>`` so
    logical datasets stay unique across semantic models.

    Per-field ``semanticFieldAnnotation`` and field-level ``aiContext`` are
    **create-only**: they are field-anchored (on ``schemaField`` URNs), not part
    of the dataset aspect bag. A logical dataset shares the ``dataset`` entity
    type, so ``client.entities.get(...)`` hydrates it as a base :class:`Dataset`
    and the annotations are not carried back on a read. To update one, rebuild a
    fresh ``SemanticModelDataset`` and re-attach its fields via the ``schema``
    constructor kwarg rather than read-modify-writing the fetched ``Dataset``.

    Server compatibility: requires a server build that includes the
    semanticModel/metric model (operator's responsibility — no automatic
    check). See :func:`datahub.sdk.require_metrics_support` for an opt-in
    preflight helper.
    """

    __slots__ = ("_semantic_field_annotations",)

    def __init__(
        self,
        *,
        platform: str,
        name: str,
        semantic_model: Union[str, SemanticModelUrn],
        alias: str,
        schema: Sequence[SemanticFieldInput],
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        description: Optional[str] = None,
        view_definition: Optional[str] = None,
        upstreams: Optional[UpstreamLineageInputType] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        # Initialize the per-field annotation store before delegating to
        # Dataset so _set_semantic_schema can populate it.
        self._semantic_field_annotations: Dict[str, _FieldAnnotation] = {}
        super().__init__(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=env,
            subtype=DatasetSubTypes.SEMANTIC_MODEL_DATASET,
            description=description,
            view_definition=view_definition,
            upstreams=upstreams,
            owners=owners,
            links=links,
            tags=tags,
            terms=terms,
            domain=domain,
            structured_properties=structured_properties,
            extra_aspects=extra_aspects,
        )
        self._set_semantic_model_properties(semantic_model=semantic_model, alias=alias)
        self._set_semantic_schema(schema, alias=alias)

    def _set_semantic_model_properties(
        self,
        *,
        semantic_model: Union[str, SemanticModelUrn],
        alias: str,
    ) -> None:
        semantic_model_urn = validate_semantic_model_urn(semantic_model)
        existing = self._get_aspect(SemanticModelPropertiesClass)
        if existing is None:
            self._set_aspect(
                SemanticModelPropertiesClass(
                    alias=alias, semanticModel=semantic_model_urn
                )
            )
        else:
            existing.alias = alias
            existing.semanticModel = semantic_model_urn

    def _set_semantic_model_back_ref(
        self, semantic_model: Union[str, SemanticModelUrn]
    ) -> None:
        """Reconcile the back-reference to the owning model, preserving alias."""
        props = self._get_aspect(SemanticModelPropertiesClass)
        assert props is not None
        props.semanticModel = str(semantic_model)

    @property
    def alias(self) -> str:
        props = self._get_aspect(SemanticModelPropertiesClass)
        assert props is not None
        return props.alias

    def _set_semantic_schema(
        self, schema: Sequence[SemanticFieldInput], *, alias: str
    ) -> None:
        platform_name = self.urn.get_data_platform_urn().platform_name
        schema_fields: List[SchemaFieldClass] = []
        for spec in schema:
            if spec.field_path in self._semantic_field_annotations:
                raise SdkUsageError(
                    f"Duplicate field_path {spec.field_path!r} in "
                    f"SemanticModelDataset {str(self.urn)}; each field must be "
                    f"unique."
                )
            schema_fields.append(
                self._build_schema_field(spec, platform_name=platform_name)
            )
            self._semantic_field_annotations[spec.field_path] = _FieldAnnotation(
                annotation=self._build_field_annotation(spec, alias=alias),
                ai_context=build_ai_context(spec.ai_context),
            )
        # Use the private schema setter so the schema is recorded exactly as
        # we built it (with per-field tags/isPartOfKey preserved).
        self._set_schema(schema_fields)

    @staticmethod
    def _build_schema_field(
        spec: SemanticFieldInput, *, platform_name: str
    ) -> SchemaFieldClass:
        from datahub.ingestion.source.sql.sql_types import resolve_sql_type
        from datahub.metadata.schema_classes import (
            NullTypeClass,
            SchemaFieldDataTypeClass,
        )

        resolved = resolve_sql_type(spec.type, platform=platform_name)
        field_type = resolved if resolved is not None else NullTypeClass()
        global_tags = None
        if spec.tags is not None:
            parsed = [
                SemanticModelDataset._parse_tag_association_class(tag)
                for tag in spec.tags
            ]
            global_tags = GlobalTagsClass(tags=parsed)
        return SchemaFieldClass(
            fieldPath=spec.field_path,
            type=SchemaFieldDataTypeClass(field_type),
            nativeDataType=spec.type,
            description=spec.description,
            nullable=spec.nullable,
            isPartOfKey=spec.is_part_of_key,
            globalTags=global_tags,
        )

    @staticmethod
    def _build_field_annotation(
        spec: SemanticFieldInput, *, alias: str
    ) -> SemanticFieldAnnotationClass:
        # expression is REQUIRED on the annotation; synthesize a trivial
        # qualified reference when the caller gives none.
        if spec.expression is None:
            expression: MetricExpressionClass = build_metric_expression(
                f"{alias}.{spec.field_path}"
            )
        else:
            expression = build_metric_expression(spec.expression)
        dimension: Optional[DimensionClass] = None
        if spec.semantic_type == SemanticFieldTypeClass.DIMENSION:
            dimension = DimensionClass(isTime=spec.is_time_dimension)
        return SemanticFieldAnnotationClass(
            type=spec.semantic_type,
            expression=expression,
            aggregationFunction=spec.aggregation_function,
            dimension=dimension,
        )

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: object) -> Self:  # type: ignore[override]
        assert isinstance(urn, DatasetUrn)
        # Bypass __init__ (which requires semantic_model/alias/schema) and
        # construct via the Entity base. _init_from_graph resets _aspects = {}
        # before repopulating from the graph. Field annotations are
        # create-only: a read-constructed instance has none.
        entity = cls.__new__(cls)
        Entity.__init__(entity, urn)  # type: ignore[arg-type]
        entity._semantic_field_annotations = {}
        return entity._init_from_graph(current_aspects)  # type: ignore[arg-type]

    def as_mcps(
        self,
        change_type: Union[str, ChangeTypeClass] = ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:  # type: ignore[override]
        mcps = super().as_mcps(change_type=change_type)
        # Emit schemaField-anchored semanticFieldAnnotation + aiContext MCPs,
        # propagating change_type so field aspects stay consistent with the
        # dataset-anchored ones (e.g. EntityClient.create requests CREATE).
        ds_urn = str(self.urn)
        for field_path, field_ann in self._semantic_field_annotations.items():
            field_urn = SchemaFieldUrn(ds_urn, field_path).urn()
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=field_urn,
                    aspect=field_ann.annotation,
                    changeType=change_type,
                )
            )
            if field_ann.ai_context is not None:
                mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=field_urn,
                        aspect=field_ann.ai_context,
                        changeType=change_type,
                    )
                )
        return mcps
