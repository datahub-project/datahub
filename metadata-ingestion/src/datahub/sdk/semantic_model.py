from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Set, Type, Union

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
    build_ai_context,
    build_metric_expression,
    make_audit_stamp,
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


SemanticModelDatasetInputType: TypeAlias = Union[
    str, DatasetUrn, "SemanticModelDataset"
]


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
    """A semantic model: a logical grouping of datasets with dimensional context.

    The semantic model is the bridge between raw datasets and the business
    metrics calculated over them. Each logical dataset it exposes is its own
    ``dataset`` entity (subtype ``Semantic Model Dataset``) carrying a
    ``semanticModelProperties`` back-reference; metrics point back at the model
    via ``metricInfo.semanticModel`` (the ``ModeledBy`` lineage edge).

    The canonical lineage chain is::

        Metric -> SemanticModel -> Logical Dataset -> Physical Dataset

    expressed entirely by ``metricInfo.semanticModel`` (ModeledBy),
    ``semanticModelInfo.datasets`` (Contains), and each logical dataset's own
    ``upstreamLineage``. Do not populate ``metricUpstreams`` for
    semantic-model-backed metrics.

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
            for dataset in datasets:
                self.add_dataset(dataset)

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
        props = self._ensure_model_props()
        return list(props.datasets) if props.datasets else []

    def add_dataset(self, dataset: SemanticModelDatasetInputType) -> None:
        """Attach a logical dataset to this model.

        If ``dataset`` is a :class:`SemanticModelDataset`, its
        ``semanticModelProperties.semanticModel`` back-reference is reconciled
        to this model's URN (so the caller does not have to set it twice) and
        its alias is left as the source of truth for relationship join paths.

        Insertion order is preserved across re-emits.
        """
        if isinstance(dataset, SemanticModelDataset):
            ds_urn = str(dataset.urn)
            dataset._set_semantic_model_back_ref(self.urn)
            self._attached_logical_datasets.append(dataset)
        else:
            ds_urn = str(dataset)
        props = self._ensure_model_props()
        if props.datasets is None:
            props.datasets = []
        if ds_urn not in props.datasets:
            props.datasets.append(ds_urn)

    def set_datasets(self, datasets: Sequence[SemanticModelDatasetInputType]) -> None:
        self._ensure_model_props().datasets = []
        self._attached_logical_datasets = []
        for dataset in datasets:
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
        # Keyed by URN so duplicate attachments of the same dataset collapse,
        # matching how props.datasets dedupes URNs.
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

        Alias checks raise (under ``strict``) only under *full* alias coverage:
        every declared dataset URN is an attached :class:`SemanticModelDataset`,
        so all aliases are known. Datasets attached as raw URN strings carry no
        alias/schema, so mismatches there downgrade to a warning. Column checks
        run only for aliases whose dataset is attached with a non-empty schema.
        """
        props = self._ensure_model_props()
        rels = props.relationships
        if not rels:
            return
        attached_by_urn = self._attached_datasets_by_urn()
        by_alias = {ds.alias: ds for ds in attached_by_urn.values()}
        known_aliases = set(by_alias)
        # Non-strict callers are construction-time setters that commonly run
        # before datasets are attached; stay quiet until there is something to
        # check. Strict (emit-time) validation still runs with no aliases so the
        # relationships-without-datasets case is caught.
        if not known_aliases and not strict:
            return
        # Subset check (not a length comparison): duplicates and instance/URN
        # mixes no longer break coverage detection. An empty declared set with
        # relationships present is full coverage too — the aliases can't match
        # anything, which is exactly the broken case we want to raise on.
        declared_urns = set(props.datasets or [])
        full_coverage = declared_urns <= set(attached_by_urn)

        def flag(message: str, *, definitive: bool) -> None:
            if strict and definitive:
                raise SdkUsageError(message)
            logger.warning(message)

        for rel in rels:
            for alias, columns in (
                (rel.from_, rel.fromColumns),
                (rel.to, rel.toColumns),
            ):
                if not alias:
                    continue
                if alias not in known_aliases:
                    flag(
                        f"SemanticModel {str(self.urn)}: relationship alias "
                        f"{alias!r} does not match any attached dataset alias "
                        f"(known: {sorted(known_aliases)}). Join path may be "
                        f"broken.",
                        definitive=full_coverage,
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
                        definitive=True,
                    )

    @property
    def ai_context(self) -> Optional[AiContextClass]:
        return self._get_aspect(AiContextClass)

    def set_ai_context(self, ai_context: AiContextInput) -> None:
        built = build_ai_context(ai_context)
        if built is None:
            # Don't emit an empty aiContext; drop any previously set one.
            self._aspects.pop(AiContextClass.ASPECT_NAME, None)  # type: ignore
            return
        self._set_aspect(built)

    def as_mcps(
        self,
        change_type: Union[str, ChangeTypeClass] = ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:
        # By emit time all datasets and relationships are attached, so validate
        # join-path aliases against the full picture (a construction-time check
        # sees an incomplete set when datasets are attached after relationships).
        self._validate_relationships(strict=True)
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
        existing = self._get_aspect(SemanticModelPropertiesClass)
        if existing is None:
            self._set_aspect(
                SemanticModelPropertiesClass(
                    alias=alias, semanticModel=str(semantic_model)
                )
            )
        else:
            existing.alias = alias
            existing.semanticModel = str(semantic_model)

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
