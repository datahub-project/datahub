from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Type, Union

from typing_extensions import Self, TypeAlias

from datahub.emitter.mce_builder import DEFAULT_ENV, make_ts_millis, parse_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    ChangeTypeClass,
    DialectClass,
    DialectExpressionClass,
    DimensionClass,
    ERModelRelationshipCardinalityClass,
    GlobalTagsClass,
    MetricExpressionClass,
    SchemaFieldClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SemanticModelRelationshipClass,
    StatusClass,
    TagAssociationClass,
)
from datahub.metadata.urns import (
    DatasetUrn,
    SchemaFieldUrn,
    SemanticModelUrn,
    Urn,
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
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity, ExtraAspectsType

_DEFAULT_ACTOR_URN = "urn:li:corpuser:__ingestion"


class SemanticFieldType(SemanticFieldTypeClass):
    pass


class ERModelRelationshipCardinality(ERModelRelationshipCardinalityClass):
    pass


@dataclass
class AiContextInput:
    """Input container for the first-class ``aiContext`` aspect.

    The aspect is only emitted when at least one field carries content; an
    all-empty ``AiContextInput`` produces no aspect.
    """

    synonyms: Optional[List[str]] = None
    instructions: Optional[str] = None
    examples: Optional[List[str]] = None
    custom_instructions: Optional[str] = None


@dataclass
class DialectExpressionInput:
    """A single (dialect, expression) pair for a metric or field expression."""

    expression: str
    dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL


MetricExpressionInputType: TypeAlias = Union[
    str,
    DialectExpressionInput,
    List[DialectExpressionInput],
    MetricExpressionClass,
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
    cardinality: Optional[ERModelRelationshipCardinalityClass] = None
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


def _ai_context_is_empty(ai: Optional[AiContextInput]) -> bool:
    if ai is None:
        return True
    return not (ai.synonyms or ai.instructions or ai.examples or ai.custom_instructions)


def _build_ai_context(ai: Optional[AiContextInput]) -> Optional[AiContextClass]:
    if _ai_context_is_empty(ai):
        return None
    return AiContextClass(
        synonyms=list(ai.synonyms) if ai.synonyms else None,  # type: ignore[union-attr]
        instructions=ai.instructions,
        examples=list(ai.examples) if ai.examples else None,  # type: ignore[union-attr]
        customInstructions=ai.custom_instructions,
    )


def _build_metric_expression(
    expression: MetricExpressionInputType,
    *,
    default_dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL,
) -> MetricExpressionClass:
    if isinstance(expression, MetricExpressionClass):
        return expression
    if isinstance(expression, DialectExpressionInput):
        dialects = [
            DialectExpressionClass(
                dialect=expression.dialect, expression=expression.expression
            )
        ]
    elif isinstance(expression, str):
        dialects = [
            DialectExpressionClass(dialect=default_dialect, expression=expression)
        ]
    elif isinstance(expression, list):
        dialects = [
            DialectExpressionClass(dialect=item.dialect, expression=item.expression)
            for item in expression
        ]
    else:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported expression input type: {type(expression)!r}")
    return MetricExpressionClass(dialects=dialects)


def _make_audit_stamp(ts: Optional[datetime]) -> Optional[AuditStampClass]:
    if ts is None:
        return None
    return AuditStampClass(time=make_ts_millis(ts), actor=_DEFAULT_ACTOR_URN)


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
    """

    __slots__ = ()

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
        self._ensure_model_props().created = _make_audit_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        stamp = self._ensure_model_props().lastModified
        if stamp is None or stamp.time == 0:
            return None
        return parse_ts_millis(stamp.time)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_model_props().lastModified = _make_audit_stamp(last_modified)

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
        else:
            ds_urn = str(dataset)
        props = self._ensure_model_props()
        if props.datasets is None:
            props.datasets = []
        if ds_urn not in props.datasets:
            props.datasets.append(ds_urn)

    def set_datasets(self, datasets: Sequence[SemanticModelDatasetInputType]) -> None:
        self._ensure_model_props().datasets = []
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

    def add_relationship(self, relationship: SemanticModelRelationshipInput) -> None:
        props = self._ensure_model_props()
        if props.relationships is None:
            props.relationships = []
        props.relationships.append(self._build_relationship(relationship))

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
            aiContext=_build_ai_context(rel.ai_context),
        )

    @property
    def ai_context(self) -> Optional[AiContextClass]:
        return self._get_aspect(AiContextClass)

    def set_ai_context(self, ai_context: AiContextInput) -> None:
        built = _build_ai_context(ai_context)
        if built is None:
            # Don't emit an empty aiContext; drop any previously set one.
            self._aspects.pop(AiContextClass.ASPECT_NAME, None)  # type: ignore[union-attr]
            return
        self._set_aspect(built)


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
        upstreams: Optional[object] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        # Build SchemaFieldClass list + per-field annotation/aiContext records
        # before delegating to Dataset so the platform is known for type
        # resolution.
        super().__init__(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=env,
            subtype=DatasetSubTypes.SEMANTIC_MODEL_DATASET,
            description=description,
            view_definition=view_definition,
            upstreams=upstreams,  # type: ignore[arg-type]
            owners=owners,
            links=links,
            tags=tags,
            terms=terms,
            domain=domain,
            structured_properties=structured_properties,
            extra_aspects=extra_aspects,
        )
        self._semantic_field_annotations: Dict[str, _FieldAnnotation] = {}
        self._set_semantic_model_back_ref(semantic_model)
        self._set_alias(alias)
        self._set_semantic_schema(schema, alias=alias)

    def _set_semantic_model_back_ref(
        self, semantic_model: Union[str, SemanticModelUrn]
    ) -> None:
        existing = self._get_aspect(SemanticModelPropertiesClass)
        if existing is None:
            self._set_aspect(
                SemanticModelPropertiesClass(
                    alias="", semanticModel=str(semantic_model)
                )
            )
        else:
            existing.semanticModel = str(semantic_model)

    def _set_alias(self, alias: str) -> None:
        props = self._get_aspect(SemanticModelPropertiesClass)
        assert props is not None
        props.alias = alias

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
            schema_fields.append(
                self._build_schema_field(spec, platform_name=platform_name)
            )
            self._semantic_field_annotations[spec.field_path] = _FieldAnnotation(
                annotation=self._build_field_annotation(spec, alias=alias),
                ai_context=_build_ai_context(spec.ai_context),
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
            parsed = [TagAssociationClass(tag=str(tag)) for tag in spec.tags]
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
            expression: MetricExpressionClass = _build_metric_expression(
                f"{alias}.{spec.field_path}"
            )
        else:
            expression = _build_metric_expression(spec.expression)
        dimension: Optional[DimensionClass] = None
        if spec.semantic_type == SemanticFieldTypeClass.DIMENSION:
            dimension = DimensionClass(isTime=spec.is_time_dimension)
        return SemanticFieldAnnotationClass(
            type=spec.semantic_type,
            expression=expression,
            aggregationFunction=spec.aggregation_function,
            dimension=dimension,
        )

    def as_mcps(
        self,
        change_type: Union[str, ChangeTypeClass] = ChangeTypeClass.UPSERT,
    ) -> List[MetadataChangeProposalWrapper]:  # type: ignore[override]
        mcps = super().as_mcps(change_type=change_type)
        # Emit schemaField-anchored semanticFieldAnnotation + aiContext MCPs.
        ds_urn = str(self.urn)
        for field_path, field_ann in self._semantic_field_annotations.items():
            field_urn = SchemaFieldUrn(ds_urn, field_path).urn()
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=field_urn, aspect=field_ann.annotation
                )
            )
            if field_ann.ai_context is not None:
                mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=field_urn, aspect=field_ann.ai_context
                    )
                )
        return mcps
