from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Sequence, Type, Union

from typing_extensions import Self, TypeAlias

from datahub.emitter.mce_builder import parse_ts_millis
from datahub.metadata.schema_classes import (
    AiContextClass,
    DerivedMetricInputClass,
    DialectClass,
    EdgeClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    StatusClass,
)
from datahub.metadata.urns import DatasetUrn, MetricUrn, SemanticModelUrn, Urn
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
from datahub.sdk.entity import Entity, ExtraAspectsType

__all__ = [
    "AiContextInput",
    "DialectExpressionInput",
    "DerivedFromInputType",
    "Metric",
    "MetricExpressionInputType",
    "SemanticModelInputType",
    "UpstreamDatasetInputType",
]

SemanticModelInputType: TypeAlias = Union[str, SemanticModelUrn]
DerivedFromInputType: TypeAlias = Union[str, MetricUrn]
UpstreamDatasetInputType: TypeAlias = Union[str, DatasetUrn]


class Metric(
    HasPlatformInstance,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    HasStructuredProperties,
    Entity,
):
    """A metric: a business measure calculated over a semantic model.

    A semantic-model-backed metric points at its owning model via
    ``metricInfo.semanticModel`` (``ModeledBy``). Its lineage flows
    ``Metric -> Logical Dataset -> Physical Dataset`` via
    ``metricUpstreams.datasetUpstreams`` (pointing at Semantic Model Dataset
    URNs) and each logical dataset's ``upstreamLineage``.

    This builder emits semantic-model-backed metrics; ``semantic_model`` is
    required. Pass ``upstream_datasets`` with the logical-dataset URNs the
    metric reads from so lineage is authored.

    ``metricRelationships`` is always emitted (even with empty ``derivedFrom``)
    so ``hasParentMetric`` indexes as false. ``metricUpstreams`` is likewise
    always emitted (even with empty ``datasetUpstreams``) so re-emits clear
    stale upstreams. ``metricInfo.expression`` is optional and is omitted when
    not provided.

    Server compatibility: requires a server build that includes the
    semanticModel/metric model (operator's responsibility — no automatic
    check). See :func:`datahub.sdk.require_metrics_support` for an opt-in
    preflight helper.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[MetricUrn]:
        return MetricUrn

    def __init__(
        self,
        *,
        platform: str,
        path: str,
        id: str,
        semantic_model: SemanticModelInputType,
        platform_instance: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        expression: Optional[MetricExpressionInputType] = None,
        derived_from: Optional[Sequence[DerivedFromInputType]] = None,
        upstream_datasets: Optional[Sequence[UpstreamDatasetInputType]] = None,
        ai_context: Optional[AiContextInput] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = MetricUrn(platform=platform, path=path, id=id)
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)
        self._set_platform_instance(urn.platform, platform_instance)
        # Status is part of the producer contract for this entity.
        self._set_aspect(StatusClass(removed=False))
        self._ensure_metric_props(
            semantic_model=validate_semantic_model_urn(semantic_model)
        )

        if name is not None:
            self.set_name(name)
        if description is not None:
            self.set_description(description)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)
        if expression is not None:
            self.set_expression(expression)
        if ai_context is not None:
            self.set_ai_context(ai_context)
        # Always emit metricRelationships so hasParentMetric indexes as false.
        self.set_derived_from(derived_from or [])
        # Always emit metricUpstreams so re-emits clear stale datasetUpstreams.
        self.set_upstream_datasets(upstream_datasets or [])
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

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: object) -> Self:  # type: ignore[override]
        assert isinstance(urn, MetricUrn)
        # Construct without routing through the strict __init__ (which requires
        # semantic_model). _init_from_graph resets self._aspects = {} before
        # repopulating from the graph, so the placeholder props created here
        # are discarded.
        entity = cls.__new__(cls)
        Entity.__init__(entity, urn)  # type: ignore[arg-type]
        return entity._init_from_graph(current_aspects)  # type: ignore[arg-type]

    @property
    def urn(self) -> MetricUrn:
        return self._urn  # type: ignore

    def _ensure_metric_props(
        self, *, semantic_model: Optional[str] = None
    ) -> MetricInfoClass:
        props = self._get_aspect(MetricInfoClass)
        if props is None:
            # name is required on the aspect; default to the URN id. semanticModel
            # is also required by the contract; use the provided value or None
            # (only the _new_from_graph path passes None, and it resets aspects
            # before repopulating).
            props = MetricInfoClass(
                name=self.urn.id,
                semanticModel=semantic_model,
            )
            self._set_aspect(props)
        return props

    @property
    def name(self) -> str:
        return self._ensure_metric_props().name

    def set_name(self, name: str) -> None:
        self._ensure_metric_props().name = name

    @property
    def description(self) -> Optional[str]:
        return self._ensure_metric_props().description

    def set_description(self, description: str) -> None:
        self._ensure_metric_props().description = description

    @property
    def created(self) -> Optional[datetime]:
        stamp = self._ensure_metric_props().created
        if stamp is None or stamp.time == 0:
            return None
        return parse_ts_millis(stamp.time)

    def set_created(self, created: datetime) -> None:
        self._ensure_metric_props().created = make_audit_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        stamp = self._ensure_metric_props().lastModified
        if stamp is None or stamp.time == 0:
            return None
        return parse_ts_millis(stamp.time)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_metric_props().lastModified = make_audit_stamp(last_modified)

    @property
    def semantic_model(self) -> Optional[str]:
        return self._ensure_metric_props().semanticModel

    def set_semantic_model(self, semantic_model: SemanticModelInputType) -> None:
        self._ensure_metric_props().semanticModel = validate_semantic_model_urn(
            semantic_model
        )

    @property
    def expression(self) -> Optional[MetricExpressionClass]:
        return self._ensure_metric_props().expression

    def set_expression(
        self,
        expression: MetricExpressionInputType,
        *,
        default_dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL,
    ) -> None:
        self._ensure_metric_props().expression = build_metric_expression(
            expression, default_dialect=default_dialect
        )

    @property
    def derived_from(self) -> List[DerivedMetricInputClass]:
        rels = self._get_aspect(MetricRelationshipsClass)
        if rels is None or rels.derivedFrom is None:
            return []
        return list(rels.derivedFrom)

    def _ensure_metric_relationships(self) -> MetricRelationshipsClass:
        # Always present so hasParentMetric indexes as false; on a graph-hydrated
        # metric this preserves server-set parentMetric/relatedMetrics instead of
        # clobbering them when only derivedFrom changes.
        rels = self._get_aspect(MetricRelationshipsClass)
        if rels is None:
            rels = MetricRelationshipsClass()
            self._set_aspect(rels)
        return rels

    def set_derived_from(self, derived_from: Sequence[DerivedFromInputType]) -> None:
        # Mutate only derivedFrom; leave parentMetric/relatedMetrics untouched.
        # Normalize so a bare URN string isn't iterated character-by-character.
        self._ensure_metric_relationships().derivedFrom = [
            DerivedMetricInputClass(destinationUrn=str(d))
            for d in as_input_list(derived_from)
        ]

    def add_derived_from(self, metric: DerivedFromInputType) -> None:
        rels = self._ensure_metric_relationships()
        dest = str(metric)
        if all(d.destinationUrn != dest for d in rels.derivedFrom):
            rels.derivedFrom.append(DerivedMetricInputClass(destinationUrn=dest))

    def _ensure_metric_upstreams(self) -> MetricUpstreamsClass:
        upstreams = self._get_aspect(MetricUpstreamsClass)
        if upstreams is None:
            upstreams = MetricUpstreamsClass()
            self._set_aspect(upstreams)
        return upstreams

    @property
    def upstream_datasets(self) -> List[str]:
        upstreams = self._get_aspect(MetricUpstreamsClass)
        if upstreams is None or upstreams.datasetUpstreams is None:
            return []
        return [edge.destinationUrn for edge in upstreams.datasetUpstreams]

    def set_upstream_datasets(
        self, upstream_datasets: Sequence[UpstreamDatasetInputType]
    ) -> None:
        """Set the logical (or physical) dataset URNs this metric reads from.

        For semantic-model-backed metrics these should be Semantic Model
        Dataset URNs so lineage flows Metric → SMD → Physical Dataset.
        """
        self._ensure_metric_upstreams().datasetUpstreams = [
            EdgeClass(destinationUrn=str(DatasetUrn.from_string(str(ds))))
            for ds in as_input_list(upstream_datasets)
        ]

    @property
    def ai_context(self) -> Optional[AiContextClass]:
        return self._get_aspect(AiContextClass)

    def set_ai_context(self, ai_context: AiContextInput) -> None:
        built = build_ai_context(ai_context)
        if built is not None:
            self._set_aspect(built)
            return
        # Empty input clears it. On a graph-hydrated metric that previously had
        # an aiContext, emit an empty aspect to overwrite the server value —
        # as_mcps only emits present aspects, so a plain pop would leave the
        # server copy intact.
        if AiContextClass.ASPECT_NAME in (self._prev_aspects or {}):
            self._set_aspect(AiContextClass())
        else:
            self._aspects.pop(AiContextClass.ASPECT_NAME, None)  # type: ignore
