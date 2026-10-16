import copy
from enum import auto
from typing import List, Optional, cast

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigEnum, ConfigModel
from datahub.emitter.mce_builder import (
    Aspect,
    make_data_platform_urn,
    make_schema_field_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import UpstreamClass, UpstreamLineageClass
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.error import InvalidUrnError

_PLATFORM_URN_PREFIX = "urn:li:dataPlatform:"


class RewriteUpstreamMode(ConfigEnum):
    """Whether a rewritten upstream replaces the original or is added beside it."""

    REPLACE = auto()
    ADD = auto()


class RewriteUpstreamUrnRule(ConfigModel):
    """Literal find-and-replace applied to an upstream dataset's name."""

    find: str = Field(description="Substring to find in the dataset name.")
    replace: str = Field(description="Text that replaces each match of find.")

    @field_validator("find")
    @classmethod
    def find_must_not_be_empty(cls, value: str) -> str:
        if value == "":
            raise ValueError("find must not be empty")
        return value


class RewriteUpstreamUrnsConfig(ConfigModel):
    """Rewrite upstream dataset URNs: optionally set the platform, then apply name rules in order."""

    platform: Optional[str] = Field(
        default=None,
        description="Platform name written onto every upstream dataset URN, "
        "for example 'fabric'. A dataPlatform URN is accepted and reduced to its name. "
        "When omitted, each upstream keeps its original platform.",
    )
    rules: List[RewriteUpstreamUrnRule] = Field(
        default_factory=list,
        description="Find-and-replace rules applied to the dataset name, in list order. "
        "Each rule sees the name produced by the previous rule. Matching is a literal "
        "substring replace, not a regular expression.",
    )
    mode: RewriteUpstreamMode = Field(
        default=RewriteUpstreamMode.REPLACE,
        description="replace overwrites each upstream URN. add keeps the original "
        "upstream and also emits the rewritten URN.",
    )

    @field_validator("platform")
    @classmethod
    def normalize_platform(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        platform = value.strip()
        if platform.startswith(_PLATFORM_URN_PREFIX):
            platform = platform[len(_PLATFORM_URN_PREFIX) :]
        if not platform:
            raise ValueError("platform must not be empty")
        return platform


class RewriteUpstreamUrns(BaseTransformer, SingleAspectTransformer):
    """Rewrite dataset URNs inside an upstreamLineage aspect.

    Table-level upstreams and fine-grained upstreams are rewritten. Fine-grained
    downstreams stay as they are, because those URNs belong to the dataset this
    aspect describes.
    """

    def __init__(self, config: RewriteUpstreamUrnsConfig, ctx: PipelineContext):
        super().__init__()
        self.config = config
        self.ctx = ctx
        self._platform_urn = (
            make_data_platform_urn(config.platform) if config.platform else None
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "RewriteUpstreamUrns":
        config = RewriteUpstreamUrnsConfig.model_validate(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "upstreamLineage"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        if aspect is None:
            return None

        lineage = cast(UpstreamLineageClass, copy.deepcopy(aspect))
        lineage.upstreams = self._rewrite_upstreams(lineage.upstreams)
        for fine_grained in lineage.fineGrainedLineages or []:
            if fine_grained.upstreams:
                fine_grained.upstreams = self._rewrite_urn_list(fine_grained.upstreams)
        return cast(Aspect, lineage)

    def _rewrite_upstreams(self, upstreams: List[UpstreamClass]) -> List[UpstreamClass]:
        rewritten = []
        for upstream in upstreams:
            new_urn = self._rewrite_urn(upstream.dataset)
            if (
                self.config.mode == RewriteUpstreamMode.ADD
                and new_urn != upstream.dataset
            ):
                rewritten.append(upstream)
                added = copy.deepcopy(upstream)
                added.dataset = new_urn
                rewritten.append(added)
            else:
                upstream.dataset = new_urn
                rewritten.append(upstream)
        return rewritten

    def _rewrite_urn_list(self, urns: List[str]) -> List[str]:
        rewritten = []
        for urn in urns:
            new_urn = self._rewrite_urn(urn)
            if self.config.mode == RewriteUpstreamMode.ADD and new_urn != urn:
                rewritten.append(urn)
            rewritten.append(new_urn)
        return rewritten

    def _rewrite_urn(self, urn: str) -> str:
        try:
            field_urn = SchemaFieldUrn.from_string(urn)
        except InvalidUrnError:
            return self._rewrite_dataset_urn(urn)

        new_parent = self._rewrite_dataset_urn(field_urn.parent)
        if new_parent == field_urn.parent:
            return urn
        return make_schema_field_urn(new_parent, field_urn.field_path)

    def _rewrite_dataset_urn(self, urn: str) -> str:
        try:
            dataset_urn = DatasetUrn.from_string(urn)
        except InvalidUrnError:
            return urn

        name = dataset_urn.name
        for rule in self.config.rules:
            name = name.replace(rule.find, rule.replace)
        platform = self._platform_urn or dataset_urn.platform
        if name == dataset_urn.name and platform == dataset_urn.platform:
            return urn
        return str(DatasetUrn(platform=platform, name=name, env=dataset_urn.env))
