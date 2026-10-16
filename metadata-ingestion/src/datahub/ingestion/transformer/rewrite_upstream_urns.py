import copy
from enum import auto
from typing import List, Optional, cast

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigEnum, ConfigModel
from datahub.emitter.mce_builder import Aspect, make_schema_field_urn
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


def _short_platform(value: str) -> str:
    platform = value.strip()
    if platform.startswith(_PLATFORM_URN_PREFIX):
        platform = platform[len(_PLATFORM_URN_PREFIX) :]
    return platform


class RewriteUpstreamNameRule(ConfigModel):
    """Literal find-and-replace applied to an upstream dataset's name."""

    find: str = Field(description="Substring to find in the dataset name.")
    replace: str = Field(description="Text that replaces each match of find.")

    @field_validator("find")
    @classmethod
    def find_must_not_be_empty(cls, value: str) -> str:
        if value == "":
            raise ValueError("find must not be empty")
        return value


class RewriteUpstreamPlatformRule(ConfigModel):
    """Match a platform, replace it, then rewrite the dataset name."""

    find: str = Field(
        description="Substring to find in the platform name, for example 'mssql'."
    )
    replace: str = Field(description="Platform name that replaces each match of find.")
    id_rules: List[RewriteUpstreamNameRule] = Field(
        default_factory=list,
        description="Name rules applied only to upstreams whose platform matched find. "
        "They run in list order, and each rule sees the name produced by the previous one.",
    )

    @field_validator("find", "replace")
    @classmethod
    def normalize_platform_token(cls, value: str) -> str:
        platform = _short_platform(value)
        if not platform:
            raise ValueError("platform find and replace must not be empty")
        return platform


class RewriteUpstreamUrnsConfig(ConfigModel):
    """Rewrite upstream dataset URNs by platform, then by name within that platform."""

    platform_rules: List[RewriteUpstreamPlatformRule] = Field(
        default_factory=list,
        description="Platform rules applied in list order. A rule runs only when its "
        "find is present in the platform name at that point. Nested id_rules then "
        "run for that upstream. Matching is a literal substring replace, not a "
        "regular expression. An upstream whose platform matches no rule is left unchanged. "
        "When this list is empty, id_rules below apply to every upstream and the "
        "platform is left as it is.",
    )
    id_rules: List[RewriteUpstreamNameRule] = Field(
        default_factory=list,
        description="Name rules applied to every upstream when platform_rules is empty. "
        "They run in list order. Each rule sees the name produced by the previous one.",
    )
    mode: RewriteUpstreamMode = Field(
        default=RewriteUpstreamMode.REPLACE,
        description="replace overwrites each upstream URN. add keeps the original "
        "upstream and also emits the rewritten URN.",
    )


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

        platform_name = dataset_urn.get_data_platform_urn().platform_name
        name = dataset_urn.name
        if self.config.platform_rules:
            for rule in self.config.platform_rules:
                if rule.find not in platform_name:
                    continue
                platform_name = platform_name.replace(rule.find, rule.replace)
                for name_rule in rule.id_rules:
                    name = name.replace(name_rule.find, name_rule.replace)
        else:
            for name_rule in self.config.id_rules:
                name = name.replace(name_rule.find, name_rule.replace)

        if (
            platform_name == dataset_urn.get_data_platform_urn().platform_name
            and name == dataset_urn.name
        ):
            return urn
        return str(DatasetUrn(platform=platform_name, name=name, env=dataset_urn.env))
