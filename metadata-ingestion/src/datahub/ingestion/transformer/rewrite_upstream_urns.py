import copy
import json
from enum import auto
from typing import Any, Iterable, List, Optional, Set, cast

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import ConfigEnum, ConfigModel
from datahub.emitter.aspect import JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mce_builder import Aspect, make_schema_field_urn
from datahub.emitter.mcp_patch_builder import _Patch
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)
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
        description="Platform name to match exactly, for example 'mssql'."
    )
    replace: str = Field(
        description="Platform name that replaces the matched platform."
    )
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
        description="Platform rules. The first rule whose find equals the upstream's "
        "platform name is applied, and its nested id_rules then run for that upstream. "
        "An upstream whose platform matches no rule is left unchanged. "
        "Cannot be combined with top-level id_rules.",
    )
    id_rules: List[RewriteUpstreamNameRule] = Field(
        default_factory=list,
        description="Name rules applied to every upstream, keeping its platform. "
        "They run in list order. Each rule sees the name produced by the previous one. "
        "Cannot be combined with platform_rules; put name rules under a platform rule instead.",
    )
    mode: RewriteUpstreamMode = Field(
        default=RewriteUpstreamMode.REPLACE,
        description="replace overwrites each upstream URN. add keeps the original "
        "upstream and also emits the rewritten URN.",
    )

    @model_validator(mode="after")
    def reject_top_level_id_rules_with_platform_rules(
        self,
    ) -> "RewriteUpstreamUrnsConfig":
        # Top-level id_rules only run when platform_rules is empty, so allowing both
        # would silently drop the top-level rules.
        if self.platform_rules and self.id_rules:
            raise ValueError(
                "id_rules cannot be combined with platform_rules. "
                "Move the name rules under the platform rule they apply to."
            )
        return self


class RewriteUpstreamUrns(BaseTransformer, SingleAspectTransformer):
    """Rewrite dataset URNs inside an upstreamLineage aspect.

    Table-level upstreams and fine-grained upstreams are rewritten. Fine-grained
    downstreams stay as they are, because those URNs belong to the dataset this
    aspect describes.

    Incremental lineage emits this aspect as a JSON patch on a raw metadata change
    proposal. Those proposals are rewritten here, because SingleAspectTransformer
    only receives proposal wrappers.
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

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        def _rewrite_patches() -> Iterable[RecordEnvelope]:
            for envelope in record_envelopes:
                record = envelope.record
                if (
                    isinstance(record, MetadataChangeProposalClass)
                    and record.aspectName == self.aspect_name()
                    and record.changeType == ChangeTypeClass.PATCH
                    and isinstance(record.aspect, GenericAspectClass)
                    and record.entityUrn
                ):
                    rewritten = self._rewrite_lineage_patch(record.aspect)
                    if rewritten is not None:
                        record.aspect = rewritten
                        self._mark_processed(record.entityUrn)
                yield envelope

        yield from super().transform(_rewrite_patches())

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
        # A rewrite can land on a URN that is already an upstream, or two upstreams
        # can rewrite to the same URN. Keep the first entry for each dataset.
        rewritten: List[UpstreamClass] = []
        seen: Set[str] = set()

        def _keep(upstream: UpstreamClass) -> None:
            if upstream.dataset not in seen:
                seen.add(upstream.dataset)
                rewritten.append(upstream)

        for upstream in upstreams:
            new_urn = self._rewrite_urn(upstream.dataset)
            if (
                self.config.mode == RewriteUpstreamMode.ADD
                and new_urn != upstream.dataset
            ):
                _keep(upstream)
                added = copy.deepcopy(upstream)
                added.dataset = new_urn
                _keep(added)
            else:
                upstream.dataset = new_urn
                _keep(upstream)
        return rewritten

    def _rewrite_urn_list(self, urns: List[str]) -> List[str]:
        rewritten: List[str] = []
        for urn in urns:
            new_urn = self._rewrite_urn(urn)
            if self.config.mode == RewriteUpstreamMode.ADD and new_urn != urn:
                rewritten.append(urn)
            rewritten.append(new_urn)
        return list(dict.fromkeys(rewritten))

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
            # Platform names are identifiers, so match them exactly. A substring
            # match would turn "mysql" into "my<replace>" for find "sql". Only the
            # first matching rule applies, so a later rule cannot re-match the
            # platform an earlier rule produced.
            rule = next(
                (r for r in self.config.platform_rules if r.find == platform_name),
                None,
            )
            if rule is not None:
                platform_name = rule.replace
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

    def _rewrite_lineage_patch(
        self, aspect: GenericAspectClass
    ) -> Optional[GenericAspectClass]:
        if aspect.contentType != JSON_PATCH_CONTENT_TYPE:
            return None
        try:
            payload = json.loads(aspect.value)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        if isinstance(payload, dict) and isinstance(payload.get("patch"), list):
            operations = payload["patch"]
            container: Any = payload
        elif isinstance(payload, list):
            operations = payload
            container = None
        else:
            return None

        rewritten_operations = []
        for operation in operations:
            if isinstance(operation, dict):
                rewritten_operations.extend(self._rewrite_patch_operation(operation))
            else:
                rewritten_operations.append(operation)
        if rewritten_operations == operations:
            return None

        if container is None:
            encoded = rewritten_operations
        else:
            container = copy.deepcopy(container)
            container["patch"] = rewritten_operations
            encoded = container
        aspect.value = json.dumps(encoded).encode()
        return aspect

    def _rewrite_patch_operation(self, operation: dict) -> List[dict]:
        components = _pointer_components(operation.get("path", ""))
        value = operation.get("value")
        if not components:
            return [operation]

        if components[0] == "upstreams" and len(components) >= 2:
            original_urn = components[1]
            rewritten_value = _rewrite_upstream_value(self, value)
            new_urn = (
                rewritten_value.get("dataset", original_urn)
                if isinstance(rewritten_value, dict)
                else self._rewrite_urn(original_urn)
            )
            if new_urn == original_urn and rewritten_value == value:
                return [operation]
            rewritten = _replace_operation(
                operation,
                ["upstreams", new_urn, *components[2:]],
                rewritten_value,
            )
            if self.config.mode == RewriteUpstreamMode.ADD and new_urn != original_urn:
                return [operation, rewritten]
            return [rewritten]

        if components == ["upstreams"] and isinstance(value, list):
            rewritten_value = []
            seen_datasets: Set[str] = set()

            def _keep(item: dict) -> None:
                if item["dataset"] not in seen_datasets:
                    seen_datasets.add(item["dataset"])
                    rewritten_value.append(item)

            for item in value:
                if not isinstance(item, dict) or not isinstance(
                    item.get("dataset"), str
                ):
                    rewritten_value.append(item)
                    continue
                new_item = _rewrite_upstream_value(self, item)
                if self.config.mode == RewriteUpstreamMode.ADD and new_item.get(
                    "dataset"
                ) != item.get("dataset"):
                    _keep(item)
                _keep(new_item)
            if rewritten_value == value:
                return [operation]
            return [_replace_operation(operation, components, rewritten_value)]

        # The upstream field URN is the last component. The downstream field URN
        # sits earlier in the path and belongs to the dataset being ingested.
        if components[0] == "fineGrainedLineages" and len(components) >= 5:
            original_urn = components[-1]
            new_urn = self._rewrite_urn(original_urn)
            if new_urn == original_urn:
                return [operation]
            rewritten = _replace_operation(
                operation, [*components[:-1], new_urn], value
            )
            if self.config.mode == RewriteUpstreamMode.ADD:
                return [operation, rewritten]
            return [rewritten]

        return [operation]


def _pointer_components(path: str) -> List[str]:
    if not path.startswith("/"):
        return []
    return [_Patch.unquote_path_component(part) for part in path[1:].split("/")]


def _pointer(components: List[str]) -> str:
    return "/" + "/".join(_Patch.quote_path_component(part) for part in components)


def _replace_operation(operation: dict, components: List[str], value: Any) -> dict:
    rewritten = dict(operation)
    rewritten["path"] = _pointer(components)
    rewritten["value"] = value
    return rewritten


def _rewrite_upstream_value(transformer: RewriteUpstreamUrns, value: Any) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("dataset"), str):
        return value
    rewritten = copy.deepcopy(value)
    rewritten["dataset"] = transformer._rewrite_urn(value["dataset"])
    return rewritten
