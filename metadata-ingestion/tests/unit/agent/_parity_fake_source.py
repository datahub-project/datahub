"""A two-level source with a probe, for the parity harness's own tests.

Groups hold items; group_pattern and item_pattern filter them. Reachable by
dotted path, so Pipeline, run_probe_method and check_filters resolve it with
no monkeypatching. `drift` is read by ingestion only and `probe_drift` by the
probe only, which is how a test makes the two disagree on purpose. Item "a"
sits in both groups, so an identity that drops the parent collides.
"""

from typing import Annotated, Dict, Iterable, List, Optional, Sequence

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext, ancestors_in
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    StatusClass,
    SubTypesClass,
)

GROUP_KIND = "Group"
ITEM_KIND = "Item"
GROUPS: Dict[str, List[str]] = {"g1": ["a", "b"], "g2": ["a", "c"]}
# Ingestion-only behaviours; the probe never reads `drift`.
IGNORE_ITEM_PATTERN = "ignore_item_pattern"
DROP_ITEM_A = "drop_item_a"
# Probe-only behaviours; ingestion never reads `probe_drift`.
LIST_NOTHING = "list_nothing"
SOFT_DEGRADE = "soft_degrade"
# A warning the source gives on every normal run, which a listing can accept.
BENIGN_NOTE = "benign_note"
# Lists item "a" of each group a second time, marked archived, which the
# probe's own override drops: one listing record judged two ways.
ARCHIVED_DUPLICATE = "archived_duplicate"
GROUP_NOTE = "group descriptions are never listed"


def item_note(group: str) -> str:
    return f"archived items of {group} are never listed"


class ItemsConfig(ConfigModel):
    group_pattern: Annotated[AllowDenyPattern, Filters(GROUP_KIND)] = Field(
        default=AllowDenyPattern.allow_all(), description="Groups to ingest."
    )
    item_pattern: Annotated[AllowDenyPattern, Filters(ITEM_KIND)] = Field(
        default=AllowDenyPattern.allow_all(), description="Items to ingest."
    )
    drift: Optional[str] = Field(default=None, description="Test-only drift.")
    probe_drift: Optional[str] = Field(
        default=None, description="Test-only drift in the probe."
    )
    password: Optional[str] = Field(
        default=None, description="A secret, so a test can collide one with a name."
    )

    @classmethod
    def probe_provider_class(cls) -> type:
        return ItemsProbe

    @classmethod
    def probe_ancestor_kinds(cls, kind: str) -> Optional[Sequence[str]]:
        return ancestors_in((GROUP_KIND,), kind, (ITEM_KIND,))

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        if self.probe_drift == ARCHIVED_DUPLICATE and ctx.attributes.get("archived"):
            return Verdict.exclude("archived")
        return None


class ItemsProbe:
    def __init__(self, config: ItemsConfig) -> None:
        self.config = config
        # Read by run_probe_method: a degraded sub-fetch, not a failure.
        self.warnings: List[str] = []

    @classmethod
    def for_config(cls, config: ItemsConfig) -> "ItemsProbe":
        return cls(config)

    def __enter__(self) -> "ItemsProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(kind=GROUP_KIND, row_limit_param="limit")
    def groups(self, limit: int = 100) -> List[Dict[str, str]]:
        """Every group, including ones the recipe drops."""
        if self.config.probe_drift == BENIGN_NOTE:
            self.warnings.append(GROUP_NOTE)
        return [{"name": group} for group in GROUPS][:limit]

    @probe_method(kind=ITEM_KIND, parent_params=("group",))
    def items(self, group: str) -> List[Dict[str, str]]:
        """Every item in one group, including ones the recipe drops."""
        if self.config.probe_drift == LIST_NOTHING:
            return []
        if self.config.probe_drift == SOFT_DEGRADE:
            self.warnings.append(f"could not read every item of {group}")
        if self.config.probe_drift == BENIGN_NOTE:
            self.warnings.append(item_note(group))
        listed = [{"name": item} for item in GROUPS[group]]
        if self.config.probe_drift == ARCHIVED_DUPLICATE:
            listed.append({"name": "a", "archived": "true"})
        return listed


@config_class(ItemsConfig)
class ItemsSource(Source):
    def __init__(self, config: ItemsConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ItemsSource":
        return cls(ItemsConfig.model_validate(config_dict), ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for group, items in GROUPS.items():
            if not self.config.group_pattern.allowed(group):
                continue
            container = make_container_urn(group)
            yield MetadataChangeProposalWrapper(
                entityUrn=container, aspect=ContainerPropertiesClass(name=group)
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=container, aspect=SubTypesClass(typeNames=[GROUP_KIND])
            ).as_workunit()
            for item in items:
                if self.config.drift == DROP_ITEM_A and item == "a":
                    continue
                if (
                    self.config.drift != IGNORE_ITEM_PATTERN
                    and not self.config.item_pattern.allowed(item)
                ):
                    continue
                yield MetadataChangeProposalWrapper(
                    entityUrn=make_dataset_urn("fake", f"{group}.{item}"),
                    aspect=StatusClass(removed=False),
                ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report


SOURCE_TYPE = f"{__name__}.{ItemsSource.__name__}"
