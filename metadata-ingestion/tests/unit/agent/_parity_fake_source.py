"""A two-level source with a probe, for the parity harness's own tests.

Groups hold items; group_pattern and item_pattern filter them. Reachable by
dotted path, so Pipeline, run_probe_method and check_filters resolve it with
no monkeypatching. `drift` is read by ingestion only, which is how a test makes
the two disagree on purpose.
"""

from typing import Annotated, Dict, Iterable, List, Optional, Sequence

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ancestors_in
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
GROUPS: Dict[str, List[str]] = {"g1": ["a", "b"], "g2": ["c"]}
# Ingestion-only behaviours; the probe never reads `drift`.
IGNORE_ITEM_PATTERN = "ignore_item_pattern"
DROP_ITEM_A = "drop_item_a"


class ItemsConfig(ConfigModel):
    group_pattern: Annotated[AllowDenyPattern, Filters(GROUP_KIND)] = Field(
        default=AllowDenyPattern.allow_all(), description="Groups to ingest."
    )
    item_pattern: Annotated[AllowDenyPattern, Filters(ITEM_KIND)] = Field(
        default=AllowDenyPattern.allow_all(), description="Items to ingest."
    )
    drift: Optional[str] = Field(default=None, description="Test-only drift.")
    password: Optional[str] = Field(
        default=None, description="A secret, so a test can collide one with a name."
    )

    @classmethod
    def probe_provider_class(cls) -> type:
        return ItemsProbe

    @classmethod
    def probe_ancestor_kinds(cls, kind: str) -> Optional[Sequence[str]]:
        return ancestors_in((GROUP_KIND,), kind, (ITEM_KIND,))


class ItemsProbe:
    @classmethod
    def for_config(cls, config: ItemsConfig) -> "ItemsProbe":
        return cls()

    def __enter__(self) -> "ItemsProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @probe_method(kind=GROUP_KIND, row_limit_param="limit")
    def groups(self, limit: int = 100) -> List[Dict[str, str]]:
        """Every group, including ones the recipe drops."""
        return [{"name": group} for group in GROUPS][:limit]

    @probe_method(kind=ITEM_KIND, parent_params=("group",))
    def items(self, group: str) -> List[Dict[str, str]]:
        """Every item in one group, including ones the recipe drops."""
        return [{"name": item} for item in GROUPS[group]]


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
