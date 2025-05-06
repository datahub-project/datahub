import logging
from functools import partial
from typing import Any, Iterable, List, Optional, Union

import progressbar
from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import (
    DomainsClass,
    GlossaryTermAssociationClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


def apply_association_to_container(
    container_urn: str,
    association_urn: str,
    association_type: str,
    emit: bool = True,
    graph: Optional[DataHubGraph] = None,
) -> Optional[List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]]:
    """
    Common function to add either tags, terms, domains, or owners to child datasets (for now).

    Args:
        container_urn: The URN of the container
        association_urn: The URN of the tag, term, or user to apply
        association_type: One of 'tag', 'term', 'domain' or 'owner'
    """
    urns: List[str] = [container_urn]
    if not graph:
        graph = get_default_graph(ClientMode.INGESTION)
    logger.info(f"Using {graph}")
    urns.extend(
        graph.get_urns_by_filter(
            container=container_urn,
            batch_size=1000,
            entity_types=["dataset", "container"],
        )
    )

    all_patches: List[Any] = []
    for urn in urns:
        builder = DatasetPatchBuilder(urn)
        patches: List[Any] = []
        if association_type == "tag":
            patches = builder.add_tag(TagAssociationClass(association_urn)).build()
        elif association_type == "term":
            patches = builder.add_term(
                GlossaryTermAssociationClass(association_urn)
            ).build()
        elif association_type == "owner":
            patches = builder.add_owner(
                OwnerClass(
                    owner=association_urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ).build()
        elif association_type == "domain":
            patches = [
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=DomainsClass(domains=[association_urn]),
                )
            ]
        all_patches.extend(patches)
    if emit:
        mcps_iter = progressbar.progressbar(all_patches, redirect_stdout=True)
        for mcp in mcps_iter:
            graph.emit(mcp)
        return None
    else:
        return all_patches


class DomainApplyConfig(ConfigModel):
    assets: List[str] = Field(
        default_factory=list,
        description="List of assets to apply domain hierarchichaly. Currently only containers and datasets are supported",
    )
    domain_urn: str = Field(default="")


class TagApplyConfig(ConfigModel):
    assets: List[str] = Field(
        default_factory=list,
        description="List of assets to apply tag hierarchichaly. Currently only containers and datasets are supported",
    )
    tag_urn: str = Field(default="")


class TermApplyConfig(ConfigModel):
    assets: List[str] = Field(
        default_factory=list,
        description="List of assets to apply term hierarchichaly. Currently only containers and datasets are supported",
    )
    term_urn: str = Field(default="")


class OwnerApplyConfig(ConfigModel):
    assets: List[str] = Field(
        default_factory=list,
        description="List of assets to apply owner hierarchichaly. Currently only containers and datasets are supported",
    )
    owner_urn: str = Field(default="")


class DataHubApplyConfig(ConfigModel):
    domain_apply: Optional[List[DomainApplyConfig]] = Field(
        default=None,
        description="List to apply domains to assets",
    )
    tag_apply: Optional[List[TagApplyConfig]] = Field(
        default=None,
        description="List to apply tags to assets",
    )
    term_apply: Optional[List[TermApplyConfig]] = Field(
        default=None,
        description="List to apply terms to assets",
    )
    owner_apply: Optional[List[OwnerApplyConfig]] = Field(
        default=None,
        description="List to apply owners to assets",
    )


@platform_name("DataHubApply")
@config_class(DataHubApplyConfig)
@support_status(SupportStatus.TESTING)
class DataHubApplySource(Source):
    """
    This source is a helper over CLI
    so people can use the helper to apply various metadata changes to DataHub
    via Managed Ingestion
    """

    def __init__(self, ctx: PipelineContext, config: DataHubApplyConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()
        self.graph = ctx.require_graph()

    def _yield_workunits(
        self,
        proposals: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ],
    ) -> Iterable[MetadataWorkUnit]:
        for proposal in proposals:
            if isinstance(proposal, MetadataChangeProposalWrapper):
                yield proposal.as_workunit()
            else:
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(proposal),
                    mcp_raw=proposal,
                )

    def _handle_assets(
        self, assets: List[str], apply_urn: str, apply_type: str
    ) -> Iterable[MetadataWorkUnit]:
        for asset in assets:
            change_proposals = apply_association_to_container(
                asset, apply_urn, apply_type, emit=False, graph=self.graph
            )
            assert change_proposals is not None
            yield from self._yield_workunits(change_proposals)

    def _yield_domain(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.domain_apply:
            return
        for apply in self.config.domain_apply:
            yield from self._handle_assets(apply.assets, apply.domain_urn, "domain")

    def _yield_tag(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.tag_apply:
            return
        for apply in self.config.tag_apply:
            yield from self._handle_assets(apply.assets, apply.tag_urn, "tag")

    def _yield_term(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.term_apply:
            return
        for apply in self.config.term_apply:
            yield from self._handle_assets(apply.assets, apply.term_urn, "term")

    def _yield_owner(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.owner_apply:
            return
        for apply in self.config.owner_apply:
            yield from self._handle_assets(apply.assets, apply.owner_urn, "owner")

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._yield_domain()
        yield from self._yield_tag()
        yield from self._yield_term()
        yield from self._yield_owner()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [partial(auto_workunit_reporter, self.get_report())]

    def get_report(self) -> SourceReport:
        return self.report
