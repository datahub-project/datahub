import logging
import pathlib
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, TypeVar, Union

from pydantic import validator
from pydantic.fields import Field

import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigModel
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_group_urn,
    make_user_urn,
    validate_ownership_type,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.urn_encoder import UrnEncoder

logger = logging.getLogger(__name__)

GlossaryNodeInterface = TypeVar(
    "GlossaryNodeInterface", "GlossaryNodeConfig", "BusinessGlossaryConfig"
)


class Owners(ConfigModel):
    type: str = models.OwnershipTypeClass.DEVELOPER
    typeUrn: Optional[str] = None
    users: Optional[List[str]] = None
    groups: Optional[List[str]] = None


OwnersMultipleTypes = Union[List[Owners], Owners]


class KnowledgeCard(ConfigModel):
    url: Optional[str] = None
    label: Optional[str] = None


class GlossaryTermConfig(ConfigModel):
    id: Optional[str] = None
    name: str
    description: str
    term_source: Optional[str] = None
    source_ref: Optional[str] = None
    source_url: Optional[str] = None
    owners: Optional[OwnersMultipleTypes] = None
    inherits: Optional[List[str]] = None
    contains: Optional[List[str]] = None
    values: Optional[List[str]] = None
    related_terms: Optional[List[str]] = None
    custom_properties: Optional[Dict[str, str]] = None
    knowledge_links: Optional[List[KnowledgeCard]] = None
    domain: Optional[str] = None

    # Private fields.
    _urn: str


class GlossaryNodeConfig(ConfigModel):
    id: Optional[str] = None
    name: str
    description: str
    owners: Optional[OwnersMultipleTypes] = None
    terms: Optional[List["GlossaryTermConfig"]] = None
    nodes: Optional[List["GlossaryNodeConfig"]] = None
    knowledge_links: Optional[List[KnowledgeCard]] = None
    custom_properties: Optional[Dict[str, str]] = None

    # Private fields.
    _urn: str


class DefaultConfig(ConfigModel):
    """Holds defaults for populating fields in glossary terms"""

    source: Optional[str] = None
    owners: OwnersMultipleTypes
    url: Optional[str] = None
    source_type: str = "INTERNAL"


class BusinessGlossarySourceConfig(ConfigModel):
    file: Union[str, pathlib.Path] = Field(
        description="File path or URL to business glossary file to ingest."
    )
    enable_auto_id: bool = Field(
        description="Generate guid urns instead of a plaintext path urn with the node/term's hierarchy.",
        default=False,
    )


class BusinessGlossaryConfig(DefaultConfig):
    version: str
    terms: Optional[List["GlossaryTermConfig"]] = None
    nodes: Optional[List["GlossaryNodeConfig"]] = None

    @validator("version")
    def version_must_be_1(cls, v):
        if v != "1":
            raise ValueError("Only version 1 is supported")
        return v


def clean_url(text: str) -> str:
    """
    Clean text for use in URLs by:
    1. Replacing spaces with hyphens
    2. Removing special characters (preserving hyphens and periods)
    3. Collapsing multiple hyphens and periods into single ones
    """
    # Replace spaces with hyphens
    text = text.replace(" ", "-")
    # Remove special characters except hyphens and periods
    text = re.sub(r"[^a-zA-Z0-9\-.]", "", text)
    # Collapse multiple hyphens into one
    text = re.sub(r"-+", "-", text)
    # Collapse multiple periods into one
    text = re.sub(r"\.+", ".", text)
    # Remove leading/trailing hyphens and periods
    text = text.strip("-.")
    return text


def create_id(path: List[str], default_id: Optional[str], enable_auto_id: bool) -> str:
    """
    Create an ID for a glossary node or term.

    Args:
        path: List of path components leading to this node/term
        default_id: Optional manually specified ID
        enable_auto_id: Whether to generate GUIDs
    """
    if default_id is not None:
        return default_id  # Use explicitly provided ID

    id_: str = ".".join(path)

    # Check for non-ASCII characters before cleaning
    if any(ord(c) > 127 for c in id_):
        return datahub_guid({"path": id_})

    if enable_auto_id:
        # Generate GUID for auto_id mode
        id_ = datahub_guid({"path": id_})
    else:
        # Clean the URL for better readability when not using auto_id
        id_ = clean_url(id_)

        # Force auto_id if the cleaned URL still contains problematic characters
        if UrnEncoder.contains_extended_reserved_char(id_):
            logger.warning(
                f"ID '{id_}' contains problematic characters after URL cleaning. Falling back to GUID generation for stability."
            )
            id_ = datahub_guid({"path": id_})

    return id_


def make_glossary_node_urn(
    path: List[str], default_id: Optional[str], enable_auto_id: bool
) -> str:
    if default_id is not None and default_id.startswith("urn:li:glossaryNode:"):
        logger.debug(
            f"node's default_id({default_id}) is in urn format for path {path}. Returning same as urn"
        )
        return default_id

    return "urn:li:glossaryNode:" + create_id(path, default_id, enable_auto_id)


def make_glossary_term_urn(
    path: List[str], default_id: Optional[str], enable_auto_id: bool
) -> str:
    if default_id is not None and default_id.startswith("urn:li:glossaryTerm:"):
        logger.debug(
            f"term's default_id({default_id}) is in urn format for path {path}. Returning same as urn"
        )
        return default_id

    return "urn:li:glossaryTerm:" + create_id(path, default_id, enable_auto_id)


def get_owners_multiple_types(owners: OwnersMultipleTypes) -> models.OwnershipClass:
    """Allows owner types to be a list and maintains backward compatibility"""
    if isinstance(owners, Owners):
        return models.OwnershipClass(owners=list(get_owners(owners)))

    owners_meta: List[models.OwnerClass] = []
    for owner in owners:
        owners_meta.extend(get_owners(owner))

    return models.OwnershipClass(owners=owners_meta)


def get_owners(owners: Owners) -> Iterable[models.OwnerClass]:
    actual_type = owners.type or models.OwnershipTypeClass.DEVELOPER

    if actual_type.startswith("urn:li:ownershipType:"):
        ownership_type: str = "CUSTOM"
        ownership_type_urn: Optional[str] = actual_type
    else:
        ownership_type, ownership_type_urn = validate_ownership_type(actual_type)

    if owners.typeUrn is not None:
        ownership_type_urn = owners.typeUrn

    if owners.users is not None:
        for o in owners.users:
            yield models.OwnerClass(
                owner=make_user_urn(o),
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )
    if owners.groups is not None:
        for o in owners.groups:
            yield models.OwnerClass(
                owner=make_group_urn(o),
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )


def get_mces(
    glossary: BusinessGlossaryConfig,
    path_vs_id: Dict[str, str],
    ingestion_config: BusinessGlossarySourceConfig,
    ctx: PipelineContext,
) -> Iterable[Union[MetadataChangeProposalWrapper, models.MetadataChangeEventClass]]:
    root_owners = get_owners_multiple_types(glossary.owners)

    if glossary.nodes:
        for node in glossary.nodes:
            yield from get_mces_from_node(
                node,
                path_vs_id=path_vs_id,
                parentNode=None,
                parentOwners=root_owners,
                defaults=glossary,
                ingestion_config=ingestion_config,
                ctx=ctx,
            )

    if glossary.terms:
        for term in glossary.terms:
            yield from get_mces_from_term(
                term,
                path_vs_id=path_vs_id,
                parentNode=None,
                parentOwnership=root_owners,
                defaults=glossary,
                ingestion_config=ingestion_config,
                ctx=ctx,
            )


def get_mce_from_snapshot(snapshot: Any) -> models.MetadataChangeEventClass:
    return models.MetadataChangeEventClass(proposedSnapshot=snapshot)


def make_institutional_memory_mcp(
    urn: str, knowledge_cards: List[KnowledgeCard]
) -> Optional[MetadataChangeProposalWrapper]:
    elements: List[models.InstitutionalMemoryMetadataClass] = []

    for knowledge_card in knowledge_cards:
        if knowledge_card.label and knowledge_card.url:
            elements.append(
                models.InstitutionalMemoryMetadataClass(
                    url=knowledge_card.url,
                    description=knowledge_card.label,
                    createStamp=models.AuditStampClass(
                        time=int(time.time() * 1000.0),
                        actor="urn:li:corpuser:datahub",
                        message="ingestion bot",
                    ),
                )
            )

    if elements:
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=models.InstitutionalMemoryClass(elements=elements),
        )

    return None


def make_domain_mcp(
    term_urn: str, domain_aspect: models.DomainsClass
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=domain_aspect)


def get_mces_from_node(
    glossaryNode: GlossaryNodeConfig,
    path_vs_id: Dict[str, str],
    parentNode: Optional[str],
    parentOwners: models.OwnershipClass,
    defaults: DefaultConfig,
    ingestion_config: BusinessGlossarySourceConfig,
    ctx: PipelineContext,
) -> Iterable[Union[MetadataChangeProposalWrapper, models.MetadataChangeEventClass]]:
    node_urn = glossaryNode._urn

    node_info = models.GlossaryNodeInfoClass(
        definition=glossaryNode.description,
        parentNode=parentNode,
        name=glossaryNode.name,
        customProperties=glossaryNode.custom_properties,
    )
    node_owners = parentOwners
    if glossaryNode.owners is not None:
        assert glossaryNode.owners is not None
        node_owners = get_owners_multiple_types(glossaryNode.owners)

    node_snapshot = models.GlossaryNodeSnapshotClass(
        urn=node_urn,
        aspects=[node_info, node_owners],
    )
    yield get_mce_from_snapshot(node_snapshot)

    if glossaryNode.knowledge_links is not None:
        mcp: Optional[MetadataChangeProposalWrapper] = make_institutional_memory_mcp(
            node_urn, glossaryNode.knowledge_links
        )
        if mcp is not None:
            yield mcp

    if glossaryNode.nodes:
        for node in glossaryNode.nodes:
            yield from get_mces_from_node(
                node,
                path_vs_id=path_vs_id,
                parentNode=node_urn,
                parentOwners=node_owners,
                defaults=defaults,
                ingestion_config=ingestion_config,
                ctx=ctx,
            )

    if glossaryNode.terms:
        for term in glossaryNode.terms:
            yield from get_mces_from_term(
                glossaryTerm=term,
                path_vs_id=path_vs_id,
                parentNode=node_urn,
                parentOwnership=node_owners,
                defaults=defaults,
                ingestion_config=ingestion_config,
                ctx=ctx,
            )


def get_domain_class(
    graph: Optional[DataHubGraph], domains: List[str]
) -> models.DomainsClass:
    # FIXME: In the ideal case, the domain registry would be an instance variable so that it
    # preserves its cache across calls to this function. However, the current implementation
    # requires the full list of domains to be passed in at instantiation time, so we can't
    # actually do that.
    domain_registry: DomainRegistry = DomainRegistry(
        cached_domains=[k for k in domains], graph=graph
    )
    domain_class = models.DomainsClass(
        domains=[domain_registry.get_domain_urn(domain) for domain in domains]
    )
    return domain_class


def get_mces_from_term(
    glossaryTerm: GlossaryTermConfig,
    path_vs_id: Dict[str, str],
    parentNode: Optional[str],
    parentOwnership: models.OwnershipClass,
    defaults: DefaultConfig,
    ingestion_config: BusinessGlossarySourceConfig,
    ctx: PipelineContext,
) -> Iterable[Union[models.MetadataChangeEventClass, MetadataChangeProposalWrapper]]:
    term_urn = glossaryTerm._urn

    aspects: List[
        Union[
            models.GlossaryTermInfoClass,
            models.GlossaryRelatedTermsClass,
            models.OwnershipClass,
            models.GlossaryTermKeyClass,
            models.StatusClass,
            models.BrowsePathsClass,
        ]
    ] = []
    term_info = models.GlossaryTermInfoClass(
        definition=glossaryTerm.description,
        termSource=(
            glossaryTerm.term_source
            if glossaryTerm.term_source is not None
            else defaults.source_type
        ),
        sourceRef=(
            glossaryTerm.source_ref if glossaryTerm.source_ref else defaults.source
        ),
        sourceUrl=glossaryTerm.source_url if glossaryTerm.source_url else defaults.url,
        parentNode=parentNode,
        customProperties=glossaryTerm.custom_properties,
        name=glossaryTerm.name,
    )
    aspects.append(term_info)

    is_a = None
    has_a = None
    values: Union[None, List[str]] = None
    related_terms: Union[None, List[str]] = None
    if glossaryTerm.inherits is not None:
        assert glossaryTerm.inherits is not None
        is_a = [
            make_glossary_term_urn(
                [term],
                default_id=path_vs_id.get(term),
                enable_auto_id=ingestion_config.enable_auto_id,
            )
            for term in glossaryTerm.inherits
        ]
    if glossaryTerm.contains is not None:
        assert glossaryTerm.contains is not None
        has_a = [
            make_glossary_term_urn(
                [term],
                default_id=path_vs_id.get(term),
                enable_auto_id=ingestion_config.enable_auto_id,
            )
            for term in glossaryTerm.contains
        ]
    if glossaryTerm.values is not None:
        assert glossaryTerm.values is not None
        values = [
            make_glossary_term_urn(
                [term],
                default_id=path_vs_id.get(term),
                enable_auto_id=ingestion_config.enable_auto_id,
            )
            for term in glossaryTerm.values
        ]
    if glossaryTerm.related_terms is not None:
        assert glossaryTerm.related_terms is not None
        related_terms = [
            make_glossary_term_urn(
                [term],
                default_id=path_vs_id.get(term),
                enable_auto_id=ingestion_config.enable_auto_id,
            )
            for term in glossaryTerm.related_terms
        ]

    if (
        is_a is not None
        or has_a is not None
        or values is not None
        or related_terms is not None
    ):
        related_term_aspect = models.GlossaryRelatedTermsClass(
            isRelatedTerms=is_a,
            hasRelatedTerms=has_a,
            values=values,
            relatedTerms=related_terms,
        )
        aspects.append(related_term_aspect)

    ownership: models.OwnershipClass = parentOwnership
    if glossaryTerm.owners is not None:
        assert glossaryTerm.owners is not None
        ownership = get_owners_multiple_types(glossaryTerm.owners)
    aspects.append(ownership)

    if glossaryTerm.domain is not None:
        yield make_domain_mcp(
            term_urn, get_domain_class(ctx.graph, [glossaryTerm.domain])
        )

    term_snapshot: models.GlossaryTermSnapshotClass = models.GlossaryTermSnapshotClass(
        urn=term_urn,
        aspects=aspects,
    )
    yield get_mce_from_snapshot(term_snapshot)

    if glossaryTerm.knowledge_links:
        mcp: Optional[MetadataChangeProposalWrapper] = make_institutional_memory_mcp(
            term_urn, glossaryTerm.knowledge_links
        )
        if mcp is not None:
            yield mcp


def materialize_all_node_urns(
    glossary: BusinessGlossaryConfig, enable_auto_id: bool
) -> None:
    """After this runs, all nodes will have an id value that is a valid urn."""

    def _process_child_terms(
        parent_node: GlossaryNodeInterface, path: List[str]
    ) -> None:
        for term in parent_node.terms or []:
            term._urn = make_glossary_term_urn(
                path + [term.name], term.id, enable_auto_id
            )

        for node in parent_node.nodes or []:
            node._urn = make_glossary_node_urn(
                path + [node.name], node.id, enable_auto_id
            )
            _process_child_terms(node, path + [node.name])

    _process_child_terms(glossary, [])


def populate_path_vs_id(glossary: BusinessGlossaryConfig) -> Dict[str, str]:
    # This needed to map paths present in inherits, contains, values, and related_terms to term's
    # urn, if one was manually specified.
    path_vs_id: Dict[str, str] = {}

    def _process_child_terms(
        parent_node: GlossaryNodeInterface, path: List[str]
    ) -> None:
        for term in parent_node.terms or []:
            path_vs_id[".".join(path + [term.name])] = term._urn

        for node in parent_node.nodes or []:
            path_vs_id[".".join(path + [node.name])] = node._urn
            _process_child_terms(node, path + [node.name])

    _process_child_terms(glossary, [])

    return path_vs_id


@platform_name("Business Glossary")
@config_class(BusinessGlossarySourceConfig)
@support_status(SupportStatus.CERTIFIED)
@dataclass
class BusinessGlossaryFileSource(Source):
    """
    This plugin pulls business glossary metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/business_glossary.yml).
    """

    config: BusinessGlossarySourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = BusinessGlossarySourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @classmethod
    def load_glossary_config(
        cls, file_name: Union[str, pathlib.Path]
    ) -> BusinessGlossaryConfig:
        config = load_config_file(file_name, resolve_env_vars=True)
        glossary_cfg = BusinessGlossaryConfig.parse_obj(config)
        return glossary_cfg

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        glossary_config = self.load_glossary_config(self.config.file)

        materialize_all_node_urns(glossary_config, self.config.enable_auto_id)
        path_vs_id = populate_path_vs_id(glossary_config)

        yield from auto_workunit(
            get_mces(
                glossary_config, path_vs_id, ingestion_config=self.config, ctx=self.ctx
            )
        )

    def get_report(self):
        return self.report
