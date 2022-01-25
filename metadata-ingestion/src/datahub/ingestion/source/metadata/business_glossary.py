import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import validator

import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigModel
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mce_builder import get_sys_time, make_group_urn, make_user_urn
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit

logger = logging.getLogger(__name__)


valid_status: models.StatusClass = models.StatusClass(removed=False)
auditStamp = models.AuditStampClass(
    time=get_sys_time(), actor="urn:li:corpUser:restEmitter"
)


class Owners(ConfigModel):
    users: Optional[List[str]]
    groups: Optional[List[str]]


class GlossaryTermConfig(ConfigModel):
    name: str
    description: str
    term_source: Optional[str]
    source_ref: Optional[str]
    source_url: Optional[str]
    owners: Optional[Owners]
    inherits: Optional[List[str]]
    contains: Optional[List[str]]
    custom_properties: Optional[Dict[str, str]]


class GlossaryNodeConfig(ConfigModel):
    name: str
    description: str
    owners: Optional[Owners]
    terms: Optional[List[GlossaryTermConfig]]
    nodes: Optional[List["GlossaryNodeConfig"]]


GlossaryNodeConfig.update_forward_refs()


class DefaultConfig(ConfigModel):
    """Holds defaults for populating fields in glossary terms"""

    source: str
    owners: Owners
    url: Optional[str] = None
    source_type: Optional[str] = "INTERNAL"


class BusinessGlossarySourceConfig(ConfigModel):
    file: str


class BusinessGlossaryConfig(DefaultConfig):
    version: str
    nodes: Optional[List[GlossaryNodeConfig]]
    terms: Optional[List[GlossaryTermConfig]]

    @validator("version")
    def version_must_be_1(cls, v):
        if v != "1":
            raise ValueError("Only version 1 is supported")


def make_glossary_node_urn(path: List[str]) -> str:
    return "urn:li:glossaryNode:" + ".".join(path)


def make_glossary_term_urn(path: List[str]) -> str:
    return "urn:li:glossaryTerm:" + ".".join(path)


def get_owners(owners: Owners) -> models.OwnershipClass:
    owners_meta: List[models.OwnerClass] = []
    if owners.users is not None:
        owners_meta = owners_meta + [
            models.OwnerClass(
                owner=make_user_urn(o),
                type=models.OwnershipTypeClass.DEVELOPER,
            )
            for o in owners.users
        ]
    if owners.groups is not None:
        owners_meta = owners_meta + [
            models.OwnerClass(
                owner=make_group_urn(o),
                type=models.OwnershipTypeClass.DEVELOPER,
            )
            for o in owners.groups
        ]
    return models.OwnershipClass(owners=owners_meta)


def get_mces(
    glossary: BusinessGlossaryConfig,
) -> List[models.MetadataChangeEventClass]:
    events: List[models.MetadataChangeEventClass] = []
    path: List[str] = []
    root_owners = get_owners(glossary.owners)

    if glossary.nodes:
        for node in glossary.nodes:
            events += get_mces_from_node(
                node,
                path + [node.name],
                parentNode=None,
                parentOwners=root_owners,
                defaults=glossary,
            )

    if glossary.terms:
        for term in glossary.terms:
            events += get_mces_from_term(
                term,
                path + [term.name],
                parentNode=None,
                parentOwnership=root_owners,
                defaults=glossary,
            )

    return events


def get_mce_from_snapshot(snapshot: Any) -> models.MetadataChangeEventClass:
    return models.MetadataChangeEventClass(proposedSnapshot=snapshot)


def get_mces_from_node(
    glossaryNode: GlossaryNodeConfig,
    path: List[str],
    parentNode: Optional[str],
    parentOwners: models.OwnershipClass,
    defaults: DefaultConfig,
) -> List[models.MetadataChangeEventClass]:
    node_urn = make_glossary_node_urn(path)
    node_info = models.GlossaryNodeInfoClass(
        definition=glossaryNode.description,
        parentNode=parentNode,
    )
    node_owners = parentOwners
    if glossaryNode.owners is not None:
        assert glossaryNode.owners is not None
        node_owners = get_owners(glossaryNode.owners)

    node_snapshot = models.GlossaryNodeSnapshotClass(
        urn=node_urn,
        aspects=[node_info, node_owners, valid_status],
    )
    mces = [get_mce_from_snapshot(node_snapshot)]
    if glossaryNode.nodes:
        for node in glossaryNode.nodes:
            mces += get_mces_from_node(
                node,
                path + [node.name],
                parentNode=node_urn,
                parentOwners=node_owners,
                defaults=defaults,
            )

    if glossaryNode.terms:
        for term in glossaryNode.terms:
            mces += get_mces_from_term(
                term,
                path + [term.name],
                parentNode=node_urn,
                parentOwnership=node_owners,
                defaults=defaults,
            )
    return mces


def get_mces_from_term(
    glossaryTerm: GlossaryTermConfig,
    path: List[str],
    parentNode: Optional[str],
    parentOwnership: models.OwnershipClass,
    defaults: DefaultConfig,
) -> List[models.MetadataChangeEventClass]:
    term_urn = make_glossary_term_urn(path)
    aspects: List[
        Union[
            models.GlossaryTermInfoClass,
            models.GlossaryRelatedTermsClass,
            models.OwnershipClass,
            models.StatusClass,
            models.GlossaryTermKeyClass,
            models.BrowsePathsClass,
        ]
    ] = []
    term_info = models.GlossaryTermInfoClass(
        definition=glossaryTerm.description,
        termSource=glossaryTerm.term_source  # type: ignore
        if glossaryTerm.term_source is not None
        else defaults.source_type,
        sourceRef=glossaryTerm.source_ref
        if glossaryTerm.source_ref
        else defaults.source,
        sourceUrl=glossaryTerm.source_url if glossaryTerm.source_url else defaults.url,
        parentNode=parentNode,
        customProperties=glossaryTerm.custom_properties,
    )
    aspects.append(term_info)

    isA_related = None
    hasA_related = None
    if glossaryTerm.inherits is not None:
        assert glossaryTerm.inherits is not None
        isA_related = [make_glossary_term_urn([term]) for term in glossaryTerm.inherits]
    if glossaryTerm.contains is not None:
        assert glossaryTerm.contains is not None
        hasA_related = [
            make_glossary_term_urn([term]) for term in glossaryTerm.contains
        ]

    if isA_related is not None or hasA_related is not None:
        relatedTerms = models.GlossaryRelatedTermsClass(
            isRelatedTerms=isA_related, hasRelatedTerms=hasA_related
        )
        aspects.append(relatedTerms)

    ownership: models.OwnershipClass = parentOwnership
    if glossaryTerm.owners is not None:
        assert glossaryTerm.owners is not None
        ownership = get_owners(glossaryTerm.owners)
    aspects.append(ownership)

    term_browse = models.BrowsePathsClass(paths=["/" + "/".join(path)])
    aspects.append(term_browse)

    term_snapshot: models.GlossaryTermSnapshotClass = models.GlossaryTermSnapshotClass(
        urn=term_urn,
        aspects=aspects,
    )
    return [get_mce_from_snapshot(term_snapshot)]


@dataclass
class BusinessGlossaryFileSource(Source):
    config: BusinessGlossarySourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = BusinessGlossarySourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def load_glossary_config(self, file_name: str) -> BusinessGlossaryConfig:
        config = load_config_file(file_name)
        glossary_cfg = BusinessGlossaryConfig.parse_obj(config)
        return glossary_cfg

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        glossary_config = self.load_glossary_config(self.config.file)
        for mce in get_mces(glossary_config):
            wu = MetadataWorkUnit(f"{mce.proposedSnapshot.urn}", mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
