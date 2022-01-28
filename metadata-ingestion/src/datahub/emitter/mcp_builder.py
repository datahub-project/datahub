from typing import Iterable, List, Union

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.domain import DomainProperties
from datahub.metadata.schema_classes import ChangeTypeClass, DomainsClass


def add_domain_wu(
    domain: str, report: SourceReport
) -> Iterable[Union[MetadataWorkUnit]]:
    domain_urn = make_domain_urn(domain)
    mcp = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{domain_urn}",
        aspectName="domainProperties",
        aspect=DomainProperties(name=domain),
    )
    wu = MetadataWorkUnit(id=f"domain-{domain_urn}", mcp=mcp)
    report.report_workunit(wu)
    yield wu


def add_domains_to_dataset_wu(
    dataset_urn: str, domains: List[str], report: SourceReport
) -> Iterable[Union[MetadataWorkUnit]]:
    mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{dataset_urn}",
        aspectName="domains",
        aspect=DomainsClass(domains=domains),
    )
    wu = MetadataWorkUnit(id=f"domains-to-{dataset_urn}", mcp=mcp)
    report.report_workunit(wu)
    yield wu
