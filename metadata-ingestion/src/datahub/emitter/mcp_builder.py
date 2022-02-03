from typing import Iterable, List, Union

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.domain import DomainProperties
from datahub.metadata.schema_classes import ChangeTypeClass, DomainsClass


def add_domain_to_entity_wu(
    entity_type:str, entity_urn: str, domain_urn: str, report: SourceReport
) -> Iterable[Union[MetadataWorkUnit]]:
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{entity_urn}",
        aspectName="domains",
        aspect=DomainsClass(domains=[domain_urn]),
    )
    wu = MetadataWorkUnit(id=f"{domain_urn}-to-{entity_urn}", mcp=mcp)
    report.report_workunit(wu)
    yield wu
