import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainsClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Batch assign multiple datasets to the same domain
domain_urn = make_domain_urn("marketing")

# List of datasets to assign
datasets = [
    "snowflake.marketing_db.campaigns",
    "snowflake.marketing_db.leads",
    "snowflake.marketing_db.opportunities",
]

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

for dataset_name in datasets:
    dataset_urn = make_dataset_urn(platform="snowflake", name=dataset_name, env="PROD")

    # Create the domains aspect with the target domain
    domains_aspect = DomainsClass(domains=[domain_urn])

    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=domains_aspect,
    )

    rest_emitter.emit(event)
    log.info(f"Assigned {dataset_urn} to domain {domain_urn}")

log.info(f"Successfully assigned {len(datasets)} datasets to domain {domain_urn}")
