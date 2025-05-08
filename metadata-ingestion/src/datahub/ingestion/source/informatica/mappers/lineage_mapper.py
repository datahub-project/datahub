from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


def make_dataset_lineage_mcp(
    inlets: list[str], to_urn: str, job_urn: str
) -> MetadataWorkUnit:
    mcp = MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInputOutputClass(
            inputDatasets=inlets, outputDatasets=[to_urn], inputDatajobs=[job_urn]
        ),
    )
    return MetadataWorkUnit(id=f"lineage-{job_urn}->{to_urn}", mcp=mcp)


def make_synonym_lineage_mcp(
    synonym_name: str, target_table: str, platform: str = "oracle", env: str = "PROD"
) -> MetadataWorkUnit:
    """
    synonym_name: 시놈 이름 (e.g., 'SALES_ORDERS')
    target_table: 타겟 테이블 이름 (e.g., 'DW.F_ORDERS')
    """
    input_urn = make_dataset_urn(platform=platform, name=synonym_name, env=env)
    output_urn = make_dataset_urn(platform=platform, name=target_table, env=env)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=output_urn,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=input_urn, type=DatasetLineageTypeClass.TRANSFORMED
                )
            ]
        ),
    )
    return MetadataWorkUnit(id=f"lineage-{input_urn}->{output_urn}", mcp=mcp)
