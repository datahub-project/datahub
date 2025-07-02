from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass, SchemaFieldClass, SchemaMetadataClass, OtherSchemaClass,
)


def make_dataset_urn_by_parts(
    platform: str, db: str, table: str, env: str = "PROD"
) -> str:
    name = f"{db}.{table}"
    return make_dataset_urn(platform=platform, name=name, env=env)


def make_dataset_snapshot_workunit(
    dataset_urn: str, name: str, description: Optional[str] = None
) -> MetadataWorkUnit:
    snapshot = DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[DatasetPropertiesClass(name=name, description=description or "")],
    )
    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    return MetadataWorkUnit(id=f"dataset-snapshot-{name}", mce=mce)

def make_schema_metadata_workunit( dataset_urn: str, fields:list[SchemaFieldClass]):
    platform_urn = dataset_urn.split(":(")[1].split(",")[0]  # e.g. urn:li:dataPlatform:tibero

    schema_metadata = SchemaMetadataClass(
        schemaName="default",
        platform=platform_urn,
        platformSchema=OtherSchemaClass(rawSchema=""),  # 생략 가능
        fields=fields,
        version=0,
        hash=""
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
    )
    return MetadataWorkUnit(id=f"dataset-metadata-{dataset_urn}", mcp=mcp)
