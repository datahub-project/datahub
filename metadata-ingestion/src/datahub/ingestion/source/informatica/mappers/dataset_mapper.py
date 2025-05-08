from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
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
