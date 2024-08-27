"""
After running the recipe in this directory, we get a large JSON file called all_covid19_datasets.json.
This script reads that JSON file, adds to it using the directives pull from a Google sheet, and
produces a new JSON file called demo_data.json.
"""

import csv
import dataclasses
import pathlib
import time
from typing import Dict, List, cast

from datahub.ingestion.sink.file import write_metadata_file as write_mces
from datahub.ingestion.source.file import read_metadata_file
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpUserInfoClass,
    CorpUserSnapshotClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DEMO_DATA_DIR = pathlib.Path("./examples/demo_data")
INPUT_ALL_DATASETS = DEMO_DATA_DIR / "all_covid19_datasets.json"
OUTPUT_ENRICHED = DEMO_DATA_DIR / "demo_data.json"
DIRECTIVES_CSV = DEMO_DATA_DIR / "directives.csv"


@dataclasses.dataclass
class Directive:
    table: str
    drop: bool
    owners: List[str]
    depends_on: List[str]


def read_mces(path: pathlib.Path) -> List[MetadataChangeEventClass]:
    objs = read_metadata_file(path)
    assert all(isinstance(obj, MetadataChangeEventClass) for obj in objs)
    return cast(List[MetadataChangeEventClass], objs)


def parse_directive(row: Dict) -> Directive:
    return Directive(
        table=row["table"],
        drop=bool(row["drop"]),
        owners=[x.strip() for x in row["owners"].split(",") if x],
        depends_on=[x.strip() for x in row["depends_on"].split(",") if x],
    )


def fetch_directives() -> List[Directive]:
    with open(DIRECTIVES_CSV, "r") as f:
        reader = csv.DictReader(f)
        rows = [parse_directive(row) for row in reader]
        return rows


def dataset_name_to_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{name},PROD)"


def clean_owner_name(name: str) -> str:
    clean = "".join(c for c in name if c.isalpha())
    return clean


def owner_name_to_urn(name: str) -> str:
    return f"urn:li:corpuser:{name}"


def create_owner_entity_mce(owner: str) -> MetadataChangeEventClass:
    clean_name = clean_owner_name(owner)
    return MetadataChangeEventClass(
        proposedSnapshot=CorpUserSnapshotClass(
            urn=owner_name_to_urn(clean_name),
            aspects=[
                CorpUserInfoClass(
                    active=True,
                    displayName=owner,
                    fullName=owner,
                    email=f"{clean_name}-demo@example.com",
                )
            ],
        )
    )


def create_ownership_aspect_mce(directive: Directive) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name_to_urn(directive.table),
            aspects=[
                OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=owner_name_to_urn(clean_owner_name(owner)),
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                        for owner in directive.owners
                    ],
                    lastModified=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub",
                    ),
                )
            ],
        )
    )


def create_lineage_aspect_mce(directive: Directive) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name_to_urn(directive.table),
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=dataset_name_to_urn(upstream),
                            type=DatasetLineageTypeClass.TRANSFORMED,
                            auditStamp=AuditStampClass(
                                time=int(time.time() * 1000),
                                actor="urn:li:corpuser:datahub",
                            ),
                        )
                        for upstream in directive.depends_on
                    ]
                )
            ],
        )
    )


def create_global_tags_aspect_mce(directive: Directive) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name_to_urn(directive.table),
            aspects=[GlobalTagsClass(tags=[])],
        )
    )


def create_editable_schema_info_aspect_mce(
    directive: Directive,
) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=dataset_name_to_urn(directive.table),
            aspects=[
                EditableSchemaMetadataClass(
                    created=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub",
                    ),
                    lastModified=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub",
                    ),
                    editableSchemaFieldInfo=[],
                )
            ],
        )
    )


if __name__ == "__main__":
    datasets = read_mces(INPUT_ALL_DATASETS)
    all_directives = fetch_directives()
    directives = [directive for directive in all_directives if not directive.drop]

    all_dataset_urns = {
        dataset_name_to_urn(directive.table) for directive in all_directives
    }
    allowed_urns = {
        dataset_name_to_urn(directive.table)
        for directive in all_directives
        if not directive.drop
    }

    missing_dataset_directives = [
        dataset.proposedSnapshot.urn
        for dataset in datasets
        if dataset.proposedSnapshot.urn not in all_dataset_urns
    ]
    assert not missing_dataset_directives

    filtered_dataset_mces = [
        dataset for dataset in datasets if dataset.proposedSnapshot.urn in allowed_urns
    ]

    owner_names = {owner for directive in directives for owner in directive.owners}
    owner_entity_mces = [
        create_owner_entity_mce(owner) for owner in sorted(owner_names)
    ]

    ownership_aspect_mces = [
        create_ownership_aspect_mce(directive)
        for directive in directives
        if directive.owners
    ]

    lineage_aspect_mces = [
        create_lineage_aspect_mce(directive)
        for directive in directives
        if directive.depends_on
    ]

    global_tags_aspect_mces = [
        create_global_tags_aspect_mce(directive)
        for directive in directives
        if not directive.drop
    ]

    editable_schema_info_aspect_mces = [
        create_editable_schema_info_aspect_mce(directive)
        for directive in directives
        if not directive.drop
    ]

    enriched_mces = (
        filtered_dataset_mces
        + owner_entity_mces
        + ownership_aspect_mces
        + lineage_aspect_mces
        + global_tags_aspect_mces
        + editable_schema_info_aspect_mces
    )
    write_mces(OUTPUT_ENRICHED, enriched_mces)
