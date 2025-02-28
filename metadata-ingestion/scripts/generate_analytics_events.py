import json
import logging
from pathlib import Path

import click

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BrowsePathsClass,
    DatasetPropertiesClass,
    EnumTypeClass,
    GlobalTagsClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    BrowsePathsV2Class,
    BrowsePathEntryClass,
)

logger = logging.getLogger(__name__)

def is_field_doc(line: str):
    return line.startswith("/**") and line.endswith("*/")

def get_field_doc(line: str):
    return line.replace("/** ", "").replace("*/", "").replace("\n", "")

def is_field(line: str):
    return ":" in line and line.endswith(";")

def create_schema_field(line: str, field_description: str, is_base_field: bool):
    is_nullable = "?" in line
    line = line.split(";")[0]
    line = line.replace("?", "").replace(";", "").replace("\n", "").replace(" ", "").strip()
    [field, type] = line.split(":")

    schema_field_type = StringTypeClass()
    if type == "string":
        schema_field_type = StringTypeClass()
    elif type == "boolean":
        schema_field_type = BooleanTypeClass()
    elif type == "number":
        schema_field_type = NumberTypeClass()
    elif "Array" in type:
        schema_field_type = ArrayTypeClass()
    elif "[]" in type:
        schema_field_type = ArrayTypeClass()
    else:
        schema_field_type = EnumTypeClass()

    description = f"Base field (in all events): {field_description}" if is_base_field else field_description
    field = SchemaField(
        fieldPath=field,
        type=SchemaFieldDataTypeClass(type=schema_field_type),
        nativeDataType=type,
        nullable=is_nullable,
        isPartOfKey=False,
        description=description
    )

    return field

def get_event_name(line: str):
    return line.replace("export interface ", "").replace(" extends BaseEvent {", "").replace("\n", "")

def create_event_mcps(event_name: str, base_fields, fields, event_doc: str):
    dataset_urn = make_dataset_urn(
        platform="datahub",
        name=event_name,
    )
    return MetadataChangeProposalWrapper.construct_many(
        entityUrn=dataset_urn,
        aspects=[
            SchemaMetadataClass(
                schemaName=str(event_name),
                platform=make_data_platform_urn("datahub"),
                platformSchema=OtherSchemaClass(rawSchema="{}"),
                fields=(base_fields + fields),
                version=0,
                hash="",
                foreignKeys=None,
            ),
            GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:Entity"), TagAssociationClass(tag="urn:li:tag:AnalyticsEvent")]
            ),
            BrowsePathsClass([f"/prod/datahub/events/{event_name}"]),
            BrowsePathsV2Class(
                [
                    BrowsePathEntryClass(id="events"),
                    BrowsePathEntryClass(id=event_name),
                ]
            ),
            DatasetPropertiesClass(
                description=event_doc
            ),
            SubTypesClass(typeNames=["Analytics Event"]),
        ],
    )

@click.command()
@click.option("--events-file", type=click.Path(exists=True), required=True)
@click.option("--mces-file", type=str, required=True)
def generate(
    events_file: str,
    mces_file: str,
) -> None:
    """
    Scrapes the event.ts source of truth file for generating mixpanel events
    and generates enriched datasets for each event with schema fields so we
    can publish these events on a datahub server.
    Used in our github workflow to publish to acryl.acryl.io
    """
    with open(events_file) as typescript_file:
        mcps = []
        is_doc = False
        is_base_event = False
        event_doc = ""
        event_name = ""
        field_description = ""
        base_fields = []
        fields = []
        
        for line in typescript_file:
            line = line.strip().replace("\n", "")
            # handle scraping the base event to get all of the generic fields for every event
            if "interface BaseEvent {" in line:
                is_base_event = True
                continue
            if is_base_event:
                # end of BaseEvent
                if "}" in line:
                    is_base_event = False
                    continue
                if is_field_doc(line):
                    field_description = get_field_doc(line)
                    continue
                if is_field(line):
                    base_field = create_schema_field(line, field_description, True)
                    base_fields.append(base_field)
                    field_description = ""
                    continue
            # we're starting a doc for an event if we don't have 
            if event_name == "" and "/**" in line:
                is_doc = True
                continue
            if is_doc:
                event_doc = line.replace("* ", "").replace("\n", "")
                is_doc = False
                continue
            if "interface" in line and "extends BaseEvent {" in line:
                event_name = get_event_name(line)
                continue
            # finished getting info from an event
            if event_name != "" and "}" in line:
                mcps += create_event_mcps(event_name, base_fields, fields, event_doc)

                is_doc = False
                event_doc = ""
                event_name = ""
                fields = []
                field_description = ""
                continue

            if event_name != "":
                if is_field_doc(line):
                    field_description = get_field_doc(line)
                if is_field(line):
                    field = create_schema_field(line, field_description, False)
                    fields.append(field)
                    field_description = ""

        
        Path(mces_file).parent.mkdir(parents=True, exist_ok=True)
        fileSink = FileSink(
            PipelineContext(run_id="generated-events"),
            FileSinkConfig(filename=mces_file),
        )
        for e in mcps:
            fileSink.write_record_async(
                RecordEnvelope(e, metadata={}), write_callback=NoopWriteCallback()
            )
        fileSink.close()
        pipeline_config = {
            "source": {
                "type": "file",
                "config": {"filename": mces_file},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": "${DATAHUB_SERVER:-http://localhost:8080}",
                    "token": "${DATAHUB_TOKEN:-}",
                },
            },
            "run_id": "analytics-modeldoc-generated",
        }
        pipeline_file = Path(mces_file).parent.absolute() / "pipeline.yml"
        with open(pipeline_file, "w") as f:
            json.dump(pipeline_config, f, indent=2)
            logger.info(f"Wrote pipeline to {pipeline_file}")



if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()