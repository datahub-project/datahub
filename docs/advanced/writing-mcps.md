# Saving MCPs to a File

## What is an MCP?

A `MetadataChangeProposal` (MCP) represents an atomic unit of change in the DataHub Metadata Graph. Each MCP carries a single aspect in its payload and is used to propose changes to DataHub's metadata. 

- Represents a single aspect change
- Used for proposing metadata changes to DataHub
- Serves as the basic building block for metadata ingestion

For more information, please see guides on [DataHub Metadata Events](../what/mxe.md) and [MCPs](mcp-mcl.md). 

## Why Write MCPs as Files?

MCPs in JSON file format are particularly valuable because they represent the lowest and most granular form of events in DataHub. There are two main use cases for using previously saved MCP files:

### Testing

MCPs allow you to easily ingest metadata. You can:

- Use it for entity ingestion by running a simple command, without a dependency on a ingestion connector:
    
    ```bash
    datahub ingest mcps <file_name>.json
    ```
    
- Create reproducible test cases for metadata ingestion
- Write and run tests when contributing to DataHub (see DataHub Testing Guide for more details)

### Debugging

MCPs are valuable for debugging because they let you:

- Examine entities in your DataHub instance at a granular level
- Export existing entities to MCP files for analysis
- Verify entity structures and relationships before ingestion

For example, if you want to understand the structure of entities in your DataHub instance, you can emit them as MCP files and examine their contents in detail.

## Saving MCPs to a file

### Exporting from Ingestion Source

You can export MCPs from an ingestion source (such as BigQuery, Snowflake, etc.) to a file using the `file` sink type in your recipe. This approach is useful when you want to:

- Save MCPs for later ingestion
- Examine existing entities in the source
- Debug ingestion issues

To get started, create a recipe file (e.g., `export_mcps.yaml`) specifying your target source and the file `sink` type:

```yaml
source:
  type: bigquery # Replace with your source type
  config:
    ... # Add your source configuration here
sink:
  type: "file"
  config:
    filename: "mcps.json"
```

Run the ingestion with the following command:

```python
datahub ingest -c export_mcps.yaml
```

This command will extract all entities from your source and write them to `mcps.json` in MCP format. 

For more details about the `file` sink type, please refer to [Metadata File](../../metadata-ingestion/sink_docs/metadata-file.md)

### Exporting from DataHub Instance

You can also export MCPs directly from an existing DataHub instance using a similar recipe approach. This method is particularly useful when you need to:

- Examine entities already in your DataHub instance
- Create test cases based on real data
- Debug entity relationships

The process is similar to exporting from an ingestion source, with the only difference being that you'll use `datahub` as the source type.
Create a recipe file (e.g., `export_mcps.yaml`) with this configuration:


```yaml
source:
  type: datahub
  config:
    # Add your DataHub connection configuration here
    server: "http://localhost:8080"
    token: "your-access-token"  # If authentication is required

sink:
  type: "file"
  config:
    filename: "mcps.json"
```

Run the ingestion:

```python
datahub ingest -c export_mcps.yaml
```

This will extract all entities from your DataHub instance and save them to `mcps.json` in MCP format.

### Creating MCPs with Python SDK

You can use the `write_metadata_file` helper to generate MCPs programmatically:

```python
from datahub.ingestion.sink.file import write_metadata_file
from pathlib import Path
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper

records = [
    MetadataChangeProposalWrapper(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,example_dataset,PROD)",
        changeType="UPSERT",
        aspectName="datasetProperties",
        aspect=DatasetPropertiesClass(
            description="Example dataset description",
            customProperties={"encoding": "utf-8"}
        ))

]
write_metadata_file(
    file=Path("mcps.json"),
    records=records,
)
```

Edit `records` to create the event and entities for your needs.

Run the Python script to generate your defined MCPs and save them to a file:

```bash
python <file_name>.py
```

For example, the above script will generate an MCP file with a single dataset entity.

```json
[
{
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,example_dataset,PROD)",
    "changeType": "UPSERT",
    "aspectName": "datasetProperties",
    "aspect": {
        "json": {
            "customProperties": {
                "encoding": "utf-8"
            },
            "description": "Example dataset description",
            "tags": []
        }
    }
}
]
```