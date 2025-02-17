# Writing MCPs as Files

## What is an MCP?

A `MetadataChangeProposal` (MCP) represents an atomic unit of change in the DataHub Metadata Graph. Each MCP carries a single aspect in its payload and is used to propose changes to DataHub's metadata. 

- Represents a single aspect change
- Used for proposing metadata changes to DataHub
- Serves as the basic building block for metadata ingestion

For more information, please see [Guide in MCPs](https://datahubproject.io/docs/advanced/mcp-mcl/)

### Basic MCP Structure

Here's the essential structure of an MCP in JSON format:

```json
{
  "entityType": string,          // Type of entity (e.g., "dataset", "chart")
  "entityUrn": string,          // URN of the entity being updated
  "changeType": string,         // Type of change (UPSERT, CREATE, DELETE, etc.)
  "aspectName": string,         // Name of the aspect being modified
  "aspect": {                   // The aspect content
    "value": bytes,            // Serialized aspect value
    "contentType": string      // Serialization format (usually "application/json")
  }
}

```

### Example MCP File

Here's a practical example of an MCP for a dataset:

```json
{
  "entityType": "dataset",
  "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,example_dataset,PROD)",
  "changeType": "UPSERT",
  "aspectName": "datasetProperties",
  "aspect": {
    "value": {
      "description": "Example dataset description",
      "customProperties": {
        "encoding": "utf-8"
      }
    },
    "contentType": "application/json"
  }
}

```

## Why Write MCPs as Files?

MCPs in JSON file format are particularly valuable because they represent the lowest and most granular form of events in DataHub. There are two main use cases for working with MCP files:

### Testing

MCPs allow you to test your metadata ingestion without setting up complex connector dependencies. You can:

- Test entity ingestion by running a simple command:
    
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

## How to write MCPs as files?

### Exporting rom DataHub Instance

You can export MCPs directly from your DataHub instance using a recipe file. This is useful when you want to:

- Examine existing entities in your DataHub instance
- Create test cases based on real data
- Debug entity relationships

First, create a recipe file (e.g., `export_mcps.yaml`):

```python
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

This will write all the entities from your DataHub instance to `mcps.json` in MCP format.

### Creating MCPs with Python SDK

You can use the `FileEmitter` class to generate MCPs programmatically:

```python
import argparse
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
)

import time

from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig

class FileEmitter:
    def __init__(
        self, filename: str, run_id: str = f"test_{int(time.time() * 1000.0)}"
    ) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id=run_id),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()

def create_mcps() -> list[MetadataChangeProposalWrapper]:
    # Create dataset MCP
    mcps = [
		    MetadataChangeProposalWrapper(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,example_dataset,PROD)",
        changeType="UPSERT",
        aspectName="datasetProperties",
        aspect=DatasetPropertiesClass(
            description="Example dataset description",
            customProperties={"encoding": "utf-8"}
        )
    )]
    return mcps

def emit_to_file(mcps: list[MetadataChangeProposalWrapper], filename: str):
    file_emitter = FileEmitter(filename)
    for mcp in mcps:
        file_emitter.emit(mcp)
    file_emitter.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_file",
        required=True,
        help="Output file path for MCPs"
    )
    args = parser.parse_args()

    mcps = create_mcps()
    emit_to_file(mcps, args.output_file)

```

Edit `mcps` list in create_mcps to create the event and entities for your needs.

Run the Python script to generate your defined MCPs and save them to a file:

```bash
python <file_name>.py --output_file="mcps.json"
```