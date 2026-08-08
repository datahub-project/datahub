# Notebook

A Notebook is a metadata entity that represents interactive computational documents combining code execution, text documentation, data visualizations, and query results. Notebooks are collaborative environments for data analysis, exploration, and documentation, commonly used in data science, analytics, and business intelligence workflows.

The Notebook entity captures both the structural components (cells containing text, queries, or charts) and the metadata about notebooks from platforms like Jupyter, Databricks, QueryBook, Hex, Mode, Deepnote, and other notebook-based tools.

> **⚠️ Notice**: The Notebook entity is currently in **BETA**. While the core functionality is stable, the entity model and UI features may evolve based on community feedback. Notebook support is actively being developed and improved.

## Identity

A Notebook is uniquely identified by two components:

- **notebookTool**: The name of the notebook platform or tool (e.g., "querybook", "jupyter", "databricks", "hex")
- **notebookId**: A globally unique identifier for the notebook within that tool

The URN structure for a Notebook is:

```
urn:li:notebook:(<notebookTool>,<notebookId>)
```

### Examples

```
urn:li:notebook:(querybook,773)
urn:li:notebook:(jupyter,analysis_2024_q1)
urn:li:notebook:(databricks,/Users/analyst/customer_segmentation)
urn:li:notebook:(hex,a8b3c5d7-1234-5678-90ab-cdef12345678)
```

### Generating Stable Notebook IDs

The `notebookId` should be globally unique for a notebook tool, even when there are multiple deployments. Best practices include:

- **URL-based IDs**: Use the notebook URL or path (e.g., `querybook.com/notebook/773`)
- **Platform IDs**: Use the platform's native notebook identifier (e.g., Databricks workspace path)
- **UUID**: Generate a stable UUID based on notebook metadata for platforms without native IDs
- **File paths**: For Jupyter notebooks, use the file path relative to a known root directory

The key requirement is that the same notebook should always produce the same URN across different ingestion runs.

## Important Capabilities

### Notebook Information

The `notebookInfo` aspect contains the core metadata about a notebook:

- **title**: The notebook's display name (searchable and used in autocomplete)
- **description**: Detailed description of what the notebook does or analyzes
- **customProperties**: Key-value pairs for platform-specific metadata
- **externalUrl**: Link to the notebook in its native platform
- **changeAuditStamps**: Tracking of who created/modified the notebook and when

The following code snippet shows you how to create a Notebook with basic information.


**Python SDK: Create a Notebook**

```python
# Inlined from /metadata-ingestion/examples/library/notebook_create.py
# metadata-ingestion/examples/library/notebook_create.py
import logging
import time
from typing import Dict, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    NotebookInfoClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_notebook_metadata(
    notebook_urn: str,
    title: str,
    description: str,
    external_url: str,
    custom_properties: Optional[Dict[str, str]] = None,
    actor: str = "urn:li:corpuser:data_scientist",
    timestamp_millis: Optional[int] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create metadata for a notebook entity.

    Args:
        notebook_urn: URN of the notebook
        title: Title of the notebook
        description: Description of the notebook
        external_url: URL to access the notebook
        custom_properties: Optional dictionary of custom properties
        actor: URN of the actor creating the notebook
        timestamp_millis: Optional timestamp in milliseconds (defaults to current time)

    Returns:
        MetadataChangeProposalWrapper containing the notebook metadata
    """
    timestamp_millis = timestamp_millis or int(time.time() * 1000)

    audit_stamp = AuditStampClass(time=timestamp_millis, actor=actor)

    notebook_info = NotebookInfoClass(
        title=title,
        description=description,
        externalUrl=external_url,
        customProperties=custom_properties or {},
        changeAuditStamps=ChangeAuditStampsClass(
            created=audit_stamp,
            lastModified=audit_stamp,
        ),
    )

    return MetadataChangeProposalWrapper(
        entityUrn=notebook_urn,
        aspect=notebook_info,
    )


def main(emitter: Optional[DatahubRestEmitter] = None) -> None:
    """
    Main function to create a notebook example.

    Args:
        emitter: Optional emitter to use (for testing). If not provided, creates a new one.

    Environment Variables:
        DATAHUB_GMS_URL: DataHub GMS server URL (default: http://localhost:8080)
        DATAHUB_GMS_TOKEN: DataHub access token (if authentication is required)
    """
    if emitter is None:
        import os

        gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
        token = os.getenv("DATAHUB_GMS_TOKEN")

        # If no token in env, try to get from datahub config
        if not token:
            try:
                from datahub.ingestion.graph.client import get_default_graph

                graph = get_default_graph()
                token = graph.config.token
            except Exception:
                # Fall back to no token
                pass

        emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

    notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

    event = create_notebook_metadata(
        notebook_urn=notebook_urn,
        title="Customer Segmentation Analysis 2024",
        description="Comprehensive analysis of customer segments including RFM analysis, cohort analysis, and predictive scoring for marketing campaigns",
        external_url="https://querybook.company.com/notebook/customer_analysis_2024",
        custom_properties={
            "workspace": "analytics",
            "team": "growth",
            "last_run": "2024-01-15T10:30:00Z",
        },
    )

    emitter.emit(event)
    log.info(f"Created notebook {notebook_urn}")


if __name__ == "__main__":
    main()

```



### Notebook Content

The `notebookContent` aspect captures the actual structure and content of a notebook through a list of cells. Each cell represents a distinct block of content within the notebook.

#### Cell Types

Notebooks support three types of cells:

1. **TEXT_CELL**: Markdown or rich text content for documentation, explanations, and narrative

   - Contains formatted text, headings, lists, images, and documentation
   - Used to explain analysis steps, provide context, and create reports

2. **QUERY_CELL**: SQL or other query language statements for data retrieval and transformation

   - Contains executable query code
   - References datasets and produces result sets
   - Can be linked to specific query entities for lineage tracking

3. **CHART_CELL**: Data visualizations and charts built from query results
   - Contains configuration for charts and visualizations
   - Can reference chart entities for metadata consistency
   - Represents visual output from data analysis

Each cell in the `notebookContent` aspect includes:

- **type**: The cell type (TEXT_CELL, QUERY_CELL, or CHART_CELL)
- **textCell**: Content for text cells (null for other types)
- **queryCell**: Content for query cells (null for other types)
- **chartCell**: Content for chart cells (null for other types)

The cell list represents the sequential structure of the notebook as it appears to users.


**Python SDK: Add content to a Notebook**

```python
# Inlined from /metadata-ingestion/examples/library/notebook_add_content.py
# metadata-ingestion/examples/library/notebook_add_content.py
import logging
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartCellClass,
    NotebookCellClass,
    NotebookCellTypeClass,
    NotebookContentClass,
    QueryCellClass,
    TextCellClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor="urn:li:corpuser:data_scientist"
)
change_audit = ChangeAuditStampsClass(created=audit_stamp, lastModified=audit_stamp)

cells = [
    NotebookCellClass(
        type=NotebookCellTypeClass.TEXT_CELL,
        textCell=TextCellClass(
            cellId="cell-1",
            cellTitle="Introduction",
            text="# Customer Segmentation Analysis\n\nThis notebook analyzes customer behavior patterns to identify high-value segments.",
            changeAuditStamps=change_audit,
        ),
    ),
    NotebookCellClass(
        type=NotebookCellTypeClass.QUERY_CELL,
        queryCell=QueryCellClass(
            cellId="cell-2",
            cellTitle="Customer Activity Query",
            rawQuery="SELECT customer_id, SUM(revenue) as total_revenue, COUNT(*) as order_count FROM orders WHERE order_date >= '2024-01-01' GROUP BY customer_id ORDER BY total_revenue DESC LIMIT 1000",
            lastExecuted=audit_stamp,
            changeAuditStamps=change_audit,
        ),
    ),
    NotebookCellClass(
        type=NotebookCellTypeClass.CHART_CELL,
        chartCell=ChartCellClass(
            cellId="cell-3",
            cellTitle="Revenue Distribution by Segment",
            changeAuditStamps=change_audit,
        ),
    ),
]

notebook_content = NotebookContentClass(cells=cells)

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=notebook_content,
)

emitter.emit(event)
log.info(f"Added content to notebook {notebook_urn}")

```



### Editable Properties

The `editableNotebookProperties` aspect allows users to add or modify certain notebook properties through the DataHub UI without affecting the source system:

- **description**: User-editable description that supplements or overrides the ingested description

This separation allows DataHub users to enrich notebook metadata while preserving the original information from the source platform.

### Ownership

Notebooks support ownership through the `ownership` aspect, allowing you to track who is responsible for maintaining and governing each notebook. Ownership types include:

- **TECHNICAL_OWNER**: Engineers or data scientists who created or maintain the notebook
- **BUSINESS_OWNER**: Business stakeholders who own the analysis or insights
- **DATA_STEWARD**: Data governance personnel responsible for notebook quality and compliance


**Python SDK: Add ownership to a Notebook**

```python
# Inlined from /metadata-ingestion/examples/library/notebook_add_owner.py
# metadata-ingestion/examples/library/notebook_add_owner.py
import logging

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

owner_to_add = make_user_urn("data_scientist")
ownership_type = OwnershipTypeClass.TECHNICAL_OWNER

owners_to_add = [
    OwnerClass(owner=owner_to_add, type=ownership_type),
]

ownership = OwnershipClass(owners=owners_to_add)

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=ownership,
)

emitter.emit(event)
log.info(f"Added owner {owner_to_add} to notebook {notebook_urn}")

```



### Tags and Glossary Terms

Notebooks can be tagged and associated with glossary terms for organization and discovery:

- **Tags** (via `globalTags` aspect): Informal categorization labels like "exploratory", "production", "deprecated", "customer-analysis"
- **Glossary Terms** (via `glossaryTerms` aspect): Formal business vocabulary linking notebooks to business concepts


**Python SDK: Add tags to a Notebook**

```python
# Inlined from /metadata-ingestion/examples/library/notebook_add_tags.py
# metadata-ingestion/examples/library/notebook_add_tags.py
import logging

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

tag_to_add = make_tag_urn("production")
tag_association = TagAssociationClass(tag=tag_to_add)

global_tags = GlobalTagsClass(tags=[tag_association])

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=global_tags,
)

emitter.emit(event)
log.info(f"Added tag {tag_to_add} to notebook {notebook_urn}")

```



### Domains

Notebooks can be assigned to one or more domains through the `domains` aspect, organizing them by business unit, team, or functional area. This helps with discovery and governance at scale.

### Browse Paths

The `browsePaths` and `browsePathsV2` aspects enable hierarchical navigation of notebooks within DataHub, allowing users to browse notebooks by platform, workspace, folder, or other organizational structures.

### Applications

The `applications` aspect allows linking notebooks to specific applications or use cases, helping track which business applications or workflows depend on particular notebooks.

### Sub Types

The `subTypes` aspect enables classification of notebooks into categories like:

- "Data Analysis"
- "ML Training"
- "Reporting"
- "Data Exploration"
- "ETL Development"

This helps users find notebooks relevant to their specific needs.

### Institutional Memory

Through the `institutionalMemory` aspect, notebooks can have links to external documentation, wikis, runbooks, or other resources that provide additional context about their purpose and usage.

### Test Results

The `testResults` aspect can capture the results of data quality tests or validation checks performed within the notebook, integrating notebook-based testing into DataHub's data quality framework.

## Integration Points

### Relationship with Datasets

Notebooks have relationships with datasets through query cells:

- **Query Subjects**: When a notebook's query cell references datasets, those relationships are captured
- **Lineage**: Notebooks can be sources of lineage information when their queries create or transform data
- **Usage Tracking**: Notebooks contribute to dataset usage statistics through their query execution patterns

### Relationship with Charts

When a notebook contains chart cells, those cells can reference chart entities, creating a relationship between the notebook and the visualizations it produces. This is particularly relevant for BI notebook tools like Mode or Hex where notebooks generate reusable charts.

### Relationship with Queries

Query cells in notebooks can be linked to query entities, enabling:

- **Query Reuse**: Track where specific queries are used across different notebooks
- **Lineage Propagation**: Leverage SQL parsing from query entities for notebook lineage
- **Usage Analytics**: Understand query patterns in the context of notebook workflows

### Platform Instance

The `dataPlatformInstance` aspect associates a notebook with a specific instance of a notebook platform (e.g., a particular Databricks workspace or Hex account), which is essential when multiple instances of the same platform exist.

### Ingestion Sources

Several DataHub connectors extract notebook metadata:

- **QueryBook**: Ingests notebooks with their cells and metadata
- **Jupyter**: Can process notebook files from repositories or file systems
- **Databricks**: Extracts notebooks from Databricks workspaces
- **Hex**: Ingests notebooks and their project context
- **Mode**: Extracts notebooks (called "reports" in Mode) with their queries and visualizations
- **Deepnote**: Can ingest collaborative notebooks from Deepnote projects

These connectors typically:

1. Discover notebooks from the platform's API or file system
2. Extract notebook metadata (title, description, author, timestamps)
3. Parse notebook structure into cells of appropriate types
4. Create relationships to referenced datasets, queries, and charts
5. Track ownership and collaboration information

### GraphQL API

Notebooks are accessible through DataHub's GraphQL API, supporting queries for:

- Notebook metadata and properties
- Notebook content and cell structure
- Relationships to datasets, charts, and queries
- Ownership and governance information

## Notable Exceptions

### Beta Status

As a BETA feature, notebooks have some limitations:

- **UI Support**: The DataHub web interface may not fully visualize all notebook capabilities
- **Lineage Extraction**: Automatic lineage from notebook queries may vary by platform
- **Search and Discovery**: Notebook-specific search features are still evolving
- **Cell Execution State**: Execution results and output cells are not currently captured

Users should expect ongoing improvements and potential schema changes as the feature matures.

### Cell Content Storage

Notebook cells store structural information and metadata but may not capture:

- **Full Execution Output**: Large result sets from query execution
- **Binary Attachments**: Images or files embedded in notebooks (except via URLs)
- **Interactive Widgets**: Dynamic UI elements in notebooks like ipywidgets

The focus is on capturing the notebook's code, structure, and metadata rather than execution artifacts.

### Platform-Specific Features

Different notebook platforms have unique features that may not map perfectly to DataHub's model:

- **Databricks**: Collaborative features, version control, and job scheduling
- **Hex**: App-building features and parameter inputs
- **Jupyter**: Kernel-specific features and extensions
- **Mode**: Report scheduling and sharing configurations

Ingestion connectors capture common features while platform-specific capabilities may be stored in `customProperties`.

### Cell Ordering

The `notebookContent` cells array preserves the order of cells as they appear in the source notebook. However, notebooks with complex branching logic or non-linear execution flows may not be fully represented by a simple ordered list.

### Versioning

The current notebook model doesn't natively track notebook versions or revision history. The `changeAuditStamps` captures last modified information, but full version control requires integration with the source platform's versioning system (e.g., Git for Jupyter, platform version history for Databricks).

### Large Notebooks

Very large notebooks with hundreds of cells may face performance considerations:

- Ingestion time increases with notebook size
- UI rendering may be optimized for notebook metadata rather than full content display
- Consider splitting extremely large notebooks into smaller, focused notebooks for better manageability

## Use Cases

Notebooks in DataHub enable several important use cases:

1. **Discovery**: Find notebooks related to specific datasets, business domains, or analysis topics
2. **Documentation**: Understand how data is analyzed and transformed through self-documenting notebook code
3. **Lineage**: Track data flows through notebook-based ETL and transformation pipelines
4. **Collaboration**: Identify notebook owners and subject matter experts for specific analyses
5. **Governance**: Apply tags, terms, and classifications to notebook-based analytics
6. **Impact Analysis**: Understand downstream dependencies when datasets used by notebooks change
7. **Knowledge Management**: Preserve institutional knowledge embedded in analysis notebooks

By bringing notebooks into DataHub's metadata graph, organizations can treat analysis code with the same rigor as production data assets.



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### notebookInfo
Information about a Notebook
Note: This is IN BETA version



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| title | string | ✓ | Title of the Notebook | Searchable |
| description | string |  | Detailed description about the Notebook | Searchable |
| changeAuditStamps | [ChangeAuditStamps](#changeauditstamps) | ✓ | Captures information about who created/last modified/deleted this Notebook and when |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "notebookInfo"
  },
  "name": "NotebookInfo",
  "namespace": "com.linkedin.notebook",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": "string",
      "name": "title",
      "doc": "Title of the Notebook"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Detailed description about the Notebook"
    },
    {
      "type": {
        "type": "record",
        "name": "ChangeAuditStamps",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": {
              "type": "record",
              "name": "AuditStamp",
              "namespace": "com.linkedin.common",
              "fields": [
                {
                  "type": "long",
                  "name": "time",
                  "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                },
                {
                  "java": {
                    "class": "com.linkedin.common.urn.Urn"
                  },
                  "type": "string",
                  "name": "actor",
                  "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                },
                {
                  "java": {
                    "class": "com.linkedin.common.urn.Urn"
                  },
                  "type": [
                    "null",
                    "string"
                  ],
                  "name": "impersonator",
                  "default": null,
                  "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                },
                {
                  "type": [
                    "null",
                    "string"
                  ],
                  "name": "message",
                  "default": null,
                  "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                }
              ],
              "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
            },
            "name": "created",
            "default": {
              "actor": "urn:li:corpuser:unknown",
              "impersonator": null,
              "time": 0,
              "message": null
            },
            "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
          },
          {
            "type": "com.linkedin.common.AuditStamp",
            "name": "lastModified",
            "default": {
              "actor": "urn:li:corpuser:unknown",
              "impersonator": null,
              "time": 0,
              "message": null
            },
            "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
          },
          {
            "type": [
              "null",
              "com.linkedin.common.AuditStamp"
            ],
            "name": "deleted",
            "default": null,
            "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations"
      },
      "name": "changeAuditStamps",
      "doc": "Captures information about who created/last modified/deleted this Notebook and when"
    }
  ],
  "doc": "Information about a Notebook\nNote: This is IN BETA version"
}
```





#### notebookContent
Content in a Notebook
Note: This is IN BETA version



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| cells | NotebookCell[] | ✓ | The content of a Notebook which is composed by a list of NotebookCell |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "notebookContent"
  },
  "name": "NotebookContent",
  "namespace": "com.linkedin.notebook",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "NotebookCell",
          "namespace": "com.linkedin.notebook",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TextCell",
                  "namespace": "com.linkedin.notebook",
                  "fields": [
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "cellTitle",
                      "default": null,
                      "doc": "Title of the cell"
                    },
                    {
                      "type": "string",
                      "name": "cellId",
                      "doc": "Unique id for the cell. This id should be globally unique for a Notebook tool even when there are multiple deployments of it. As an example, Notebook URL could be used here for QueryBook such as 'querybook.com/notebook/773/?cellId=1234'"
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "ChangeAuditStamps",
                        "namespace": "com.linkedin.common",
                        "fields": [
                          {
                            "type": {
                              "type": "record",
                              "name": "AuditStamp",
                              "namespace": "com.linkedin.common",
                              "fields": [
                                {
                                  "type": "long",
                                  "name": "time",
                                  "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                                },
                                {
                                  "java": {
                                    "class": "com.linkedin.common.urn.Urn"
                                  },
                                  "type": "string",
                                  "name": "actor",
                                  "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                                },
                                {
                                  "java": {
                                    "class": "com.linkedin.common.urn.Urn"
                                  },
                                  "type": [
                                    "null",
                                    "string"
                                  ],
                                  "name": "impersonator",
                                  "default": null,
                                  "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                                },
                                {
                                  "type": [
                                    "null",
                                    "string"
                                  ],
                                  "name": "message",
                                  "default": null,
                                  "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                                }
                              ],
                              "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                            },
                            "name": "created",
                            "default": {
                              "actor": "urn:li:corpuser:unknown",
                              "impersonator": null,
                              "time": 0,
                              "message": null
                            },
                            "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
                          },
                          {
                            "type": "com.linkedin.common.AuditStamp",
                            "name": "lastModified",
                            "default": {
                              "actor": "urn:li:corpuser:unknown",
                              "impersonator": null,
                              "time": 0,
                              "message": null
                            },
                            "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
                          },
                          {
                            "type": [
                              "null",
                              "com.linkedin.common.AuditStamp"
                            ],
                            "name": "deleted",
                            "default": null,
                            "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
                          }
                        ],
                        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations"
                      },
                      "name": "changeAuditStamps",
                      "doc": "Captures information about who created/last modified/deleted this Notebook cell and when"
                    },
                    {
                      "type": "string",
                      "name": "text",
                      "doc": "The actual text in a TextCell in a Notebook"
                    }
                  ],
                  "doc": "Text cell in a Notebook, which will present content in text format"
                }
              ],
              "name": "textCell",
              "default": null,
              "doc": "The text cell content. The will be non-null only when all other cell field is null."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "QueryCell",
                  "namespace": "com.linkedin.notebook",
                  "fields": [
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "cellTitle",
                      "default": null,
                      "doc": "Title of the cell"
                    },
                    {
                      "type": "string",
                      "name": "cellId",
                      "doc": "Unique id for the cell. This id should be globally unique for a Notebook tool even when there are multiple deployments of it. As an example, Notebook URL could be used here for QueryBook such as 'querybook.com/notebook/773/?cellId=1234'"
                    },
                    {
                      "type": "com.linkedin.common.ChangeAuditStamps",
                      "name": "changeAuditStamps",
                      "doc": "Captures information about who created/last modified/deleted this Notebook cell and when"
                    },
                    {
                      "type": "string",
                      "name": "rawQuery",
                      "doc": "Raw query to explain some specific logic in a Notebook"
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.common.AuditStamp"
                      ],
                      "name": "lastExecuted",
                      "default": null,
                      "doc": "Captures information about who last executed this query cell and when"
                    }
                  ],
                  "doc": "Query cell in a Notebook, which will present content in query format"
                }
              ],
              "name": "queryCell",
              "default": null,
              "doc": "The query cell content. The will be non-null only when all other cell field is null."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "ChartCell",
                  "namespace": "com.linkedin.notebook",
                  "fields": [
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "cellTitle",
                      "default": null,
                      "doc": "Title of the cell"
                    },
                    {
                      "type": "string",
                      "name": "cellId",
                      "doc": "Unique id for the cell. This id should be globally unique for a Notebook tool even when there are multiple deployments of it. As an example, Notebook URL could be used here for QueryBook such as 'querybook.com/notebook/773/?cellId=1234'"
                    },
                    {
                      "type": "com.linkedin.common.ChangeAuditStamps",
                      "name": "changeAuditStamps",
                      "doc": "Captures information about who created/last modified/deleted this Notebook cell and when"
                    }
                  ],
                  "doc": "Chart cell in a notebook, which will present content in chart format"
                }
              ],
              "name": "chartCell",
              "default": null,
              "doc": "The chart cell content. The will be non-null only when all other cell field is null."
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "CHART_CELL": "CHART Notebook cell type. The cell content is chart only.",
                  "QUERY_CELL": "QUERY Notebook cell type. The cell context is query only.",
                  "TEXT_CELL": "TEXT Notebook cell type. The cell context is text only."
                },
                "name": "NotebookCellType",
                "namespace": "com.linkedin.notebook",
                "symbols": [
                  "TEXT_CELL",
                  "QUERY_CELL",
                  "CHART_CELL"
                ],
                "doc": "Type of Notebook Cell"
              },
              "name": "type",
              "doc": "The type of this Notebook cell"
            }
          ],
          "doc": "A record of all supported cells for a Notebook. Only one type of cell will be non-null."
        }
      },
      "name": "cells",
      "default": [],
      "doc": "The content of a Notebook which is composed by a list of NotebookCell"
    }
  ],
  "doc": "Content in a Notebook\nNote: This is IN BETA version"
}
```





#### editableNotebookProperties
Stores editable changes made to properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines
Note: This is IN BETA version



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| description | string |  | Edited documentation of the Notebook | Searchable (editedDescription) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableNotebookProperties"
  },
  "name": "EditableNotebookProperties",
  "namespace": "com.linkedin.notebook",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Edited documentation of the Notebook"
    }
  ],
  "doc": "Stores editable changes made to properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines\nNote: This is IN BETA version"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```





#### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| paths | string[] | ✓ | A list of valid browse paths for the entity.  Browse paths are expected to be forward slash-separ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "context",
                "default": null,
                "doc": "Additional context about the association"
              },
              {
                "Searchable": {
                  "/actor": {
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "MetadataAttribution",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When this metadata was updated."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "source",
                        "default": null,
                        "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                      },
                      {
                        "type": {
                          "type": "map",
                          "values": "string"
                        },
                        "name": "sourceDetail",
                        "default": {},
                        "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                      }
                    ],
                    "doc": "Information about who, why, and how this metadata was applied"
                  }
                ],
                "name": "attribution",
                "default": null,
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





#### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| path | BrowsePathEntry[] | ✓ | A valid browse path for the entity. This field is provided by DataHub by default. This aspect is ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### ChangeAuditStamps

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations

**Fields:**

- `created` (AuditStamp): An AuditStamp corresponding to the creation of this resource/association/sub-...
- `lastModified` (AuditStamp): An AuditStamp corresponding to the last modification of this resource/associa...
- `deleted` (AuditStamp?): An AuditStamp corresponding to the deletion of this resource/association/sub-...

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Outgoing
These are the relationships stored in this entity's aspects
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
