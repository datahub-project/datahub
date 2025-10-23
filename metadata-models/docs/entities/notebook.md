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

<details>
<summary>Python SDK: Create a Notebook</summary>

```python
{{ inline /metadata-ingestion/examples/library/notebook_create.py show_path_as_comment }}
```

</details>

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

<details>
<summary>Python SDK: Add content to a Notebook</summary>

```python
{{ inline /metadata-ingestion/examples/library/notebook_add_content.py show_path_as_comment }}
```

</details>

### Editable Properties

The `editableNotebookProperties` aspect allows users to add or modify certain notebook properties through the DataHub UI without affecting the source system:

- **description**: User-editable description that supplements or overrides the ingested description

This separation allows DataHub users to enrich notebook metadata while preserving the original information from the source platform.

### Ownership

Notebooks support ownership through the `ownership` aspect, allowing you to track who is responsible for maintaining and governing each notebook. Ownership types include:

- **TECHNICAL_OWNER**: Engineers or data scientists who created or maintain the notebook
- **BUSINESS_OWNER**: Business stakeholders who own the analysis or insights
- **DATA_STEWARD**: Data governance personnel responsible for notebook quality and compliance

<details>
<summary>Python SDK: Add ownership to a Notebook</summary>

```python
{{ inline /metadata-ingestion/examples/library/notebook_add_owner.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

Notebooks can be tagged and associated with glossary terms for organization and discovery:

- **Tags** (via `globalTags` aspect): Informal categorization labels like "exploratory", "production", "deprecated", "customer-analysis"
- **Glossary Terms** (via `glossaryTerms` aspect): Formal business vocabulary linking notebooks to business concepts

<details>
<summary>Python SDK: Add tags to a Notebook</summary>

```python
{{ inline /metadata-ingestion/examples/library/notebook_add_tags.py show_path_as_comment }}
```

</details>

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
