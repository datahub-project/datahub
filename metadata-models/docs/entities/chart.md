# Chart

Charts are visual representations of data, typically found in Business Intelligence (BI) platforms and dashboarding tools. In DataHub, charts represent individual visualizations such as bar charts, pie charts, line graphs, tables, and other data displays. Charts are typically ingested from platforms like Looker, Tableau, PowerBI, Superset, Mode, and other BI tools.

## Identity

Charts are identified by two pieces of information:

- **The platform that they belong to**: This is the specific BI tool or dashboarding platform that hosts the chart. Examples include `looker`, `tableau`, `powerbi`, `superset`, `mode`, etc. This corresponds to the `dashboardTool` field in the chart's key aspect.
- **The chart identifier in the specific platform**: Each platform has its own way of uniquely identifying charts within its system. For example, Looker uses identifiers like `look/1234`, while PowerBI might use tile identifiers like `tile-abc-123`.

An example of a chart identifier is `urn:li:chart:(looker,look/1234)`.

For platforms with multiple instances (e.g., separate Looker deployments for different environments), the URN can include a platform instance identifier: `urn:li:chart:(looker,look/1234,prod-instance)`.

## Important Capabilities

### Chart Information and Metadata

The core metadata about a chart is stored in the `chartInfo` aspect. This includes:

- **Title**: The display name of the chart (searchable)
- **Description**: A detailed description of what the chart shows
- **Chart Type**: The type of visualization (BAR, PIE, SCATTER, TABLE, TEXT, LINE, AREA, HISTOGRAM, BOX_PLOT, WORD_CLOUD, COHORT)
- **Chart URL**: A link to view the chart in its native platform
- **Access Level**: The access level of the chart (PUBLIC or PRIVATE)
- **Last Modified**: Audit stamps tracking when the chart was created, modified, or deleted
- **Last Refreshed**: Timestamp of when the chart data was last refreshed

The following code snippet shows you how to create a chart with basic information.

<details>
<summary>Python SDK: Create a chart</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_create_simple.py show_path_as_comment }}
```

</details>

For more complex chart creation with additional metadata:

<details>
<summary>Python SDK: Create a chart with full metadata</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_create_complex.py show_path_as_comment }}
```

</details>

### Chart Queries

Charts often have underlying queries (SQL, LookML, etc.) that define how the data is retrieved and processed. The `chartQuery` aspect stores this information:

- **Raw Query**: The actual query text used to generate the chart
- **Query Type**: The type of query (LOOKML or SQL)

This information is particularly useful for understanding data lineage and for auditing purposes.

### Data Lineage and Input Datasets

Charts consume data from one or more datasets (or sometimes from other charts). The `chartInfo` aspect's `inputEdges` field tracks these relationships, creating `Consumes` relationships in the metadata graph. This enables:

- **Impact Analysis**: Understanding which charts are affected when a dataset changes
- **Data Lineage Visualization**: Showing the flow of data from source datasets through to charts
- **Dependency Tracking**: Identifying all upstream dependencies for a chart

<details>
<summary>Python SDK: Add lineage to a chart</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_add_lineage.py show_path_as_comment }}
```

</details>

### Field-Level Lineage

The `inputFields` aspect provides fine-grained tracking of which specific dataset fields (columns) are referenced by the chart. Each input field creates a `consumesField` relationship to a `schemaField` entity, enabling:

- **Column-Level Impact Analysis**: Understanding which charts use a specific column
- **Data Sensitivity Tracking**: Identifying charts that display sensitive fields
- **Schema Change Impact**: Predicting the effect of schema changes on visualizations

### Editable Properties

DataHub separates metadata that comes from ingestion sources (in `chartInfo`) from metadata that users edit in the DataHub UI (in `editableChartProperties`). This separation ensures that:

- User edits are preserved across ingestion runs
- Source system metadata remains authoritative for its fields
- Users can enhance metadata without interfering with automated ingestion

The `editableChartProperties` aspect currently supports:

- **Description**: A user-provided description that supplements or overrides the ingested description

### Tags and Glossary Terms

Charts can have Tags or Terms attached to them. Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms.

#### Adding Tags to a Chart

Tags are added to charts using the `globalTags` aspect.

<details>
<summary>Python SDK: Add a tag to a chart</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_add_tag.py show_path_as_comment }}
```

</details>

#### Adding Glossary Terms to a Chart

Glossary terms are added using the `glossaryTerms` aspect.

<details>
<summary>Python SDK: Add a glossary term to a chart</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_add_term.py show_path_as_comment }}
```

</details>

### Ownership

Ownership is associated with a chart using the `ownership` aspect. Owners can be of different types such as `DATAOWNER`, `TECHNICAL_OWNER`, `BUSINESS_OWNER`, etc. Ownership can be inherited from source systems or added in DataHub.

<details>
<summary>Python SDK: Add an owner to a chart</summary>

```python
{{ inline /metadata-ingestion/examples/library/chart_add_owner.py show_path_as_comment }}
```

</details>

### Usage Statistics

Charts can track usage metrics through the `chartUsageStatistics` aspect (experimental). This timeseries aspect captures:

- **Views Count**: Total number of times the chart has been viewed
- **Unique User Count**: Number of distinct users who viewed the chart
- **Per-User Counts**: Detailed usage breakdown by individual users

Usage statistics help identify:

- Popular charts that might need performance optimization
- Unused charts that could be deprecated
- User engagement patterns

### Organizational Context

#### Domains

Charts can be organized into Domains (business areas or data products) using the `domains` aspect. This helps with:

- Organizing charts by business function
- Access control and governance
- Discovery by domain experts

#### Containers

Charts typically belong to a Dashboard (their parent container). The `container` aspect tracks this relationship, creating a hierarchical structure:

```
Dashboard (Container)
├── Chart 1
├── Chart 2
└── Chart 3
```

This hierarchy is important for:

- Navigating related visualizations
- Understanding chart context
- Propagating metadata (like ownership) from dashboard to charts

### Embedding and External URLs

The `embed` aspect stores URLs that allow embedding the chart in external applications or viewing it in its native platform. This supports:

- Embedding charts in wikis or documentation
- Deep linking to the chart in the BI tool
- Integration with external portals

### Chart Subtypes

Charts from different platforms may have platform-specific subtypes defined in the `subTypes` aspect. Examples include:

- **Looker**: `Look` (a saved Looker visualization)
- **PowerBI**: `PowerBI Tile` (a tile on a PowerBI report page)
- **Mode**: `Chart`, `Report` (different Mode visualization types)

Subtypes help users understand the platform-specific nature of the chart.

## Integration with External Systems

### Ingestion from BI Platforms

Charts are typically ingested automatically from BI platforms using DataHub's ingestion connectors:

- **Looker**: Ingests Looks (saved visualizations) and dashboard elements
- **Tableau**: Ingests sheets (worksheets) from workbooks
- **PowerBI**: Ingests tiles from reports
- **Superset**: Ingests charts from dashboards
- **Mode**: Ingests charts and visualizations
- **Metabase**: Ingests questions and visualizations

Each connector maps platform-specific chart metadata to DataHub's standardized chart model.

### Querying Chart Information

You can retrieve chart information using DataHub's REST API:

<details>
<summary>Fetch chart entity snapshot</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Achart%3A(looker,look%2F1234)'
```

The response includes all aspects of the chart, including:

- Chart information (title, description, type)
- Input datasets (lineage)
- Ownership, tags, and terms
- Usage statistics
- And all other configured aspects

</details>

### Relationships API

You can query chart relationships to understand its connections to other entities:

<details>
<summary>Find datasets consumed by a chart</summary>

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Achart%3A(looker,look%2F1234)&types=Consumes'
```

This returns all datasets (and potentially other charts) that this chart consumes data from.

</details>

<details>
<summary>Find charts that consume a specific dataset</summary>

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Abigquery,project.dataset.table,PROD)&types=Consumes'
```

This returns all charts that depend on the specified dataset.

</details>

## GraphQL API

Charts are fully supported in DataHub's GraphQL API, which provides:

- **Queries**: Search, browse, and retrieve charts
- **Mutations**: Create, update, and delete charts
- **Faceted Search**: Filter charts by tool, type, access level, and query type
- **Lineage Queries**: Traverse upstream and downstream relationships
- **Batch Loading**: Efficiently load multiple charts

The GraphQL `Chart` type includes all standard metadata fields plus chart-specific properties like query definitions and usage statistics.

## Notable Exceptions

### Platform-Specific Variations

Different BI platforms have different concepts that map to charts:

- **Tableau Sheets vs Dashboards**: In Tableau, a "sheet" (worksheet) maps to a DataHub chart, while a "dashboard" maps to a DataHub dashboard entity. A Tableau dashboard can contain multiple sheets.

- **PowerBI Tiles**: PowerBI has the concept of "tiles" (pinned visualizations) which are modeled as charts in DataHub. A tile can reference multiple underlying reports or datasets.

- **Looker Looks vs Dashboard Elements**: Looker has standalone "Looks" (saved visualizations) and dashboard elements. Both are modeled as charts in DataHub.

### Chart-to-Chart Lineage

While most charts consume data from datasets, some platforms support charts that derive from other charts. DataHub supports this through chart-to-chart `Consumes` relationships, enabling multi-level visualization lineage.

### Deprecated Fields

The `chartInfo.inputs` field is deprecated in favor of `chartInfo.inputEdges`. The `inputEdges` field provides richer relationship metadata including timestamps and actors for when relationships were created or modified.

## Related Entities

Charts frequently interact with these other DataHub entities:

- **Dashboard**: Charts are typically contained within dashboards
- **Dataset**: Charts consume data from datasets
- **SchemaField**: Charts reference specific fields/columns through field-level lineage
- **DataPlatform**: Charts are associated with a specific BI platform
- **Domain**: Charts can be organized into business domains
- **GlossaryTerm**: Charts can be annotated with business terms
- **Tag**: Charts can be tagged for classification and discovery
- **CorpUser / CorpGroup**: Charts have owners and are used by users
