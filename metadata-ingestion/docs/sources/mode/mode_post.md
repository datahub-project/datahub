### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Report

Report metadata is sourced from Mode report APIs, including title, description, ownership, and chart associations.

- [`/api/{account}/reports/{report}`](https://mode.com/developer/api-reference/analytics/reports/)

#### Chart

Chart-level metadata is sourced from Mode chart APIs:

- [`/api/{workspace}/reports/{report}/queries/{query}/charts`](https://mode.com/developer/api-reference/analytics/charts/#getChart)

#### Chart Information

Extracted chart details include chart type, chart title, and chart-specific metadata used to build DataHub chart entities.

#### Table Information

Table result metadata from report queries is used to identify upstream dataset context and query relationships.

#### Pivot Table Information

Pivot result metadata is extracted when available to improve chart/dataset relationship coverage for pivot-based analyses.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
