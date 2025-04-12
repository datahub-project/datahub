This connector ingests [Hex](https://hex.tech/) assets into DataHub.

### Concept Mapping

| Hex Concept | DataHub Concept                                                                                    | Notes               |
|-------------|----------------------------------------------------------------------------------------------------|---------------------|
| `"hex"`     | [Data Platform](https://datahubproject.io/docs/generated/metamodel/entities/dataplatform/)         |                     |
| Workspace   | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)                |  |
| Project     | [Dashboard](https://datahubproject.io/docs/generated/metamodel/entities/dashboard/)                | Subtype `Project`   |
| Component   | [Dashboard](https://datahubproject.io/docs/generated/metamodel/entities/dashboard/)                | Subtype `Component` |
| Collection  | [Tag](https://datahubproject.io/docs/generated/metamodel/entities/Tag/)                            |  |

Other Hex concepts are not mapped to DataHub entities yet.

### Limitations

Currently, the [Hex API](https://learn.hex.tech/docs/api/api-reference) has some limitations that affect the completeness of the extracted metadata:

1. **Projects and Components Relationship**: The API does not support fetching the many-to-many relationship between Projects and their Components.

2. **Metadata Access**: There is no direct method to retrieve metadata for Collections, Status, or Categories. This information is only available indirectly through references within Projects and Components.

Please keep these limitations in mind when working with the Hex connector.

For the **Dataset - Hex Project lineage**, the connector relies on the 
[_Hex query metadata_](https://learn.hex.tech/docs/explore-data/cells/sql-cells/sql-cells-introduction#query-metadata) feature.
Therefore, in order to extract lineage information, the required setup must include:

- A separated warehouse ingestor (_eg_ BigQuery, Snowflake, Redshift, ...) with `use_queries_v2` enabled in order to fetch Queries.
  This will ingest the queries into DataHub as `Query` entities and the ones triggered by Hex will include the corresponding _Hex query metadata_.
- A DataHub server with version >= SaaS `0.3.10` or > OSS `1.0.0` so the `Query` entities are properly indexed by source (Hex in this case) and so fetched and processed by the Hex ingestor in order to emit the Dataset - Project lineage.

Please note:

- Lineage is only captured for scheduled executions of the Project.
- In cases where queries are handled by [`hextoolkit`](https://learn.hex.tech/tutorials/connect-to-data/using-the-hextoolkit), _Hex query metadata_ is not injected, which prevents capturing lineage.
