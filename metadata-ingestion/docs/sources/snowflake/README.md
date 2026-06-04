## Overview

Snowflake is a data platform used to store and query analytical or operational data. Learn more in the [official Snowflake documentation](https://www.snowflake.com/).

The DataHub integration for Snowflake covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, tags, and stateful deletion detection.

:::info Snowflake Ingestion through the UI

The following video shows you how to ingest Snowflake metadata through the UI.

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/15d0401caa1c4aa483afef1d351760db"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

Read on if you are interested in ingesting Snowflake metadata using the **datahub** cli, or want to learn about all the configuration parameters that are supported by the connectors.
:::

## Concept Mapping

| Snowflake Concept               | DataHub Entity (Subtype)                                     | Notes                                                                                                                               |
| ------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Account                         | Platform Instance                                            | Top-level scope; all URNs include the configured platform instance.                                                                 |
| Database                        | Container (DATABASE)                                         | Top-level namespace. Ingested with description, tags, and Snowsight URL.                                                            |
| Schema                          | Container (SCHEMA)                                           | Nested under its Database container.                                                                                                |
| Table                           | Dataset (TABLE)                                              | Includes regular, Iceberg, and hybrid tables. Schema, PKs/FKs, tags, and descriptions are extracted.                                |
| Dynamic Table                   | Dataset (DYNAMIC TABLE)                                      | Includes target lag, SQL definition, and lineage to source tables.                                                                  |
| View                            | Dataset (VIEW)                                               | Standard, materialized, and secure views. View definition is captured.                                                              |
| Semantic View                   | Dataset (SEMANTIC VIEW)                                      | Columns classified as DIMENSION, FACT, or METRIC. Column-level lineage to physical tables is extracted.                             |
| Stream                          | Dataset (SNOWFLAKE STREAM)                                   | Change-data-capture stream. Adds `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` columns and lineage to the source table. |
| External Table                  | Dataset (TABLE)                                              | Lineage to the backing cloud storage location is emitted when available.                                                            |
| Internal Stage                  | Container (SNOWFLAKE STAGE) + Dataset (SNOWFLAKE STAGE DATA) | Emits both a Container (organizational) and a Dataset (for the resident data).                                                      |
| External Stage                  | Container (SNOWFLAKE STAGE)                                  | Container only; the backing cloud storage asset (S3/GCS/Azure) is referenced via lineage.                                           |
| Task                            | DataFlow (SNOWFLAKE TASK GROUP) + DataJob (SNOWFLAKE TASK)   | One DataFlow per schema; each task is a DataJob. Predecessor relationships appear as DataJob inputs.                                |
| Pipe                            | DataFlow (SNOWFLAKE PIPE GROUP) + DataJob (SNOWFLAKE PIPE)   | One DataFlow per schema; each pipe is a DataJob linking a stage to a target table via lineage.                                      |
| Streamlit App                   | Dashboard (STREAMLIT)                                        | App name, owner, and Snowsight URL captured as custom properties.                                                                   |
| Column / field                  | SchemaField                                                  | Column type, nullability, descriptions, and tags are extracted where available.                                                     |
| Role                            | CorpGroup                                                    | Ownership roles are mapped to `urn:li:corpGroup:{role_name}`.                                                                       |
| Tag                             | Tag or Structured Property                                   | Controlled by `extract_tags` config. Tags support database/schema/table/column inheritance.                                         |
| Table- and column-level lineage | Lineage edges                                                | Extracted from view definitions, dynamic table definitions, and SQL query history.                                                  |
| Query operations and usage      | DatasetUsageStatistics, Operation                            | Per-dataset query counts, user access patterns, and DML operation metrics.                                                          |
