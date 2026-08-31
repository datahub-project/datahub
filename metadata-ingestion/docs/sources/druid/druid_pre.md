### Overview

The `druid` module ingests metadata from Druid into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Table, row, and column statistics via optional SQL profiling.

:::tip Schema pattern

It is important to explicitly define the deny schema pattern for internal Druid databases (lookup & sys) if adding a schema pattern. Otherwise, the crawler may crash before processing relevant databases. This deny pattern is defined by default but is overriden by user-submitted configurations.
:::

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
