### Overview

The `clickhouse-usage` module ingests metadata from Clickhouse into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin has the below functionalities:

- For a specific dataset this plugin ingests the following statistics -
  - top n queries.
  - top users.
  - usage of each column in the dataset.
- Aggregation of these statistics into buckets, by day or hour granularity.
- Usage information is computed by querying the `system.query_log` table. In case you have a cluster or need to apply additional
  transformation/filters you can create a view and put to the `query_log_table` setting.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
