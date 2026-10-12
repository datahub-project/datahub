### Overview

The `sql-queries` module ingests metadata from SQL Queries into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source reads SQL query records and parses them to generate lineage and usage. Query records can come from a newline-delimited JSON file (local path or S3) via `query_file`, or from an Elasticsearch/OpenSearch index via `query_source`. Exactly one of the two must be configured.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
