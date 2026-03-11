### Overview

The `sql-queries` module ingests metadata from SQL Queries into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source reads a newline-delimited JSON file containing SQL queries and parses them to generate lineage.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
