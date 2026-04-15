### Overview

The `dremio` module ingests metadata from Dremio into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin integrates with Dremio to extract and ingest metadata into DataHub. The following types of metadata are extracted:

- Metadata for Spaces, Folders, Sources, and Datasets:
  - Includes physical and virtual datasets, with detailed information about each dataset.
  - Extracts metadata about Dremio's organizational hierarchy: Spaces (top-level), Folders (sub-level), and Sources (external data connections).
    \*Schema and Column Information:
  - Column types and schema metadata associated with each physical and virtual dataset.
  - Extracts column-level metadata, such as names, data types, and descriptions, if available.
- Lineage Information:
  - Dataset-level and column-level lineage tracking:
    - Dataset-level lineage shows dependencies and relationships between physical and virtual datasets.
    - Column-level lineage tracks transformations applied to individual columns across datasets.
  - Lineage information helps trace the flow of data and transformations within Dremio.
- Ownership and Glossary Terms:
  - Metadata related to ownership of datasets, extracted from Dremio’s ownership model.
  - Glossary terms and business metadata associated with datasets, providing additional context to the data.
  - Note: Ownership information will only be available for the Cloud and Enterprise editions, it will not be available for the Community edition.
- Optional SQL Profiling (if enabled):
  - Table, row, and column statistics can be profiled and ingested via optional SQL queries.
  - Extracts statistics about tables and columns, such as row counts and data distribution, for better insight into the dataset structure.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. **Generate an API Token**:

   - Log in to your Dremio instance.
   - Navigate to your user profile in the top-right corner.
   - Select **Generate API Token** to create an API token for programmatic access.

2. **Permissions**:

   - The token should have **read-only** or **admin** permissions that allow it to:
     - View all datasets (physical and virtual).
     - Access all spaces, folders, and sources.
     - Retrieve dataset and column-level lineage information.

3. **Verify External Data Source Permissions**:
   - If Dremio is connected to external data sources (e.g., AWS S3, relational databases), ensure that Dremio has access to the credentials required for querying those sources.
