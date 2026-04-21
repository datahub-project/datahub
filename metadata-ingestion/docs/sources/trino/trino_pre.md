### Overview

The `trino` module ingests metadata from Trino into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extracts metadata and two distinct relations to connector datasets (e.g. Iceberg, Hive):

- Siblings: Same logical dataset in two platforms (Trino catalog table ↔ connector table).
- UpstreamLineage: This Trino dataset reads from the connector dataset (table or view). Optionally includes column-level lineage (fineGrainedLineages) when schema is available.
- Also extracts: databases, schemas, tables, column types, optional profiling.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
