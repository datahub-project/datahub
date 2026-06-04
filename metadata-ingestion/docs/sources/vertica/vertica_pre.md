### Overview

The `vertica` module ingests metadata from Vertica into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The DataHub Vertica plugin extracts the following:

- Metadata for databases, schemas, views, tables, and projections
- Table level lineage
- Metadata for ML Models

### Prerequisites

- **Vertica Server** 10.1.1-0 or later (may work with older versions, untested)
- **Credentials**: Username and password
