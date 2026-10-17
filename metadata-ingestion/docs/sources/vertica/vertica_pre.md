### Overview

The `vertica` module ingests metadata from Vertica into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The DataHub Vertica plugin extracts the following:

- Metadata for databases, schemas, views, tables, and projections
- Table-level lineage for views and projections, and column-level lineage for views (parsed from their definitions; the catalog dependency map is the fallback for a view that does not parse)
- Metadata for ML Models

### Prerequisites

- **Vertica Server** 10.1.1-0 or later (may work with older versions, untested)
- **Credentials**: Username and password
