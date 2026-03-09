### Overview

The `sqlalchemy` module ingests metadata from SQLAlchemy into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The sqlalchemy source is useful if we don't have a pre-built source for your chosen database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/) defined elsewhere.
In order to use this, you must `pip install` the required dialect packages yourself.

This plugin extracts the following:

- Metadata for databases, schemas, views, and tables
- Column types associated with each table
- Table, row, and column statistics via optional SQL profiling.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
