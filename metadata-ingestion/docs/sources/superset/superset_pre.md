### Overview

The `superset` module ingests metadata from Superset into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Charts, dashboards, and associated metadata

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.
