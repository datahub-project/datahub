### Overview

The `hana` module ingests metadata from Hana into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The implementation uses the [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana). The SQLAlchemy Dialect for SAP HANA is an open-source project hosted at GitHub that is actively maintained by SAP SE, and is not part of a licensed SAP HANA edition or option. It is provided under the terms of the project license. Please notice that sqlalchemy-hana isn't an official SAP product and isn't covered by SAP support.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
