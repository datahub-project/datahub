### Overview

The `powerbi-report-server` module ingests metadata from Powerbi Report Server into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

### Configuration Notes

1. Grant user access to Report Server following [Microsoft documentation](https://docs.microsoft.com/en-us/sql/reporting-services/security/grant-user-access-to-a-report-server?view=sql-server-ver16)
2. Use these credentials in your ingestion recipe

### Concept mapping

| Power BI Report Server | Datahub     |
| ---------------------- | ----------- |
| `Paginated Report`     | `Dashboard` |
| `Power BI Report`      | `Dashboard` |
| `Mobile Report`        | `Dashboard` |
| `Linked Report`        | `Dashboard` |
| `Dataset, Datasource`  | `N/A`       |
