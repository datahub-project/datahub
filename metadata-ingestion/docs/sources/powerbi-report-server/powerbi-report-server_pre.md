### Overview

The `powerbi-report-server` module ingests metadata from Powerbi Report Server into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Metadata that can be ingested:

- eport name
- eport description
- wnership(can add existing users in DataHub as owners)
- ransfer folders structure to DataHub as it is in Report Server
- ebUrl to report in Report Server

Due to limits of PBIRS REST API, it's impossible to ingest next data for now:

- tiles info
- datasource of report
- dataset of report

Next types of report can be ingested:

- PowerBI report(.pbix)
- Paginated report(.rdl)
- Linked report

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Grant user access to Report Server following [Microsoft documentation](https://docs.microsoft.com/en-us/sql/reporting-services/security/grant-user-access-to-a-report-server?view=sql-server-ver16)
2. Use these credentials in your ingestion recipe
