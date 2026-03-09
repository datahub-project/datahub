### Overview

The `powerbi` module ingests metadata from Powerbi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Power BI dashboards, tiles and datasets
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) to create a Microsoft AD Application. Once Microsoft AD Application is created you can configure client-credential i.e. client_id and client_secret in recipe for ingestion.
2. Enable admin access if you want to ingest data source and dataset information, including lineage, and endorsement tags. Refer section [Admin Ingestion vs. Basic Ingestion](#admin-ingestion-vs-basic-ingestion) for more detail.

   Login to PowerBI as Admin and from `Admin API settings` allow below permissions

   - Allow service principals to use read-only admin APIs
   - Enhance admin APIs responses with detailed metadata
   - Enhance admin APIs responses with DAX and mashup expressions
