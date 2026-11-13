### Overview

This connector extracts tabular model metadata from Azure Analysis Services (and Power BI Premium
semantic models) through the XMLA endpoint. It emits each model as a semantic model, and each table
as a semantic model dataset with its columns, DAX measures, and relationships. It can optionally
resolve upstream lineage from partition queries and intra-model column lineage from DAX dependencies.

### Prerequisites

The connector talks to the server's XMLA endpoint over HTTPS using an Azure AD bearer token, so no
.NET runtime or ADOMD client is required.

1. Identify the server endpoint:
   - Azure Analysis Services: `asazure://<region>.asazure.windows.net/<server>`
   - Power BI Premium: `powerbi://api.powerbi.com/v1.0/myorg/<workspace>`
2. Provision an identity and make it an administrator of each model to ingest: add it to a model
   role with the Administrator permission, or make it a server administrator. The connector reads
   metadata through the `$SYSTEM.TMSCHEMA_*` Dynamic Management Views and `DISCOVER_CALC_DEPENDENCY`,
   which a Read role cannot query. A service principal is recommended for automated ingestion.
