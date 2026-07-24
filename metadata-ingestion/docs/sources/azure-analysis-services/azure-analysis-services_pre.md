### Prerequisites

The connector talks to the server's XMLA endpoint over HTTPS using an Azure AD bearer token, so no
.NET runtime or ADOMD client is required.

1. Identify the server endpoint:
   - Azure Analysis Services: `asazure://<region>.asazure.windows.net/<server>`
   - Power BI Premium: `powerbi://api.powerbi.com/v1.0/myorg/<workspace>`
2. Provision an identity with at least read access to the model(s). A service principal is
   recommended for automated ingestion; it must be added as a member of a model role (or as a
   server administrator) so it can read metadata through XMLA.
3. Grant the identity the model-level permission needed to run schema Dynamic Management View
   queries (`$SYSTEM.TMSCHEMA_*`) and `DISCOVER_CALC_DEPENDENCY`.
