### Overview

The `powerbi` module ingests metadata from Powerbi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Power BI dashboards, tiles and datasets
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

### Prerequisites

In order to execute this source, you will need to have a Microsoft Entra Application service principal and grant permissions to it inside Power BI.

[Power BI's APIs](https://learn.microsoft.com/en-us/rest/api/power-bi/) can be categorized into two sets of API methods, with different permission structures:

- Public APIs are designed for developers to interact with specific resources within a tenant, and require the Entra application to be explicitly granted access to individual Workspaces.
- The Admin APIs are designed for administrators to interact with the entire Power BI tenant at a high level, and return metadata on all Power BI resources.

The recommended way to execute Power BI ingestion is to do both: add your Entra application to the workspaces you want to ingest, andgrant it access to the public _and_ Admin APIs. That way ingestion can extract the most metadata.

#### Public APIs ingestion

To grant public API access to your Entra application:

1. **Grant permissions to access Fabric public APIs:** Add your Entra Application's parent Entra Group under your Power BI/Fabric tenant settings in order to grant API access.

   a. In Power BI or Fabric, go to `Settings` -> `Admin portal`

   b. In the `Admin portal`, navigate to `Tenant settings`

   d. Under `Developer Settings`, enable the option `Service principals can call Fabric Public APIs` (or `Allow service principals to use Power BI APIs` in older versions of Power BI), and add your application's Entra group under `Specific security groups`.

2. **Add your Entra application as a member of your Power BI workspaces:** For workspaces which you want to ingest into DataHub, add the Entra application as a member. For most cases `Viewer` role is enough, but for profiling the `Contributor` role is required.

If you have granted your Entra application permissions to the public APIs and added it as a member in a workspace, then the Power BI Source will be able to ingest the below metadata of that particular workspace:

- Dashboards
- Dashboard Tiles
- Reports
- Report Pages

If you don't want to add an Entra application as a member in your workspace, then you can enable `admin_apis_only: true` in your recipe to use the Power BI Admin API only. Caveats of setting `admin_apis_only` to `true`:

- Report Pages will not get ingested as the page API is not available in the Power BI Admin API
- [Power BI Parameters](https://learn.microsoft.com/en-us/power-query/power-query-query-parameters) will not get resolved to actual values while processing M-Query for table lineage
- Dataset profiling is unavailable, as it requires access to the non-admin workspace API

#### Admin APIs ingestion

To grant admin API access to the Entra application:

1. **Grant permissions to access Admin APIs:** Add your Entra Application's parent Entra Group under your Power BI/Fabric tenant settings in order to grant API access.

   a. In Power BI or Fabric, go to `Settings` -> `Admin portal`

   b. In the `Admin portal`, navigate to `Tenant settings`

   d. For each of the following options, enable the option and add your Entra application's Group under `Specific security groups`:

   - `Service principals can access read-only admin APIs`
   - `Enhance admin APIs responses with detailed metadata`
   - `Enhance admin APIs responses with DAX and mashup expressions`

If you have granted your Entra application permissions to the Admin APIs, then the Power BI Source will be able to ingest the below listed metadata of that particular workspace:

- Lineage
- Datasets
- Endorsement as tag
- Dashboards
- Dashboard Tiles
- Reports
- Report Pages
- App

#### Authentication

The source authenticates as the Entra application (service principal) using either a client secret or a certificate:

- **Client secret**: set `client_secret` to a client secret created under your Entra application's `Certificates & secrets`.
- **Certificate**: upload the certificate's public key to your Entra application under `Certificates & secrets` -> `Certificates`, then configure one of:
  - `certificate_path`: path to a PEM file readable by the ingestion process, or
  - `certificate_data`: the PEM content inline, typically referencing a secret, e.g. `certificate_data: "${POWERBI_CERTIFICATE_PEM}"`.

The PEM must contain both the private key and the certificate. If the private key is encrypted, set `certificate_password`. When storing the PEM in a DataHub secret or environment variable, you can keep it as-is (multi-line) or collapse it to a single line with literal `\n` escapes — both forms are accepted.

For example, to generate a self-signed certificate and prepare it for use:

```shell
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=datahub-powerbi-ingestion"
# Upload cert.pem to the Entra application, then combine both files into the PEM used by the recipe:
cat key.pem cert.pem > powerbi.pem
```

Configure exactly one of `client_secret` or the certificate options — not both.
