### Overview

The `azure-ad` module ingests metadata from Azure Ad into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Required Azure AD Application Permissions

Create a DataHub application in the Azure AD portal and grant these **Application** permissions:

- `Group.Read.All`
- `GroupMember.Read.All`
- `User.Read.All`

You can add permissions in the **API permissions** tab of your application configuration.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/azure-ad/azure_ad_api_permissions.png"/>
</p>

You can verify required endpoint values from the **Endpoints** action in the application overview.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/azure-ad/azure_ad_endpoints.png"/>
</p>

#### SSO Caveat

Users ingested from this connector will only be able to log in to DataHub if Okta OIDC SSO is configured in your DataHub deployment.
