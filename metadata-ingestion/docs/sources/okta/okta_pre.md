### Overview

The `okta` module ingests metadata from Okta into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Note that any users ingested from this connector will not be able to log into DataHub unless you have Okta OIDC SSO enabled. You can, however, have these users ingested into DataHub before they log in for the first time if you would like to take actions like adding them to a group or assigning them a role.

For instructions on how to do configure Okta OIDC SSO, please read the documentation [here](https://docs.datahub.com/docs/authentication/guides/sso/configure-oidc-react#create-an-application-in-okta-developer-console).

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

As a prerequisite, you should create a DataHub Application within the Okta Developer Console with full permissions to read your organization's Users and Groups.

#### Compatibility

Validated against Okta API Versions:

- `2021.07.2`

Validated against load:

- User Count: `1000`
- Group Count: `100`
- Group Membership Edges: `1000` (1 per User)
- Run Time (Wall Clock): `2min 7sec`
