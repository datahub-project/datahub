### Overview

[Workday](https://www.workday.com/) is a cloud platform for HR, finance, and analytics. This source ingests metadata from [Workday Prism Analytics](https://www.workday.com/en-us/products/prism-analytics/overview.html) via the Prism Analytics REST API (v3).

The tenant is ingested as a container. Prism tables are ingested as datasets with schema fields, Prism datasets (pipeline definitions) and external data sources as datasets, and Workday-sourced data sources (RaaS custom reports and business-object sources) as report datasets. Lineage flows from tables to the datasets and data sources they derive from; external data sources can additionally resolve to the upstream warehouse dataset they were loaded from via `data_source_platform_mapping`.

### Prerequisites

1. In Workday, run the **Register API Client for Integrations** task to create an OAuth 2.0 client with the **Client Credentials** grant type.
2. Link the API client to an **Integration System User (ISU)** whose security groups grant read access to Prism Analytics (and the functional areas behind the reports you want to ingest).
3. Note the generated **Client ID** and **Client Secret** (the secret is shown only once), your **tenant** name, and your Workday **services host** (for example `https://wd2-impl-services1.workday.com`).

The connector requests short-lived bearer tokens from `{base_url}/ccx/oauth2/{tenant}/token` using the client credentials and refreshes them automatically; the Client Credentials grant does not issue a refresh token.
